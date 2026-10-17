/**
 * Ties the replicated store to the consensus log: applies committed entries, folds them into a
 * snapshot once enough have accumulated, and installs a snapshot sent by the leader.
 *
 * Compaction runs only after the store has applied an index, so the snapshot always matches the
 * store exactly. The leader keeps its latest snapshot so it can send it to a peer that has fallen
 * behind the compacted part of the log.
 */

import type { RaftConsensusNode, SnapshotOffer } from './consensus.js';
import { CommitApplier, KeyValueStore, type StoreImage } from './state_machine.js';

/** Compact once this many applied entries sit above the last snapshot. */
export const DEFAULT_COMPACT_THRESHOLD = 1_000;

export interface SnapshotPayload extends SnapshotOffer {
  readonly index: number;
  readonly term: number;
  readonly data: StoreImage;
}

export interface StateHostOptions {
  readonly node: RaftConsensusNode;
  readonly store: KeyValueStore;
  /** Entries above the snapshot that trigger compaction. Positive integer. Defaults to DEFAULT_COMPACT_THRESHOLD. */
  readonly compactThreshold?: number;
}

export class StateHost {
  private readonly node: RaftConsensusNode;
  private readonly store: KeyValueStore;
  private readonly applier: CommitApplier;
  private readonly compactThreshold: number;
  private latest: SnapshotPayload | undefined;

  /** @throws {RangeError} If the compaction threshold is not a positive integer. */
  constructor(options: StateHostOptions) {
    const threshold = options.compactThreshold ?? DEFAULT_COMPACT_THRESHOLD;
    if (!Number.isInteger(threshold) || threshold <= 0) {
      throw new RangeError(`compactThreshold must be a positive integer, got ${threshold}`);
    }
    this.node = options.node;
    this.store = options.store;
    this.applier = new CommitApplier(options.node, options.store);
    this.compactThreshold = threshold;
  }

  /**
   * Applies every committed entry, then compacts if enough have accumulated.
   *
   * @returns The number of entries applied by this call.
   */
  applyCommitted(): number {
    const applied = this.applier.applyCommitted();
    this.maybeCompact();
    return applied;
  }

  /** The snapshot the leader would send to a lagging peer, if one exists. */
  get latestSnapshot(): SnapshotPayload | undefined {
    return this.latest;
  }

  /**
   * Adopts a snapshot from the leader. Returns true if this node now holds the state through
   * `snapshot.index`, including when it already did. Returns false only when the snapshot does
   * not fit this node.
   *
   * @throws {TypeError} If the snapshot data is malformed.
   * @throws {Error} If the restored store does not end at the snapshot index.
   */
  install(snapshot: SnapshotOffer): boolean {
    if (snapshot.index <= this.store.lastApplied) return true;

    const image = this.restoreStore(snapshot);
    if (!this.node.installSnapshot(snapshot.index, snapshot.term)) return false;

    this.latest = { index: snapshot.index, term: snapshot.term, data: image };
    return true;
  }

  /**
   * Restores the store from a snapshot read from disk, at start-up. The consensus node is not
   * touched: the caller sets its fields to match.
   *
   * @throws {TypeError} If the snapshot data is malformed.
   * @throws {Error} If the data does not end at the snapshot index.
   */
  load(snapshot: SnapshotOffer): void {
    const image = this.restoreStore(snapshot);
    this.latest = { index: snapshot.index, term: snapshot.term, data: image };
  }

  /**
   * Restores the store to a snapshot. It restores into a staging store first, so a bad snapshot
   * leaves this node's state untouched.
   */
  private restoreStore(snapshot: SnapshotOffer): StoreImage {
    const staging = new KeyValueStore();
    staging.restore(snapshot.data);
    if (staging.lastApplied !== snapshot.index) {
      throw new Error(`snapshot data ends at ${staging.lastApplied}, expected ${snapshot.index}`);
    }
    const image = staging.image();
    this.store.restore(image);
    return image;
  }

  private maybeCompact(): void {
    const applied = this.store.lastApplied;
    if (applied - this.node.snapshotIndex < this.compactThreshold) return;

    const term = this.node.termAt(applied);
    if (term === undefined) return;

    this.latest = { index: applied, term, data: this.store.image() };
    this.node.compactTo(applied);
  }
}
