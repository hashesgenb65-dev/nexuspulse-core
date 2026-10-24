/**
 * Ties the replicated store to the consensus log: applies committed entries, folds them into a
 * snapshot once enough have accumulated, and installs a snapshot sent by the leader.
 *
 * Compaction runs only after the store has applied an index, so the snapshot always matches the
 * store exactly. The leader keeps its latest snapshot so it can send it to a peer that has fallen
 * behind the compacted part of the log.
 *
 * With a snapshot directory, compaction is pending until the snapshot file is on disk. The log is
 * not trimmed until then, so `state.json` always holds either the old log or a log whose snapshot
 * file exists. Without a directory the snapshot stays in memory and the log is trimmed at once.
 */

import type { RaftConsensusNode, SnapshotOffer } from './consensus.js';
import { CommitApplier, KeyValueStore, type StoreImage } from './state_machine.js';
import { writeSnapshotFileAsync } from './snapshot_writer.js';

/** Compact once this many applied entries sit above the last snapshot. */
export const DEFAULT_COMPACT_THRESHOLD = 1_000;

/** A background snapshot that takes longer than this is reported, though it is still allowed to finish. */
export const SNAPSHOT_PENDING_WARN_MS = 10_000;

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
  /**
   * Directory for snapshot files. When set, compaction writes the snapshot in the background and trims
   * the log only after the file is on disk.
   */
  readonly snapshotDirectory?: string;
  /** Called after a background snapshot has been written and the log trimmed. The caller saves state here. */
  readonly onSnapshotCommitted?: () => void;
  /** Receives background snapshot failures and warnings. They never stop the node. */
  readonly onError?: (error: unknown, where: string) => void;
}

interface PendingCompaction {
  readonly index: number;
  readonly term: number;
  readonly startedAt: number;
}

export class StateHost {
  private readonly node: RaftConsensusNode;
  private readonly store: KeyValueStore;
  private readonly applier: CommitApplier;
  private readonly compactThreshold: number;
  private readonly snapshotDirectory: string | undefined;
  private readonly onSnapshotCommitted: (() => void) | undefined;
  private readonly onError: ((error: unknown, where: string) => void) | undefined;
  private latest: SnapshotPayload | undefined;
  private pending: PendingCompaction | undefined;
  private disposed = false;

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
    this.snapshotDirectory = options.snapshotDirectory;
    this.onSnapshotCommitted = options.onSnapshotCommitted;
    this.onError = options.onError;
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

  /** True while a background snapshot is being written. */
  get compactionPending(): boolean {
    return this.pending !== undefined;
  }

  /**
   * Stops committing background snapshots. A write still in progress finishes, but its result is
   * dropped, so the log is not trimmed after the node has stopped.
   */
  dispose(): void {
    this.disposed = true;
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
    if (this.pending !== undefined || this.disposed) return;
    const applied = this.store.lastApplied;
    if (applied - this.node.snapshotIndex < this.compactThreshold) return;

    const term = this.node.termAt(applied);
    if (term === undefined) return;

    if (this.snapshotDirectory === undefined) {
      // Shares the store's values: the snapshot is only serialized, and apply never changes a stored value in place.
      this.latest = { index: applied, term, data: this.store.referenceImage() };
      this.node.compactTo(applied);
      return;
    }
    this.startBackgroundCompaction(this.snapshotDirectory, applied, term);
  }

  /**
   * Writes the snapshot file in the background. The log is trimmed only when the file is on disk, so
   * until then the node keeps the full log and the old snapshot.
   */
  private startBackgroundCompaction(directory: string, index: number, term: number): void {
    const data = this.store.referenceImage();
    const pending: PendingCompaction = { index, term, startedAt: Date.now() };
    this.pending = pending;
    writeSnapshotFileAsync(directory, { index, term, data }).then(
      () => this.commitCompaction(pending, data),
      (error: unknown) => {
        if (this.pending === pending) this.pending = undefined;
        this.report(error, 'snapshot');
      },
    );
  }

  /**
   * Trims the log once the snapshot file is on disk. Nothing is trimmed if the node has stopped, or if a
   * snapshot newer than this one was installed from the leader in the meantime.
   */
  private commitCompaction(pending: PendingCompaction, data: StoreImage): void {
    if (this.pending === pending) this.pending = undefined;
    const elapsed = Date.now() - pending.startedAt;
    if (elapsed > SNAPSHOT_PENDING_WARN_MS) {
      this.report(new Error(`snapshot ${pending.index} took ${elapsed} ms to write`), 'snapshot');
    }
    if (this.disposed) return;
    if (pending.index <= this.node.snapshotIndex) return;
    if (this.node.termAt(pending.index) !== pending.term) {
      this.report(new Error(`snapshot ${pending.index} no longer matches the log; not compacting`), 'snapshot');
      return;
    }

    this.latest = { index: pending.index, term: pending.term, data };
    try {
      this.node.compactTo(pending.index);
    } catch (error) {
      this.report(error, 'snapshot');
      return;
    }
    try {
      this.onSnapshotCommitted?.();
    } catch (error) {
      this.report(error, 'snapshot');
    }
  }

  private report(error: unknown, where: string): void {
    this.onError?.(error, where);
  }
}
