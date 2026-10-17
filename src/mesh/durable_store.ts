/**
 * Durable state for one mesh member: the current term, the vote, the log and the latest snapshot,
 * kept in one JSON file in the node's data directory.
 *
 * Raft needs these to survive a restart. A node must not vote twice in one term, and must not forget
 * entries it has acknowledged to the leader.
 *
 * Writes are atomic. The new state is written to a temporary file, flushed to disk, and renamed over
 * the old file. A crash leaves either the old file or the new one, never a mix of the two.
 *
 * The file is read at start-up and written after every change to the term, the vote, the log or the
 * snapshot. The driver saves before it answers a peer, so a reply never outruns the state behind it.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import type { LogEntry, RaftConsensusNode } from './consensus.js';
import type { SnapshotPayload, StateHost } from './snapshot.js';

export const STATE_FILE_VERSION = 1;
export const STATE_FILE_NAME = 'state.json';

/** What a node must keep across restarts. `snapshot` is null when nothing has been compacted. */
export interface DurableState {
  readonly currentTerm: number;
  readonly votedFor: string | null;
  readonly snapshotIndex: number;
  readonly snapshotTerm: number;
  readonly snapshot: SnapshotPayload | null;
  readonly log: readonly LogEntry[];
}

/** Raised when the state file cannot be read or does not describe a consistent node. Code: DURABLE_STATE. */
export class DurableStateError extends Error {
  public readonly code = 'DURABLE_STATE';

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'DurableStateError';
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0;
}

/**
 * Checks a parsed state file and returns it typed. Checks the version, the types, that the log
 * is contiguous from the snapshot, and that no entry is from a term after the current one.
 *
 * @throws {DurableStateError} On any inconsistency.
 */
export function parseDurableState(raw: unknown): DurableState {
  if (!isRecord(raw)) throw new DurableStateError('state file must be an object');
  if (raw.version !== STATE_FILE_VERSION) {
    throw new DurableStateError(`unsupported state file version ${JSON.stringify(raw.version)}`);
  }

  const { currentTerm, votedFor, snapshotIndex, snapshotTerm, snapshot, log } = raw;
  if (!isNonNegativeInteger(currentTerm)) throw new DurableStateError('currentTerm must be a non-negative integer');
  if (votedFor !== null && typeof votedFor !== 'string') throw new DurableStateError('votedFor must be a string or null');
  if (!isNonNegativeInteger(snapshotIndex) || !isNonNegativeInteger(snapshotTerm)) {
    throw new DurableStateError('snapshotIndex and snapshotTerm must be non-negative integers');
  }

  let latest: SnapshotPayload | null = null;
  if (snapshot !== null) {
    if (!isRecord(snapshot) || snapshot.index !== snapshotIndex || !isNonNegativeInteger(snapshot.term)) {
      throw new DurableStateError('snapshot must match snapshotIndex');
    }
    if (!isRecord(snapshot.data)) throw new DurableStateError('snapshot data must be an object');
    latest = snapshot as unknown as SnapshotPayload;
  } else if (snapshotIndex !== 0) {
    throw new DurableStateError('snapshotIndex is above zero but no snapshot is stored');
  }

  if (!Array.isArray(log)) throw new DurableStateError('log must be an array');
  let previousTerm = snapshotTerm;
  for (const [offset, entry] of log.entries()) {
    const expected = snapshotIndex + offset + 1;
    if (!isRecord(entry) || entry.index !== expected) {
      throw new DurableStateError(`log entry ${offset} must have index ${expected}`);
    }
    if (!isNonNegativeInteger(entry.term) || entry.term > currentTerm || entry.term < previousTerm) {
      throw new DurableStateError(`log entry ${expected} has an impossible term`);
    }
    if (typeof entry.command !== 'string' || !('payload' in entry)) {
      throw new DurableStateError(`log entry ${expected} is malformed`);
    }
    previousTerm = entry.term;
  }

  return {
    currentTerm,
    votedFor,
    snapshotIndex,
    snapshotTerm,
    snapshot: latest,
    log: log as LogEntry[],
  };
}

/** One state file in a data directory. Writes are atomic; see the module comment. */
export class DurableStateFile {
  private readonly directory: string;
  private readonly file: string;

  constructor(directory: string) {
    this.directory = directory;
    this.file = path.join(directory, STATE_FILE_NAME);
  }

  /** Full path of the state file. */
  get location(): string {
    return this.file;
  }

  /**
   * Reads the state file.
   *
   * @returns undefined if there is no state file yet, which means a fresh node.
   * @throws {DurableStateError} If the file exists but is not valid JSON or is inconsistent.
   */
  load(): DurableState | undefined {
    let text: string;
    try {
      text = fs.readFileSync(this.file, 'utf8');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
      throw error;
    }

    let raw: unknown;
    try {
      raw = JSON.parse(text) as unknown;
    } catch {
      throw new DurableStateError(`${this.file} is not valid JSON`);
    }
    return parseDurableState(raw);
  }

  /** Replaces the state file atomically: write a temporary file, flush it, then rename it into place. */
  save(state: DurableState): void {
    fs.mkdirSync(this.directory, { recursive: true });
    const temporary = `${this.file}.tmp`;
    const bytes = Buffer.from(JSON.stringify({ version: STATE_FILE_VERSION, ...state }), 'utf8');

    const fd = fs.openSync(temporary, 'w');
    try {
      let written = 0;
      while (written < bytes.length) written += fs.writeSync(fd, bytes, written, bytes.length - written);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
    fs.renameSync(temporary, this.file);
    syncDirectory(this.directory);
  }
}

/** Flushes a directory entry so the rename survives a crash. Some platforms cannot open directories; there it is skipped. */
function syncDirectory(directory: string): void {
  let fd: number;
  try {
    fd = fs.openSync(directory, 'r');
  } catch {
    return;
  }
  try {
    fs.fsyncSync(fd);
  } catch {
    // Directory fsync is not supported everywhere; the file itself was already flushed.
  } finally {
    fs.closeSync(fd);
  }
}

export interface NodePersisterOptions {
  readonly file: DurableStateFile;
  readonly node: RaftConsensusNode;
  readonly host: StateHost;
}

/**
 * Writes the node's durable state when it has changed. Compares a fingerprint of the term, vote,
 * log end and snapshot, so repeated calls with nothing new do not touch the disk.
 *
 * Within one term, two log entries with the same index always hold the same command, so the last
 * index and term identify the log's end.
 */
export class NodePersister {
  private readonly file: DurableStateFile;
  private readonly node: RaftConsensusNode;
  private readonly host: StateHost;
  private lastFingerprint: string | undefined;

  constructor(options: NodePersisterOptions) {
    this.file = options.file;
    this.node = options.node;
    this.host = options.host;
  }

  /** Call after restoring from disk, so that the restored state is not written straight back. */
  markSaved(): void {
    this.lastFingerprint = this.fingerprint();
  }

  /**
   * Saves if anything durable has changed since the last save.
   *
   * @returns true if the file was written.
   * @throws {Error} If the write fails. The caller must then stop the node: its memory may be ahead of the disk.
   */
  save(): boolean {
    const fingerprint = this.fingerprint();
    if (fingerprint === this.lastFingerprint) return false;

    const latest = this.host.latestSnapshot ?? null;
    this.file.save({
      currentTerm: this.node.currentTerm,
      votedFor: this.node.votedFor,
      snapshotIndex: this.node.snapshotIndex,
      snapshotTerm: this.node.snapshotTerm,
      snapshot: latest as SnapshotPayload | null,
      log: this.node.log,
    });
    this.lastFingerprint = fingerprint;
    return true;
  }

  private fingerprint(): string {
    const last = this.node.log[this.node.log.length - 1];
    return JSON.stringify([
      this.node.currentTerm,
      this.node.votedFor,
      this.node.snapshotIndex,
      this.node.snapshotTerm,
      this.host.latestSnapshot?.index ?? 0,
      this.node.lastIndex,
      last?.term ?? 0,
    ]);
  }
}

/**
 * Loads the state into a fresh node and host. Call before the driver starts.
 *
 * @throws {DurableStateError} If the state does not fit the node.
 */
export function restoreNode(state: DurableState, node: RaftConsensusNode, host: StateHost): void {
  if (state.snapshot !== null) host.load(state.snapshot);
  node.currentTerm = state.currentTerm;
  node.votedFor = state.votedFor;
  node.snapshotIndex = state.snapshotIndex;
  node.snapshotTerm = state.snapshotTerm;
  node.log = [...state.log];
  // Committed entries are re-learned from the leader; only the snapshot is known to be applied.
  node.commitIndex = state.snapshotIndex;
}
