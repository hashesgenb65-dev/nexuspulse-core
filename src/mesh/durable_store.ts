/**
 * Durable state for one mesh member, kept in the node's data directory: the current term, the vote
 * and the log in `state.json`, and the latest snapshot in `snapshot-<index>.json`.
 *
 * Raft needs these to survive a restart. A node must not vote twice in one term, and must not forget
 * entries it has acknowledged to the leader.
 *
 * The snapshot is large and changes only when the node compacts, so it has its own file. It is written
 * once per snapshot index. `state.json` refers to it by index and is rewritten after every change to
 * the term, the vote or the log, which keeps those writes small.
 *
 * Writes are atomic. Each file is written to a temporary file, flushed to disk, and renamed over the
 * old one. The snapshot file is written before `state.json` refers to it, and older snapshot files are
 * removed only after that. A crash at any point leaves a state that loads.
 *
 * The state is read at start-up and written after every change to the term, the vote, the log or the
 * snapshot. The driver saves before it answers a peer, so a reply never outruns the state behind it.
 *
 * Version 1 files kept the snapshot inside `state.json`. They still load; the next save moves it.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import type { LogEntry, RaftConsensusNode } from './consensus.js';
import type { SnapshotPayload, StateHost } from './snapshot.js';

export const STATE_FILE_VERSION = 2;
/** The version that stored the snapshot inside `state.json`. Still read, never written. */
export const LEGACY_STATE_FILE_VERSION = 1;
export const STATE_FILE_NAME = 'state.json';

const SNAPSHOT_FILE_PATTERN = /^snapshot-(\d+)\.json$/;

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
  if (raw.version !== STATE_FILE_VERSION && raw.version !== LEGACY_STATE_FILE_VERSION) {
    throw new DurableStateError(`unsupported state file version ${JSON.stringify(raw.version)}`);
  }

  const { currentTerm, votedFor, snapshotIndex, snapshotTerm, log } = raw;
  // A version 2 file has no snapshot field: DurableStateFile.load fills it in from its own file.
  const snapshot = raw.snapshot ?? null;
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
    if (isRecord(raw) && raw.version === STATE_FILE_VERSION && raw.snapshot === undefined) {
      const { snapshotIndex } = raw;
      if (isNonNegativeInteger(snapshotIndex) && snapshotIndex > 0) {
        raw = { ...raw, snapshot: this.readSnapshot(snapshotIndex) };
      }
    }
    return parseDurableState(raw);
  }

  /**
   * Writes the state: the snapshot first if it is new, then `state.json`, then removes older snapshot
   * files. Each write is atomic.
   */
  save(state: DurableState): void {
    fs.mkdirSync(this.directory, { recursive: true });
    if (state.snapshot !== null) this.writeSnapshot(state.snapshot);

    const body = {
      version: STATE_FILE_VERSION,
      currentTerm: state.currentTerm,
      votedFor: state.votedFor,
      snapshotIndex: state.snapshotIndex,
      snapshotTerm: state.snapshotTerm,
      log: state.log,
    };
    writeAtomically(this.file, Buffer.from(JSON.stringify(body), 'utf8'));
    this.removeStaleSnapshots(state.snapshotIndex);
  }

  private snapshotPath(index: number): string {
    return path.join(this.directory, `snapshot-${index}.json`);
  }

  /** Writes a snapshot file unless one for this index already exists. A snapshot never changes for its index. */
  private writeSnapshot(snapshot: SnapshotPayload): void {
    const target = this.snapshotPath(snapshot.index);
    if (fs.existsSync(target)) return;
    const { index, term, data } = snapshot;
    writeAtomically(target, Buffer.from(JSON.stringify({ index, term, data }), 'utf8'));
  }

  /** Reads the snapshot file for `index`. The state validation checks that it matches. */
  private readSnapshot(index: number): unknown {
    let text: string;
    try {
      text = fs.readFileSync(this.snapshotPath(index), 'utf8');
    } catch {
      throw new DurableStateError(`snapshot file for index ${index} is missing or unreadable`);
    }
    try {
      return JSON.parse(text) as unknown;
    } catch {
      throw new DurableStateError(`snapshot file for index ${index} is not valid JSON`);
    }
  }

  /**
   * Removes snapshot files for other indexes. Best effort: a file that cannot be removed is harmless
   * and is tried again on the next save.
   */
  private removeStaleSnapshots(keep: number): void {
    for (const name of fs.readdirSync(this.directory)) {
      const match = SNAPSHOT_FILE_PATTERN.exec(name);
      if (match === null || Number(match[1]) === keep) continue;
      try {
        fs.unlinkSync(path.join(this.directory, name));
      } catch {
        // Left for the next save.
      }
    }
  }
}

/** Replaces a file atomically: write a temporary file, flush it, rename it into place, then flush the directory. */
function writeAtomically(target: string, bytes: Buffer): void {
  const temporary = `${target}.tmp`;
  const fd = fs.openSync(temporary, 'w');
  try {
    let written = 0;
    while (written < bytes.length) written += fs.writeSync(fd, bytes, written, bytes.length - written);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(temporary, target);
  syncDirectory(path.dirname(target));
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
