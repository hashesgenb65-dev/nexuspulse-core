/**
 * Replicated key-value state machine: applies committed log entries, in index order, to a map
 * of keys to values.
 *
 * Commands. Each log entry carries a command name and a payload:
 *   SET     payload { key: string, value: unknown }  stores the value under the key
 *   DELETE  payload { key: string }                  removes the key; a missing key is a no-op
 *
 * Every node applies the same committed entries in the same order, so every node reaches the
 * same state. An entry that is not a valid command is still applied: it changes nothing and is
 * recorded in {@link KeyValueStore.rejected}. Every node makes the same decision, so the stores
 * stay identical and the node keeps running.
 *
 * Values are copied on the way in, so later changes to the caller's objects do not reach the store.
 */

import type { LogEntry, RaftConsensusNode } from './consensus.js';

export const MAX_KEY_LENGTH = 256;

export type Command =
  | { readonly op: 'SET'; readonly key: string; readonly value: unknown }
  | { readonly op: 'DELETE'; readonly key: string };

export type ParseResult =
  | { readonly ok: true; readonly command: Command }
  | { readonly ok: false; readonly reason: string };

export interface RejectedEntry {
  readonly index: number;
  readonly reason: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Turns a log entry into a command, or says why it is not one. */
export function parseCommand(entry: LogEntry): ParseResult {
  if (!isRecord(entry.payload)) return { ok: false, reason: 'payload must be an object' };
  const { key } = entry.payload;
  if (typeof key !== 'string' || key.length === 0 || key.length > MAX_KEY_LENGTH) {
    return { ok: false, reason: `key must be 1 to ${MAX_KEY_LENGTH} characters` };
  }

  switch (entry.command) {
    case 'SET':
      if (!('value' in entry.payload)) return { ok: false, reason: 'SET needs a value' };
      return { ok: true, command: { op: 'SET', key, value: entry.payload.value } };
    case 'DELETE':
      return { ok: true, command: { op: 'DELETE', key } };
    default:
      return { ok: false, reason: `unknown command ${JSON.stringify(entry.command)}` };
  }
}

export class KeyValueStore {
  private readonly data = new Map<string, unknown>();
  private applied = 0;
  private readonly rejectedEntries: RejectedEntry[] = [];

  /** Index of the last entry applied. Zero before anything is applied. */
  get lastApplied(): number {
    return this.applied;
  }

  get size(): number {
    return this.data.size;
  }

  /** Entries that were applied as no-ops because they were not valid commands. */
  get rejected(): readonly RejectedEntry[] {
    return [...this.rejectedEntries];
  }

  has(key: string): boolean {
    return this.data.has(key);
  }

  /** Returns the value stored under a key, or undefined. */
  get(key: string): unknown {
    return this.data.get(key);
  }

  /**
   * Applies one entry. Entries must arrive in order, one index at a time.
   *
   * @returns 'applied' if the entry changed the store, 'rejected' if it was a no-op.
   * @throws {RangeError} If the entry is not the one directly after lastApplied.
   */
  apply(entry: LogEntry): 'applied' | 'rejected' {
    if (entry.index !== this.applied + 1) {
      throw new RangeError(`expected entry ${this.applied + 1}, got ${entry.index}`);
    }
    this.applied = entry.index;

    const parsed = parseCommand(entry);
    if (!parsed.ok) {
      this.rejectedEntries.push({ index: entry.index, reason: parsed.reason });
      return 'rejected';
    }

    const { command } = parsed;
    if (command.op === 'SET') {
      this.data.set(command.key, structuredClone(command.value));
    } else {
      this.data.delete(command.key);
    }
    return 'applied';
  }

  /** A copy of the whole state, with keys in sorted order so that two stores compare equal. */
  snapshot(): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const key of [...this.data.keys()].sort()) {
      out[key] = structuredClone(this.data.get(key));
    }
    return out;
  }
}

/**
 * Feeds a node's committed entries to a store. Call {@link CommitApplier.applyCommitted}
 * whenever the node's commitIndex may have moved.
 */
export class CommitApplier {
  private readonly node: RaftConsensusNode;
  private readonly store: KeyValueStore;

  constructor(node: RaftConsensusNode, store: KeyValueStore) {
    this.node = node;
    this.store = store;
  }

  /**
   * Applies every committed entry the store has not seen yet.
   *
   * @returns The number of entries applied by this call.
   * @throws {Error} If a committed index is missing from the log, which should never happen.
   */
  applyCommitted(): number {
    let count = 0;
    while (this.store.lastApplied < this.node.commitIndex) {
      const next = this.store.lastApplied + 1;
      const entry = this.node.log[next - 1];
      if (entry === undefined || entry.index !== next) {
        throw new Error(`committed entry ${next} is missing from the log`);
      }
      this.store.apply(entry);
      count++;
    }
    return count;
  }
}
