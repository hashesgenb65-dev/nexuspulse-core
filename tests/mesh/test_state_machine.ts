/**
 * Unit tests for the key-value state machine: command parsing, ordered application,
 * rejected commands, value isolation, and the commit applier. Also covers the single-node
 * commit fix in RaftConsensusNode.proposeCommand.
 */

import { describe, it, expect } from 'vitest';
import { RaftConsensusNode, type LogEntry } from '../../src/mesh/consensus';
import {
  CommitApplier,
  KeyValueStore,
  MAX_KEY_LENGTH,
  parseCommand,
} from '../../src/mesh/state_machine';

function entry(index: number, command: string, payload: unknown, term = 1): LogEntry {
  return { index, term, command, payload };
}

function set(index: number, key: string, value: unknown): LogEntry {
  return entry(index, 'SET', { key, value });
}

function del(index: number, key: string): LogEntry {
  return entry(index, 'DELETE', { key });
}

function soloNode(): RaftConsensusNode {
  const node = new RaftConsensusNode({
    nodeId: 'solo',
    clusterPeers: [],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
  node.start();
  return node;
}

describe('parseCommand', () => {
  it('reads SET and DELETE', () => {
    expect(parseCommand(set(1, 'a', 7))).toEqual({ ok: true, command: { op: 'SET', key: 'a', value: 7 } });
    expect(parseCommand(del(1, 'a'))).toEqual({ ok: true, command: { op: 'DELETE', key: 'a' } });
  });

  it('rejects a payload that is not an object', () => {
    expect(parseCommand(entry(1, 'SET', 'text'))).toMatchObject({ ok: false, reason: 'payload must be an object' });
    expect(parseCommand(entry(1, 'SET', [1, 2]))).toMatchObject({ ok: false });
    expect(parseCommand(entry(1, 'SET', null))).toMatchObject({ ok: false });
  });

  it('rejects a missing, empty or overlong key', () => {
    expect(parseCommand(entry(1, 'DELETE', {}))).toMatchObject({ ok: false });
    expect(parseCommand(entry(1, 'DELETE', { key: '' }))).toMatchObject({ ok: false });
    expect(parseCommand(entry(1, 'DELETE', { key: 'k'.repeat(MAX_KEY_LENGTH + 1) }))).toMatchObject({ ok: false });
    expect(parseCommand(entry(1, 'DELETE', { key: 'k'.repeat(MAX_KEY_LENGTH) }))).toMatchObject({ ok: true });
  });

  it('rejects a SET without a value', () => {
    expect(parseCommand(entry(1, 'SET', { key: 'a' }))).toMatchObject({ ok: false, reason: 'SET needs a value' });
  });

  it('rejects an unknown command and names it', () => {
    expect(parseCommand(entry(1, 'INCR', { key: 'a' }))).toEqual({
      ok: false,
      reason: 'unknown command "INCR"',
    });
  });
});

describe('KeyValueStore', () => {
  it('stores, overwrites and deletes values', () => {
    const store = new KeyValueStore();
    expect(store.apply(set(1, 'a', 1))).toBe('applied');
    expect(store.apply(set(2, 'a', 2))).toBe('applied');
    expect(store.get('a')).toBe(2);

    expect(store.apply(del(3, 'a'))).toBe('applied');
    expect(store.has('a')).toBe(false);
    expect(store.size).toBe(0);
  });

  it('treats a delete of a missing key as a no-op that still advances', () => {
    const store = new KeyValueStore();
    expect(store.apply(del(1, 'never-set'))).toBe('applied');
    expect(store.lastApplied).toBe(1);
    expect(store.size).toBe(0);
  });

  it('applies entries only in index order, one at a time', () => {
    const store = new KeyValueStore();
    store.apply(set(1, 'a', 1));
    expect(() => store.apply(set(3, 'b', 2))).toThrowError(RangeError);
    expect(() => store.apply(set(1, 'a', 9))).toThrowError(/expected entry 2, got 1/);
    expect(store.lastApplied).toBe(1);
    expect(store.get('a')).toBe(1);
  });

  it('records invalid commands and leaves the state unchanged', () => {
    const store = new KeyValueStore();
    store.apply(set(1, 'a', 1));

    expect(store.apply(entry(2, 'INCR', { key: 'a' }))).toBe('rejected');
    expect(store.apply(entry(3, 'SET', { key: 'b' }))).toBe('rejected');

    expect(store.lastApplied).toBe(3);
    expect(store.get('a')).toBe(1);
    expect(store.has('b')).toBe(false);
    expect(store.rejected.map((r) => r.index)).toEqual([2, 3]);
  });

  it('returns a snapshot with keys in sorted order', () => {
    const store = new KeyValueStore();
    store.apply(set(1, 'zeta', 1));
    store.apply(set(2, 'alpha', { nested: [true] }));
    store.apply(set(3, 'mid', 'x'));

    const snapshot = store.snapshot();
    expect(Object.keys(snapshot)).toEqual(['alpha', 'mid', 'zeta']);
    expect(snapshot).toEqual({ alpha: { nested: [true] }, mid: 'x', zeta: 1 });
  });

  it('copies values on the way in, so later changes to the caller do not reach the store', () => {
    const store = new KeyValueStore();
    const value = { count: 1 };
    store.apply(set(1, 'a', value));
    value.count = 99;

    expect(store.get('a')).toEqual({ count: 1 });
  });

  it('copies values on the way out, so changes to a snapshot do not reach the store', () => {
    const store = new KeyValueStore();
    store.apply(set(1, 'a', { count: 1 }));
    const snapshot = store.snapshot();
    (snapshot.a as { count: number }).count = 50;

    expect(store.get('a')).toEqual({ count: 1 });
  });

  it('two stores that apply the same entries have equal snapshots', () => {
    const entries = [set(1, 'a', 1), set(2, 'b', 'two'), del(3, 'a'), entry(4, 'NOOP', {}), set(5, 'c', [3])];
    const first = new KeyValueStore();
    const second = new KeyValueStore();
    for (const e of entries) first.apply(e);
    for (const e of entries) second.apply(e);

    expect(JSON.stringify(first.snapshot())).toBe(JSON.stringify(second.snapshot()));
    expect(first.rejected).toEqual(second.rejected);
  });
});

describe('CommitApplier', () => {
  it('applies only the entries up to commitIndex', () => {
    const node = soloNode();
    node.appendEntries(1, 'other', [set(1, 'a', 1), set(2, 'b', 2), set(3, 'c', 3)], { leaderCommit: 2 });
    const store = new KeyValueStore();
    const applier = new CommitApplier(node, store);

    expect(applier.applyCommitted()).toBe(2);
    expect(store.lastApplied).toBe(2);
    expect(store.has('c')).toBe(false);
  });

  it('applies incrementally as the commit index moves', () => {
    const node = soloNode();
    node.appendEntries(1, 'other', [set(1, 'a', 1), set(2, 'b', 2)]);
    const store = new KeyValueStore();
    const applier = new CommitApplier(node, store);

    expect(applier.applyCommitted()).toBe(0);
    node.appendEntries(1, 'other', [], { prevLogIndex: 2, prevLogTerm: 1, leaderCommit: 1 });
    expect(applier.applyCommitted()).toBe(1);
    node.appendEntries(1, 'other', [], { prevLogIndex: 2, prevLogTerm: 1, leaderCommit: 2 });
    expect(applier.applyCommitted()).toBe(1);
    expect(applier.applyCommitted()).toBe(0);
    expect(store.get('b')).toBe(2);
  });

  it('refuses to apply a committed index that the log does not hold', () => {
    const node = soloNode();
    node.commitIndex = 2;
    const applier = new CommitApplier(node, new KeyValueStore());
    expect(() => applier.applyCommitted()).toThrowError(/committed entry 1 is missing/);
  });
});

describe('single-node commits', () => {
  it('commits a proposed entry straight away when there are no peers', () => {
    const node = soloNode();
    node.startElection();
    expect(node.state).toBe('LEADER');

    node.proposeCommand('SET', { key: 'a', value: 1 });
    expect(node.commitIndex).toBe(1);

    const store = new KeyValueStore();
    expect(new CommitApplier(node, store).applyCommitted()).toBe(1);
    expect(store.get('a')).toBe(1);
  });
});
