/**
 * Unit tests for snapshots: saving and restoring the store, compaction and installation in the
 * state host, and the driver's handling of an InstallSnapshot from a leader.
 */

import { describe, it, expect, vi } from 'vitest';
import { RaftConsensusNode, type LogEntry, type InstallSnapshotRequest } from '../../src/mesh/consensus';
import { ConsensusDriver, type PeerTransport, type Scheduler } from '../../src/mesh/driver';
import { StateHost, DEFAULT_COMPACT_THRESHOLD } from '../../src/mesh/snapshot';
import { KeyValueStore } from '../../src/mesh/state_machine';

function set(index: number, key: string, value: unknown, term = 1): LogEntry {
  return { index, term, command: 'SET', payload: { key, value } };
}

function followerWith(entries: LogEntry[], leaderCommit: number): RaftConsensusNode {
  const node = new RaftConsensusNode({
    nodeId: 'follower',
    clusterPeers: ['leader'],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
  node.start();
  node.appendEntries(1, 'leader', entries, { prevLogIndex: 0, prevLogTerm: 0, leaderCommit });
  return node;
}

function applied(entries: LogEntry[]): KeyValueStore {
  const store = new KeyValueStore();
  for (const entry of entries) store.apply(entry);
  return store;
}

const idleScheduler: Scheduler = {
  schedule: () => 0,
  cancel: () => undefined,
};

const unusedTransport: PeerTransport = {
  requestVote: async () => {
    throw new Error('not used');
  },
  appendEntries: async () => {
    throw new Error('not used');
  },
  installSnapshot: async () => {
    throw new Error('not used');
  },
};

describe('KeyValueStore image and restore', () => {
  it('restores the same data, index and rejected record', () => {
    const source = applied([set(1, 'a', 1), set(2, 'b', { x: [1] })]);
    source.apply({ index: 3, term: 1, command: 'INCR', payload: { key: 'a' } });

    const target = new KeyValueStore();
    target.restore(source.image());

    expect(target.snapshot()).toEqual(source.snapshot());
    expect(target.lastApplied).toBe(3);
    expect(target.rejected).toEqual(source.rejected);
  });

  it('copies values both ways, so later changes do not leak between the stores', () => {
    const source = applied([set(1, 'b', { x: [1] })]);
    const image = source.image();
    (image.data.b as { x: number[] }).x.push(9);
    expect(source.get('b')).toEqual({ x: [1] });

    const target = new KeyValueStore();
    target.restore(image);
    (target.get('b') as { x: number[] }).x.push(7);
    expect(image.data.b).toEqual({ x: [1, 9] });
  });

  it('replaces what the store held before, rather than merging into it', () => {
    const target = applied([set(1, 'old', 'gone')]);
    target.restore(applied([set(1, 'new', 'here')]).image());
    expect(target.has('old')).toBe(false);
    expect(target.get('new')).toBe('here');
  });

  it('refuses a malformed image and leaves the store unchanged', () => {
    const store = applied([set(1, 'a', 1)]);
    expect(() => store.restore('nope')).toThrowError(TypeError);
    expect(() => store.restore({ lastApplied: -1, data: {}, rejected: [] })).toThrowError(TypeError);
    expect(() => store.restore({ lastApplied: 1, data: {}, rejected: [{ index: 'x', reason: 'r' }] })).toThrowError(
      TypeError,
    );
    expect(store.lastApplied).toBe(1);
    expect(store.get('a')).toBe(1);
  });
});

describe('StateHost compaction', () => {
  it('folds applied entries into a snapshot once the lag reaches the threshold', () => {
    const entries = Array.from({ length: 10 }, (_, i) => set(i + 1, `k${i}`, i));
    const node = followerWith(entries, 10);
    const store = new KeyValueStore();
    const host = new StateHost({ node, store, compactThreshold: 4 });

    expect(host.applyCommitted()).toBe(10);
    expect(node.snapshotIndex).toBe(10);
    expect(node.log).toHaveLength(0);
    expect(host.latestSnapshot).toMatchObject({ index: 10, term: 1 });
    expect(host.latestSnapshot?.data.lastApplied).toBe(10);
  });

  it('keeps the log while the lag is under the threshold', () => {
    const entries = Array.from({ length: 3 }, (_, i) => set(i + 1, `k${i}`, i));
    const node = followerWith(entries, 3);
    const host = new StateHost({ node, store: new KeyValueStore(), compactThreshold: 4 });

    host.applyCommitted();
    expect(node.snapshotIndex).toBe(0);
    expect(host.latestSnapshot).toBeUndefined();
  });

  it('keeps applying committed entries after compaction, looking them up by index', () => {
    const first = Array.from({ length: 5 }, (_, i) => set(i + 1, `k${i}`, i));
    const node = followerWith(first, 5);
    const store = new KeyValueStore();
    const host = new StateHost({ node, store, compactThreshold: 5 });
    host.applyCommitted();
    expect(node.snapshotIndex).toBe(5);

    // Entries 6 to 8 arrive after the snapshot. Their log positions no longer match their indexes.
    node.appendEntries(
      1,
      'leader',
      [set(6, 'k5', 5), set(7, 'k6', 6), set(8, 'k7', 7)],
      { prevLogIndex: 5, prevLogTerm: 1, leaderCommit: 8 },
    );
    expect(host.applyCommitted()).toBe(3);
    expect(store.lastApplied).toBe(8);
    expect(store.get('k7')).toBe(7);
  });

  it('uses a default threshold when none is given', () => {
    expect(DEFAULT_COMPACT_THRESHOLD).toBe(1_000);
    expect(() => new StateHost({ node: followerWith([], 0), store: new KeyValueStore() })).not.toThrow();
  });

  it('rejects a threshold that is not a positive integer', () => {
    const node = followerWith([], 0);
    for (const bad of [0, -1, 1.5]) {
      expect(() => new StateHost({ node, store: new KeyValueStore(), compactThreshold: bad })).toThrowError(RangeError);
    }
  });
});

describe('StateHost install', () => {
  const source = applied(Array.from({ length: 5 }, (_, i) => set(i + 1, `k${i}`, i, 2)));
  const offer = { index: 5, term: 2, data: source.image() };

  it('restores the store and moves the node to the snapshot index', () => {
    const node = followerWith([], 0);
    const store = new KeyValueStore();
    const host = new StateHost({ node, store });

    expect(host.install(offer)).toBe(true);
    expect(store.snapshot()).toEqual(source.snapshot());
    expect(node.snapshotIndex).toBe(5);
    expect(node.snapshotTerm).toBe(2);
    expect(node.commitIndex).toBe(5);
    expect(host.latestSnapshot).toMatchObject({ index: 5, term: 2 });
  });

  it('reports success without changes when the node already holds that state', () => {
    const node = followerWith([], 0);
    const store = new KeyValueStore();
    const host = new StateHost({ node, store });
    host.install(offer);

    expect(host.install({ ...offer, index: 3 })).toBe(true);
    expect(store.lastApplied).toBe(5);
  });

  it('leaves the store unchanged when the data does not match its index', () => {
    const node = followerWith([], 0);
    const store = applied([set(1, 'keep', 'me')]);
    const host = new StateHost({ node, store });

    expect(() => host.install({ index: 9, term: 2, data: source.image() })).toThrowError(/expected 9/);
    expect(store.lastApplied).toBe(1);
    expect(store.get('keep')).toBe('me');
    expect(node.snapshotIndex).toBe(0);
  });

  it('refuses malformed data without touching the store', () => {
    const store = new KeyValueStore();
    const host = new StateHost({ node: followerWith([], 0), store });
    expect(() => host.install({ index: 5, term: 2, data: 'garbage' })).toThrowError(TypeError);
    expect(store.lastApplied).toBe(0);
  });
});

describe('driver snapshot handling', () => {
  function driverFor(node: RaftConsensusNode, install: (snapshot: { index: number; term: number; data: unknown }) => boolean) {
    const onError = vi.fn();
    const driver = new ConsensusDriver({
      node,
      transport: unusedTransport,
      scheduler: idleScheduler,
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
      onError,
      installSnapshot: install,
    });
    return { driver, onError };
  }

  function request(term: number, index = 5): InstallSnapshotRequest {
    return {
      term,
      leaderId: 'leader',
      snapshot: { index, term: 2, data: applied([set(1, 'a', 1)]).image() },
    };
  }

  it('records the leader and reports the install when it succeeds', () => {
    const node = followerWith([], 0);
    const install = vi.fn(() => true);
    const { driver } = driverFor(node, install);

    expect(driver.handleSnapshotRequest(request(3))).toEqual({ term: 3, installed: true });
    expect(node.leaderId).toBe('leader');
    expect(node.currentTerm).toBe(3);
    expect(install).toHaveBeenCalledOnce();
  });

  it('rejects a snapshot from an older term without installing it', () => {
    const node = followerWith([], 0);
    node.currentTerm = 4;
    const install = vi.fn(() => true);
    const { driver } = driverFor(node, install);

    expect(driver.handleSnapshotRequest(request(3))).toEqual({ term: 4, installed: false });
    expect(install).not.toHaveBeenCalled();
  });

  it('reports not installed, and still recognises the leader, when the host throws', () => {
    const node = followerWith([], 0);
    const { driver, onError } = driverFor(node, () => {
      throw new TypeError('bad data');
    });

    expect(driver.handleSnapshotRequest(request(3))).toEqual({ term: 3, installed: false });
    expect(node.leaderId).toBe('leader');
    expect(onError).toHaveBeenCalledWith(expect.any(TypeError), 'installSnapshot');
  });

  it('reports not installed when the host has no snapshot support', () => {
    const node = followerWith([], 0);
    const driver = new ConsensusDriver({
      node,
      transport: unusedTransport,
      scheduler: idleScheduler,
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
    });
    expect(driver.handleSnapshotRequest(request(3)).installed).toBe(false);
  });
});

describe('leader snapshot bookkeeping', () => {
  it('counts a confirmed install as matched and moves nextIndex past it', () => {
    const node = new RaftConsensusNode({ nodeId: 'a', clusterPeers: ['b', 'c'], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
    node.start();
    node.startElection();
    node.recordVote('b', { term: node.currentTerm, voteGranted: true });
    expect(node.state).toBe('LEADER');

    node.handleSnapshotReply('b', node.currentTerm, 5, { term: node.currentTerm, installed: true });
    expect(node.peerNeedsSnapshot('b')).toBe(false);
  });

  it('ignores a reply that did not install, or that comes from another term', () => {
    const node = new RaftConsensusNode({ nodeId: 'a', clusterPeers: ['b', 'c'], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
    node.start();
    node.startElection();
    node.recordVote('b', { term: node.currentTerm, voteGranted: true });
    const term = node.currentTerm;

    node.handleSnapshotReply('b', term, 5, { term, installed: false });
    node.handleSnapshotReply('b', term - 1, 5, { term, installed: true });
    expect(node.state).toBe('LEADER');
  });

  it('steps down on a reply from a newer term', () => {
    const node = new RaftConsensusNode({ nodeId: 'a', clusterPeers: ['b', 'c'], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
    node.start();
    node.startElection();
    node.recordVote('b', { term: node.currentTerm, voteGranted: true });

    node.handleSnapshotReply('b', node.currentTerm, 5, { term: node.currentTerm + 1, installed: false });
    expect(node.state).toBe('FOLLOWER');
  });
});
