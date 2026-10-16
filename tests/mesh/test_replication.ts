/**
 * Tests for log replication: the follower's AppendEntries checks, conflict repair,
 * commit index movement, and the leader's bookkeeping for each peer.
 */

import { describe, it, expect } from 'vitest';
import { RaftConsensusNode, type LogEntry } from '../../src/mesh/consensus';

function entry(index: number, term: number, command = 'SET'): LogEntry {
  return { index, term, command, payload: { index } };
}

function makeNode(peers: string[] = ['node_2', 'node_3']): RaftConsensusNode {
  const node = new RaftConsensusNode({
    nodeId: 'node_1',
    clusterPeers: peers,
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
  node.start();
  return node;
}

/** Makes `node` the leader of its cluster by winning a majority of votes. */
function electLeader(node: RaftConsensusNode, votes: string[]): void {
  node.startElection();
  for (const peer of votes) {
    node.recordVote(peer, { term: node.currentTerm, voteGranted: true });
  }
  if (node.state !== 'LEADER') throw new Error('test setup: node did not win the election');
}

describe('follower: consistency check', () => {
  it('rejects entries when the entry at prevLogIndex is missing', () => {
    const follower = makeNode();
    const reply = follower.appendEntries(1, 'node_2', [entry(4, 1)], { prevLogIndex: 3, prevLogTerm: 1 });
    expect(reply.success).toBe(false);
    expect(follower.log).toHaveLength(0);
  });

  it('rejects entries when the term at prevLogIndex differs', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1)]);

    const reply = follower.appendEntries(2, 'node_2', [entry(3, 2)], { prevLogIndex: 2, prevLogTerm: 2 });
    expect(reply.success).toBe(false);
    expect(follower.log).toHaveLength(2);
  });

  it('still records the leader and the new term when it rejects the entries', () => {
    const follower = makeNode();
    follower.appendEntries(5, 'node_3', [entry(2, 5)], { prevLogIndex: 1, prevLogTerm: 5 });
    expect(follower.currentTerm).toBe(5);
    expect(follower.leaderId).toBe('node_3');
    expect(follower.state).toBe('FOLLOWER');
  });

  it('accepts entries that follow a matching anchor', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1)]);
    const reply = follower.appendEntries(1, 'node_2', [entry(2, 1)], { prevLogIndex: 1, prevLogTerm: 1 });
    expect(reply.success).toBe(true);
    expect(follower.log.map((e) => e.index)).toEqual([1, 2]);
  });

  it('rejects a request whose entries do not start right after prevLogIndex', () => {
    const follower = makeNode();
    const reply = follower.appendEntries(1, 'node_2', [entry(3, 1)], { prevLogIndex: 1, prevLogTerm: 0 });
    expect(reply.success).toBe(false);
    expect(follower.log).toHaveLength(0);
  });

  it('rejects a request with a gap between its entries', () => {
    const follower = makeNode();
    const reply = follower.appendEntries(1, 'node_2', [entry(1, 1), entry(3, 1)]);
    expect(reply.success).toBe(false);
    expect(follower.log).toHaveLength(0);
  });

  it('rejects a request from an older term without touching the log', () => {
    const follower = makeNode();
    follower.appendEntries(4, 'node_2', [entry(1, 4)]);
    const reply = follower.appendEntries(3, 'node_3', [entry(2, 3)], { prevLogIndex: 1, prevLogTerm: 4 });
    expect(reply).toEqual({ term: 4, success: false });
    expect(follower.leaderId).toBe('node_2');
    expect(follower.log).toHaveLength(1);
  });
});

describe('follower: repairing conflicts', () => {
  it('replaces a conflicting entry and everything after it', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1), entry(3, 1)]);

    const reply = follower.appendEntries(2, 'node_3', [entry(2, 2, 'NEW'), entry(3, 2, 'NEW')], {
      prevLogIndex: 1,
      prevLogTerm: 1,
    });

    expect(reply.success).toBe(true);
    expect(follower.log.map((e) => [e.index, e.term, e.command])).toEqual([
      [1, 1, 'SET'],
      [2, 2, 'NEW'],
      [3, 2, 'NEW'],
    ]);
  });

  it('keeps matching entries instead of duplicating them', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1)]);

    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1), entry(3, 1)]);
    expect(follower.log.map((e) => e.index)).toEqual([1, 2, 3]);
  });

  it('does not truncate when a shorter retransmitted request matches a prefix', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1), entry(3, 1)]);

    // A retransmitted shorter request with matching entries must not remove anything.
    follower.appendEntries(1, 'node_2', [entry(1, 1)]);
    expect(follower.log).toHaveLength(3);
  });
});

describe('follower: commit index', () => {
  it('advances to leaderCommit but not past the last entry of the request', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1)], { leaderCommit: 5 });
    expect(follower.commitIndex).toBe(2);
  });

  it('follows leaderCommit when it is smaller than the last entry', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1), entry(3, 1)], { leaderCommit: 1 });
    expect(follower.commitIndex).toBe(1);
  });

  it('never moves commitIndex backwards', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1), entry(3, 1)], { leaderCommit: 3 });
    follower.appendEntries(1, 'node_2', [], { prevLogIndex: 3, prevLogTerm: 1, leaderCommit: 1 });
    expect(follower.commitIndex).toBe(3);
  });

  it('a heartbeat can raise commitIndex once the follower has the entries', () => {
    const follower = makeNode();
    follower.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1)]);
    expect(follower.commitIndex).toBe(0);

    follower.appendEntries(1, 'node_2', [], { prevLogIndex: 2, prevLogTerm: 1, leaderCommit: 2 });
    expect(follower.commitIndex).toBe(2);
  });
});

describe('leader: building requests', () => {
  it('sends the whole log to a peer that has nothing yet', () => {
    const leader = makeNode(['node_2', 'node_3']);
    electLeader(leader, ['node_2']);
    leader.proposeCommand('SET', { a: 1 });
    leader.proposeCommand('SET', { a: 2 });

    const request = leader.buildAppendRequest('node_2');
    expect(request.term).toBe(1);
    expect(request.leaderId).toBe('node_1');
    expect(request.prevLogIndex).toBe(0);
    expect(request.prevLogTerm).toBe(0);
    expect(request.entries.map((e) => e.index)).toEqual([1, 2]);
  });

  it('sends an empty heartbeat when the peer is up to date', () => {
    const leader = makeNode(['node_2', 'node_3']);
    electLeader(leader, ['node_2']);
    leader.proposeCommand('SET', {});

    const first = leader.buildAppendRequest('node_2');
    leader.handleAppendReply('node_2', first, { term: 1, success: true });

    const heartbeat = leader.buildAppendRequest('node_2');
    expect(heartbeat.entries).toEqual([]);
    expect(heartbeat.prevLogIndex).toBe(1);
    expect(heartbeat.prevLogTerm).toBe(1);
  });

  it('refuses to build a request when not the leader', () => {
    expect(() => makeNode().buildAppendRequest('node_2')).toThrowError(/is not the cluster leader/);
  });

  it('refuses to build a request for an unknown peer', () => {
    const leader = makeNode(['node_2', 'node_3']);
    electLeader(leader, ['node_2']);
    expect(() => leader.buildAppendRequest('stranger')).toThrowError(/is not a peer/);
  });
});

describe('leader: handling replies', () => {
  it('commits once a majority holds the entry', () => {
    const leader = makeNode(['node_2', 'node_3']);
    electLeader(leader, ['node_2']);
    leader.proposeCommand('SET', {});
    leader.proposeCommand('SET', {});

    const request = leader.buildAppendRequest('node_2');
    const commit = leader.handleAppendReply('node_2', request, { term: 1, success: true });

    // The leader and node_2 hold entries 1 and 2, which is a majority of three.
    expect(commit).toBe(2);
    expect(leader.commitIndex).toBe(2);
  });

  it('does not commit with only the leader holding the entry in a five-node cluster', () => {
    const leader = makeNode(['node_2', 'node_3', 'node_4', 'node_5']);
    electLeader(leader, ['node_2', 'node_3']);
    leader.proposeCommand('SET', {});

    const request = leader.buildAppendRequest('node_2');
    leader.handleAppendReply('node_2', request, { term: 1, success: true });
    expect(leader.commitIndex).toBe(0);

    const second = leader.buildAppendRequest('node_3');
    leader.handleAppendReply('node_3', second, { term: 1, success: true });
    expect(leader.commitIndex).toBe(1);
  });

  it('moves nextIndex back on a rejection and retries with an earlier prefix', () => {
    const leader = makeNode(['node_2', 'node_3']);
    leader.appendEntries(1, 'node_2', [entry(1, 1)]);
    // Winning term 2 starts nextIndex for each peer just after the leader's one entry.
    electLeader(leader, ['node_2']);

    // nextIndex for node_2 is 2, so the first request is a heartbeat anchored at index 1. Reject it.
    const rejected = leader.buildAppendRequest('node_2');
    expect(rejected.prevLogIndex).toBe(1);
    leader.handleAppendReply('node_2', rejected, { term: 2, success: false });

    const retry = leader.buildAppendRequest('node_2');
    expect(retry.prevLogIndex).toBe(0);
    expect(retry.entries.map((e) => e.index)).toEqual([1]);
  });

  it('ignores a stale rejection that belongs to an earlier request', () => {
    const leader = makeNode(['node_2', 'node_3']);
    leader.appendEntries(1, 'node_2', [entry(1, 1), entry(2, 1)]);
    electLeader(leader, ['node_2']);

    const old = leader.buildAppendRequest('node_2'); // anchored at index 2, the leader's whole log
    leader.handleAppendReply('node_2', old, { term: 2, success: true });
    const current = leader.buildAppendRequest('node_2');

    // A rejection of `old` arrives late. The peer has acknowledged index 2, so it cannot be right.
    leader.handleAppendReply('node_2', old, { term: 2, success: false });
    expect(leader.buildAppendRequest('node_2')).toEqual(current);
  });

  it('steps down when a reply carries a newer term', () => {
    const leader = makeNode(['node_2', 'node_3']);
    electLeader(leader, ['node_2']);
    const request = leader.buildAppendRequest('node_2');

    leader.handleAppendReply('node_2', request, { term: 9, success: false });
    expect(leader.state).toBe('FOLLOWER');
    expect(leader.currentTerm).toBe(9);
  });

  it('ignores replies from an older term', () => {
    const leader = makeNode(['node_2', 'node_3']);
    electLeader(leader, ['node_2']);
    leader.proposeCommand('SET', {});
    const request = leader.buildAppendRequest('node_2');

    const commit = leader.handleAppendReply('node_2', request, { term: 0, success: true });
    expect(commit).toBe(0);
    expect(leader.state).toBe('LEADER');
  });
});

describe('leader: committing only entries from its own term', () => {
  it('does not commit an earlier-term entry by counting replicas, then commits it with a current-term entry', () => {
    // node_1 holds an entry from term 1, then wins term 2 without writing anything new.
    const leader = makeNode(['node_2', 'node_3']);
    leader.appendEntries(1, 'node_2', [entry(1, 1)]);
    electLeader(leader, ['node_2']);
    expect(leader.currentTerm).toBe(2);

    // node_2 now also holds entry 1, which is a majority, but that entry is from term 1.
    const catchUp = leader.buildAppendRequest('node_2');
    expect(catchUp.prevLogIndex).toBe(1);
    leader.handleAppendReply('node_2', catchUp, { term: 2, success: true });
    expect(leader.commitIndex).toBe(0);

    // Once a term-2 entry is replicated to a majority, both entries commit together.
    leader.proposeCommand('SET', { fresh: true });
    const next = leader.buildAppendRequest('node_2');
    leader.handleAppendReply('node_2', next, { term: 2, success: true });
    expect(leader.commitIndex).toBe(2);
  });
});

describe('leader and follower together', () => {
  it('brings a lagging follower up to date and commits the entries', () => {
    const leader = makeNode(['node_2', 'node_3']);
    electLeader(leader, ['node_2']);
    for (let i = 0; i < 5; i++) leader.proposeCommand('SET', { i });

    const follower = makeNode(['node_1', 'node_3']);
    let reply = { term: 0, success: false };
    for (let round = 0; round < 10 && (!reply.success || follower.log.length < 5); round++) {
      const request = leader.buildAppendRequest('node_2');
      reply = follower.appendEntries(request.term, request.leaderId, request.entries, {
        prevLogIndex: request.prevLogIndex,
        prevLogTerm: request.prevLogTerm,
        leaderCommit: request.leaderCommit,
      });
      leader.handleAppendReply('node_2', request, reply);
    }

    expect(follower.log.map((e) => e.index)).toEqual([1, 2, 3, 4, 5]);
    expect(leader.commitIndex).toBe(5);
  });
});
