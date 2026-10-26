/**
 * AppendEntries size cap: a long uncompacted log tail is sent in several requests, each under the size
 * limit, and a successful reply acknowledges exactly what its request carried.
 */

import { describe, it, expect } from 'vitest';
import { MAX_APPEND_ENTRY_BYTES, RaftConsensusNode } from '../../src/mesh/consensus';

const FRAME_LIMIT_BYTES = 1 << 20;

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

function electLeader(node: RaftConsensusNode, votes: string[]): void {
  node.startElection();
  for (const peer of votes) {
    node.recordVote(peer, { term: node.currentTerm, voteGranted: true });
  }
}

function bytesOf(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), 'utf8');
}

describe('AppendEntries size cap', () => {
  it('sends fewer entries than the log holds when they would exceed the cap', () => {
    const leader = makeNode();
    electLeader(leader, ['node_2']);
    const value = 'x'.repeat(100 * 1024);
    for (let i = 0; i < 40; i++) leader.proposeCommand('SET', { key: `k${i}`, value });

    const request = leader.buildAppendRequest('node_2');
    expect(request.entries.length).toBeGreaterThan(0);
    expect(request.entries.length).toBeLessThan(41);
    expect(request.entries[0]?.index).toBe(1);
    expect(bytesOf(request.entries)).toBeLessThanOrEqual(MAX_APPEND_ENTRY_BYTES);
  });

  it('keeps every request under the peer frame limit', () => {
    const leader = makeNode();
    electLeader(leader, ['node_2']);
    const value = 'x'.repeat(100 * 1024);
    for (let i = 0; i < 40; i++) leader.proposeCommand('SET', { key: `k${i}`, value });

    const request = leader.buildAppendRequest('node_2');
    expect(bytesOf(request)).toBeLessThan(FRAME_LIMIT_BYTES);
  });

  it('delivers a long tail over several successful replies, with nothing lost or repeated', () => {
    const leader = makeNode();
    electLeader(leader, ['node_2']);
    const value = 'x'.repeat(100 * 1024);
    for (let i = 0; i < 40; i++) leader.proposeCommand('SET', { key: `k${i}`, value });
    const lastIndex = leader.lastIndex;

    let requests = 0;
    let delivered = 0;
    for (;;) {
      const request = leader.buildAppendRequest('node_2');
      if (request.entries.length === 0) break; // the peer is caught up
      expect(request.prevLogIndex).toBe(delivered);
      expect(request.entries[0]?.index).toBe(delivered + 1);
      delivered += request.entries.length;
      leader.handleAppendReply('node_2', request, { term: leader.currentTerm, success: true });
      requests++;
      if (requests > 100) throw new Error('replication did not finish');
    }
    expect(requests).toBeGreaterThan(1);
    expect(delivered).toBe(lastIndex);
    expect(leader.commitIndex).toBe(lastIndex);
  });

  it('still sends one entry on its own when that entry is over the cap', () => {
    const leader = makeNode();
    electLeader(leader, ['node_2']);
    leader.proposeCommand('SET', { key: 'big', value: 'x'.repeat(MAX_APPEND_ENTRY_BYTES + 1024) });

    const request = leader.buildAppendRequest('node_2');
    expect(request.entries.map((entry) => entry.index)).toEqual([1]);
    expect(bytesOf(request.entries[0])).toBeGreaterThan(MAX_APPEND_ENTRY_BYTES);
  });

  it('leaves small requests unchanged', () => {
    const leader = makeNode();
    electLeader(leader, ['node_2']);
    leader.proposeCommand('SET', { a: 1 });
    leader.proposeCommand('SET', { a: 2 });

    const request = leader.buildAppendRequest('node_2');
    expect(request.entries.map((entry) => entry.index)).toEqual([1, 2]);
  });
});
