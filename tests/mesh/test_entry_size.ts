/**
 * Entry size limit: a command over MAX_ENTRY_BYTES is refused when it is proposed, nothing is appended,
 * and a command exactly at the limit is accepted.
 */

import { describe, it, expect } from 'vitest';
import { EntryTooLargeError, MAX_ENTRY_BYTES, RaftConsensusNode } from '../../src/mesh/consensus';

const FRAME_LIMIT_BYTES = 1 << 20;

function leaderNode(): RaftConsensusNode {
  const node = new RaftConsensusNode({
    nodeId: 'node_1',
    clusterPeers: ['node_2', 'node_3'],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
  node.start();
  node.startElection();
  node.recordVote('node_2', { term: node.currentTerm, voteGranted: true });
  return node;
}

/** A payload whose command and payload together are exactly `bytes` long, in ASCII. */
function payloadOfSize(bytes: number): { value: string } {
  const empty = Buffer.byteLength(JSON.stringify({ command: 'SET', payload: { value: '' } }), 'utf8');
  return { value: 'x'.repeat(bytes - empty) };
}

describe('entry size limit', () => {
  it('accepts a command exactly at the limit', () => {
    const leader = leaderNode();
    const before = leader.lastIndex;
    const entry = leader.proposeCommand('SET', payloadOfSize(MAX_ENTRY_BYTES));
    expect(entry.index).toBe(before + 1);
  });

  it('refuses a command one byte over the limit, and appends nothing', () => {
    const leader = leaderNode();
    const before = leader.lastIndex;
    const logLength = leader.log.length;

    expect(() => leader.proposeCommand('SET', payloadOfSize(MAX_ENTRY_BYTES + 1))).toThrow(EntryTooLargeError);
    expect(leader.lastIndex).toBe(before);
    expect(leader.log.length).toBe(logLength);
  });

  it('reports the refusal with a stable code', () => {
    const leader = leaderNode();
    try {
      leader.proposeCommand('SET', payloadOfSize(MAX_ENTRY_BYTES + 100));
      throw new Error('expected a refusal');
    } catch (error) {
      expect(error).toBeInstanceOf(EntryTooLargeError);
      expect((error as EntryTooLargeError).code).toBe('ENTRY_TOO_LARGE');
    }
  });

  it('leaves room under the peer frame limit for the request around an entry at the limit', () => {
    expect(MAX_ENTRY_BYTES + 64 * 1024).toBeLessThan(FRAME_LIMIT_BYTES);
  });

  it('still reports a non-leader as a non-leader, even for a command that is too large', () => {
    const follower = new RaftConsensusNode({
      nodeId: 'node_9',
      clusterPeers: ['node_2', 'node_3'],
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
    });
    expect(() => follower.proposeCommand('SET', payloadOfSize(MAX_ENTRY_BYTES + 1))).toThrow(/not the cluster leader/);
    expect(() => follower.proposeCommand('SET', payloadOfSize(MAX_ENTRY_BYTES + 1))).not.toThrow(EntryTooLargeError);
  });
});
