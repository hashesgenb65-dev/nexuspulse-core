import { describe, it, expect, beforeEach } from 'vitest';
import { RaftConsensusNode } from '../../src/mesh/consensus';

describe('RaftConsensusNode Module', () => {
  let node: RaftConsensusNode;

  beforeEach(() => {
    node = new RaftConsensusNode({
      nodeId: 'node_1',
      clusterPeers: ['node_2', 'node_3'],
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
    });
  });

  it('should initialize as a FOLLOWER with term 0', () => {
    expect(node.state).toBe('FOLLOWER');
    expect(node.currentTerm).toBe(0);
    expect(node.leaderId).toBeNull();
  });

  it('should grant vote to valid candidate request', () => {
    const res = node.requestVote(1, 'node_2', 0, 0);
    expect(res.voteGranted).toBe(true);
    expect(node.currentTerm).toBe(1);
    expect(node.votedFor).toBe('node_2');
  });

  it('should accept append entries from active leader', () => {
    const res = node.appendEntries(2, 'node_3', [
      { index: 1, term: 2, command: 'SET', payload: { key: 'a', val: 1 } },
    ]);

    expect(res.success).toBe(true);
    expect(node.state).toBe('FOLLOWER');
    expect(node.leaderId).toBe('node_3');
    expect(node.log).toHaveLength(1);
  });

  it('should reject command proposals when not in LEADER state', () => {
    expect(() => node.proposeCommand('SET', { x: 1 })).toThrowError(/is not the cluster leader/);
  });
});
