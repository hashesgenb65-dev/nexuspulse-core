/**
 * Tests for leader election: starting an election, counting votes, stepping down,
 * and the log-freshness rule that decides whether a vote is granted.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { RaftConsensusNode } from '../../src/mesh/consensus';

function makeNode(peers: string[] = ['node_2', 'node_3']): RaftConsensusNode {
  return new RaftConsensusNode({
    nodeId: 'node_1',
    clusterPeers: peers,
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
}

const GRANTED = { term: 1, voteGranted: true };
const DENIED = { term: 1, voteGranted: false };

describe('quorum size', () => {
  it('is a strict majority of the whole cluster, including this node', () => {
    expect(makeNode([]).quorumSize).toBe(1);
    expect(makeNode(['a', 'b']).quorumSize).toBe(2);
    expect(makeNode(['a', 'b', 'c']).quorumSize).toBe(3);
    expect(makeNode(['a', 'b', 'c', 'd']).quorumSize).toBe(3);
    expect(makeNode(['a', 'b', 'c', 'd', 'e', 'f']).quorumSize).toBe(4);
  });
});

describe('startElection', () => {
  let node: RaftConsensusNode;

  beforeEach(() => {
    node = makeNode();
    node.start();
  });

  it('moves to the next term as a candidate and votes for itself', () => {
    const { request, won } = node.startElection();
    expect(won).toBe(false);
    expect(node.state).toBe('CANDIDATE');
    expect(node.currentTerm).toBe(1);
    expect(node.votedFor).toBe('node_1');
    expect(request).toEqual({ term: 1, candidateId: 'node_1', lastLogIndex: 0, lastLogTerm: 0 });
  });

  it('describes the last log entry in its vote request', () => {
    node.appendEntries(3, 'node_2', [{ index: 1, term: 3, command: 'SET', payload: {} }]);
    const { request } = node.startElection();
    expect(request.term).toBe(4);
    expect(request.lastLogIndex).toBe(1);
    expect(request.lastLogTerm).toBe(3);
  });

  it('refuses to start when the node is not running', () => {
    node.stop();
    expect(() => node.startElection()).toThrowError(/is not running/);
    expect(node.currentTerm).toBe(0);
  });

  it('refuses to start while already the leader', () => {
    node.startElection();
    node.recordVote('node_2', GRANTED);
    expect(node.state).toBe('LEADER');
    expect(() => node.startElection()).toThrowError(/already the leader/);
  });

  it('clears the tally of an earlier election when it starts again', () => {
    node.startElection();
    node.recordVote('node_2', DENIED);
    const second = node.startElection();
    expect(second.request.term).toBe(2);
    expect(node.getStateSummary().votesReceived).toBe(1);
  });
});

describe('vote counting', () => {
  let node: RaftConsensusNode;

  beforeEach(() => {
    node = makeNode(['node_2', 'node_3']);
    node.start();
    node.startElection();
  });

  it('becomes leader once a majority of the cluster has voted for it', () => {
    expect(node.recordVote('node_2', GRANTED)).toBe(true);
    expect(node.state).toBe('LEADER');
    expect(node.leaderId).toBe('node_1');
  });

  it('stays a candidate when every peer refuses', () => {
    expect(node.recordVote('node_2', DENIED)).toBe(false);
    expect(node.recordVote('node_3', DENIED)).toBe(false);
    expect(node.state).toBe('CANDIDATE');
    expect(node.leaderId).toBeNull();
  });

  it('counts a peer only once even if it replies several times', () => {
    expect(node.recordVote('node_2', GRANTED)).toBe(true);

    const again = makeNode(['node_2', 'node_3', 'node_4', 'node_5']);
    again.start();
    again.startElection();
    again.recordVote('node_2', GRANTED);
    again.recordVote('node_2', GRANTED);
    again.recordVote('node_2', GRANTED);
    expect(again.state).toBe('CANDIDATE');
    expect(again.getStateSummary().votesReceived).toBe(2);
  });

  it('ignores a vote from a node that is not a peer', () => {
    expect(node.recordVote('stranger', GRANTED)).toBe(false);
    expect(node.state).toBe('CANDIDATE');
    expect(node.getStateSummary().votesReceived).toBe(1);
  });

  it('ignores a vote reply from an older term', () => {
    node.startElection(); // now in term 2
    expect(node.recordVote('node_2', { term: 1, voteGranted: true })).toBe(false);
    expect(node.state).toBe('CANDIDATE');
  });

  it('ignores replies once the node is no longer a candidate', () => {
    node.recordVote('node_2', GRANTED);
    expect(node.recordVote('node_3', GRANTED)).toBe(false);
    expect(node.state).toBe('LEADER');
  });

  it('steps down when a reply carries a newer term', () => {
    expect(node.recordVote('node_2', { term: 7, voteGranted: false })).toBe(false);
    expect(node.state).toBe('FOLLOWER');
    expect(node.currentTerm).toBe(7);
    expect(node.votedFor).toBeNull();
    expect(node.getStateSummary().votesReceived).toBe(0);
  });
});

describe('small and larger clusters', () => {
  it('wins a one-node cluster on its own vote', () => {
    const solo = makeNode([]);
    solo.start();
    const { won } = solo.startElection();
    expect(won).toBe(true);
    expect(solo.state).toBe('LEADER');
    expect(solo.leaderId).toBe('node_1');
  });

  it('needs three votes in a five-node cluster, not two', () => {
    const five = makeNode(['node_2', 'node_3', 'node_4', 'node_5']);
    five.start();
    five.startElection();
    expect(five.recordVote('node_2', GRANTED)).toBe(false);
    expect(five.state).toBe('CANDIDATE');
    expect(five.recordVote('node_3', GRANTED)).toBe(true);
    expect(five.state).toBe('LEADER');
  });
});

describe('a leader', () => {
  it('can propose commands once elected', () => {
    const node = makeNode();
    node.start();
    node.startElection();
    node.recordVote('node_2', GRANTED);

    const entry = node.proposeCommand('SET', { key: 'a' });
    expect(entry).toEqual({ index: 1, term: 1, command: 'SET', payload: { key: 'a' } });
    expect(node.log).toHaveLength(1);
  });
});

describe('vote granting and log freshness', () => {
  it('refuses a candidate whose last log term is older than ours (section 5.4.1)', () => {
    const node = makeNode();
    node.appendEntries(2, 'node_2', [{ index: 1, term: 2, command: 'SET', payload: {} }]);

    // Candidate is at term 3 but its last entry is from term 1, so its log is stale.
    expect(node.requestVote(3, 'node_3', 5, 1)).toEqual({ term: 3, voteGranted: false });
    expect(node.votedFor).toBeNull();
  });

  it('grants a vote to a candidate with an equally up-to-date log', () => {
    const node = makeNode();
    node.appendEntries(2, 'node_2', [{ index: 1, term: 2, command: 'SET', payload: {} }]);
    expect(node.requestVote(3, 'node_3', 1, 2)).toEqual({ term: 3, voteGranted: true });
  });

  it('does not vote twice in the same term for different candidates', () => {
    const node = makeNode();
    expect(node.requestVote(1, 'node_2', 0, 0).voteGranted).toBe(true);
    expect(node.requestVote(1, 'node_3', 0, 0).voteGranted).toBe(false);
  });
});

describe('AppendEntries after a higher term', () => {
  it('clears the earlier vote when a newer term arrives from a leader', () => {
    const node = makeNode();
    node.requestVote(1, 'node_3', 0, 0);
    expect(node.votedFor).toBe('node_3');

    node.appendEntries(2, 'node_2', []);
    expect(node.votedFor).toBeNull();
    expect(node.leaderId).toBe('node_2');
  });

  it('turns a candidate into a follower on an AppendEntries of the same term', () => {
    const node = makeNode();
    node.start();
    node.startElection();
    expect(node.state).toBe('CANDIDATE');

    node.appendEntries(1, 'node_2', []);
    expect(node.state).toBe('FOLLOWER');
    expect(node.leaderId).toBe('node_2');
    expect(node.getStateSummary().votesReceived).toBe(0);
  });
});
