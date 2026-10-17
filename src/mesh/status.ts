/**
 * Status of one mesh member, served at GET /v1/status. It reads from the node and store as they are
 * now; it does not contact peers, so it reports what this member knows, not the cluster's live health.
 */

import type { NodeState, RaftConsensusNode } from './consensus.js';
import type { KeyValueStore } from './state_machine.js';

export const STATUS_PATH = '/v1/status';

export interface NodeStatus {
  readonly nodeId: string;
  readonly role: NodeState;
  readonly term: number;
  readonly leaderId: string | null;
  readonly votedFor: string | null;
  /** Index of the last log entry, counting entries folded into the snapshot. */
  readonly lastLogIndex: number;
  readonly commitIndex: number;
  /** Index of the last entry applied to the store. Lags commitIndex by the entries still being applied. */
  readonly appliedIndex: number;
  readonly snapshotIndex: number;
  /** Entries still held in memory above the snapshot. */
  readonly logLength: number;
  readonly peers: readonly string[];
  /** Votes needed to win an election or commit an entry. */
  readonly quorumSize: number;
  /** True when the member writes its state to disk. */
  readonly durable: boolean;
}

/** Reads the member's status. Pure: nothing is changed. */
export function nodeStatus(node: RaftConsensusNode, store: KeyValueStore, durable: boolean): NodeStatus {
  return {
    nodeId: node.nodeId,
    role: node.state,
    term: node.currentTerm,
    leaderId: node.leaderId,
    votedFor: node.votedFor,
    lastLogIndex: node.lastIndex,
    commitIndex: node.commitIndex,
    appliedIndex: store.lastApplied,
    snapshotIndex: node.snapshotIndex,
    logLength: node.log.length,
    peers: [...node.peerIds],
    quorumSize: node.quorumSize,
    durable,
  };
}
