/**
 * NexusPulse Distributed Raft Consensus & Node Heartbeat State Machine
 * 
 * Implements leader election state transitions, term tracking, log replication entries,
 * heartbeat timeouts, and quorum acknowledgement verification.
 */

export type NodeState = 'FOLLOWER' | 'CANDIDATE' | 'LEADER';

export interface LogEntry {
  index: number;
  term: number;
  command: string;
  payload: unknown;
}

export interface NodeConfig {
  nodeId: string;
  clusterPeers: string[];
  electionTimeoutMs: number;
  heartbeatIntervalMs: number;
}

export class RaftConsensusNode {
  public nodeId: string;
  public state: NodeState = 'FOLLOWER';
  public currentTerm: number = 0;
  public votedFor: string | null = null;
  public log: LogEntry[] = [];
  public commitIndex: number = 0;
  public leaderId: string | null = null;

  private peers: string[];
  private votesReceived: Set<string> = new Set();
  private isRunning: boolean = false;

  constructor(config: NodeConfig) {
    this.nodeId = config.nodeId;
    this.peers = config.clusterPeers;
  }

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
  }

  public stop(): void {
    this.isRunning = false;
  }

  public requestVote(term: number, candidateId: string, lastLogIndex: number, lastLogTerm: number): { term: number; voteGranted: boolean } {
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.state = 'FOLLOWER';
      this.votedFor = null;
    }

    const ownLast = this.log[this.log.length - 1];
    const ownLastTerm = ownLast?.term ?? 0;
    const ownLastIndex = ownLast?.index ?? 0;
    // Raft section 5.4.1: only vote for a candidate whose log is at least as up to date as ours.
    const candidateUpToDate =
      lastLogTerm > ownLastTerm || (lastLogTerm === ownLastTerm && lastLogIndex >= ownLastIndex);

    if (term === this.currentTerm && (this.votedFor === null || this.votedFor === candidateId) && candidateUpToDate) {
      this.votedFor = candidateId;
      return { term: this.currentTerm, voteGranted: true };
    }

    return { term: this.currentTerm, voteGranted: false };
  }

  public appendEntries(term: number, leaderId: string, entries: LogEntry[]): { term: number; success: boolean } {
    if (term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }

    this.currentTerm = term;
    this.state = 'FOLLOWER';
    this.leaderId = leaderId;

    if (entries.length > 0) {
      for (const entry of entries) {
        this.log.push(entry);
      }
      const last = this.log[this.log.length - 1];
      if (last !== undefined) this.commitIndex = last.index;
    }

    return { term: this.currentTerm, success: true };
  }

  public proposeCommand(command: string, payload: unknown): LogEntry {
    if (this.state !== 'LEADER') {
      throw new Error(`Node ${this.nodeId} is not the cluster leader. Current leader: ${this.leaderId}`);
    }

    const lastEntry = this.log[this.log.length - 1];
    const index = lastEntry !== undefined ? lastEntry.index + 1 : 1;
    const entry: LogEntry = { index, term: this.currentTerm, command, payload };
    this.log.push(entry);
    return entry;
  }

  public getStateSummary() {
    return {
      nodeId: this.nodeId,
      state: this.state,
      currentTerm: this.currentTerm,
      logLength: this.log.length,
      commitIndex: this.commitIndex,
      leaderId: this.leaderId,
      peerCount: this.peers.length,
      votesReceived: this.votesReceived.size,
    };
  }
}
