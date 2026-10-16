/**
 * NexusPulse Distributed Raft Consensus & Node Heartbeat State Machine
 *
 * Implements leader election state transitions, term tracking, log replication entries,
 * heartbeat timeouts, and quorum acknowledgement verification.
 *
 * Election. {@link RaftConsensusNode.startElection} moves a node to the next term as a
 * candidate. The caller sends the returned {@link VoteRequest} to every peer and passes each
 * reply to {@link RaftConsensusNode.recordVote}, which counts distinct granted votes and
 * promotes the node to leader once a strict majority of the cluster has voted for it.
 *
 * Replication. The leader keeps a nextIndex and a matchIndex per peer. It builds each
 * AppendEntries with {@link RaftConsensusNode.buildAppendRequest} and feeds each reply to
 * {@link RaftConsensusNode.handleAppendReply}. Followers check prevLogIndex and prevLogTerm
 * (section 5.3), replace conflicting entries, and advance commitIndex from the leader's
 * commit index. The leader commits only entries from its own term (section 5.4.2).
 *
 * Log indices are 1-based and contiguous: the entry at position p has index p + 1.
 *
 * Not implemented here: election and heartbeat timers, and the network transport.
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

/** A RequestVote message sent by a candidate to each peer. */
export interface VoteRequest {
  term: number;
  candidateId: string;
  lastLogIndex: number;
  lastLogTerm: number;
}

/** A peer's reply to a {@link VoteRequest}, or the result of a vote check. */
export interface VoteResponse {
  term: number;
  voteGranted: boolean;
}

/** The outcome of {@link RaftConsensusNode.startElection}. */
export interface ElectionStart {
  /** Send this to every peer. */
  readonly request: VoteRequest;
  /** True when the candidate already holds a majority, which happens in a one-node cluster. */
  readonly won: boolean;
}

/** An AppendEntries message sent by the leader to one peer. */
export interface AppendRequest {
  term: number;
  leaderId: string;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

/** A follower's reply to an {@link AppendRequest}. */
export interface AppendReply {
  term: number;
  success: boolean;
}

/** The consistency fields of an AppendEntries call. Each defaults to 0, the sentinel for an empty log. */
export interface AppendPrev {
  prevLogIndex?: number;
  prevLogTerm?: number;
  leaderCommit?: number;
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
  /** Leader only: the next index to send to each peer. */
  private nextIndex: Map<string, number> = new Map();
  /** Leader only: the highest index known to be replicated on each peer. */
  private matchIndex: Map<string, number> = new Map();

  constructor(config: NodeConfig) {
    this.nodeId = config.nodeId;
    this.peers = config.clusterPeers;
  }

  /** Votes needed to win an election: a strict majority of the whole cluster, including this node. */
  public get quorumSize(): number {
    return Math.floor((this.peers.length + 1) / 2) + 1;
  }

  public start(): void {
    if (this.isRunning) return;
    this.isRunning = true;
  }

  public stop(): void {
    this.isRunning = false;
  }

  public requestVote(term: number, candidateId: string, lastLogIndex: number, lastLogTerm: number): VoteResponse {
    if (term > this.currentTerm) {
      this.stepDown(term);
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

  /**
   * Starts an election: moves to the next term, becomes a candidate and votes for itself.
   *
   * @throws {Error} If the node is not running or is already the leader.
   */
  public startElection(): ElectionStart {
    if (!this.isRunning) throw new Error(`Node ${this.nodeId} is not running`);
    if (this.state === 'LEADER') throw new Error(`Node ${this.nodeId} is already the leader`);

    this.currentTerm += 1;
    this.state = 'CANDIDATE';
    this.votedFor = this.nodeId;
    this.leaderId = null;
    this.votesReceived = new Set([this.nodeId]);

    const last = this.log[this.log.length - 1];
    const request: VoteRequest = {
      term: this.currentTerm,
      candidateId: this.nodeId,
      lastLogIndex: last?.index ?? 0,
      lastLogTerm: last?.term ?? 0,
    };

    const won = this.votesReceived.size >= this.quorumSize;
    if (won) this.becomeLeader();
    return { request, won };
  }

  /**
   * Records a vote reply from a peer. Returns true if this reply made the node leader.
   *
   * - A reply with a newer term makes the node step down and is otherwise ignored.
   * - A reply from an older term, from a node that is not a peer, or received while the
   *   node is not a candidate is ignored.
   * - A peer is counted once, however many times it replies.
   */
  public recordVote(peerId: string, reply: VoteResponse): boolean {
    if (reply.term > this.currentTerm) {
      this.stepDown(reply.term);
      return false;
    }
    if (this.state !== 'CANDIDATE' || reply.term !== this.currentTerm) return false;
    if (!this.peers.includes(peerId)) return false;

    if (reply.voteGranted) this.votesReceived.add(peerId);
    if (this.votesReceived.size < this.quorumSize) return false;

    this.becomeLeader();
    return true;
  }

  /**
   * Follower side of AppendEntries (Raft section 5.3).
   *
   * The leader's term and identity are recorded even when the entries are rejected, so the
   * node knows who the leader is. The consistency check then requires an entry at prevLogIndex
   * with term prevLogTerm. Entries that already match are skipped. An entry whose term differs
   * from the one at the same index replaces it, together with everything after it. commitIndex
   * moves up to the smaller of leaderCommit and the index of the last entry in this request.
   *
   * With no `prev` fields the call behaves as if the log were empty, so existing callers that
   * send entries from index 1 still work.
   */
  public appendEntries(
    term: number,
    leaderId: string,
    entries: LogEntry[],
    prev: AppendPrev = {},
  ): AppendReply {
    if (term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }

    if (term > this.currentTerm) {
      this.stepDown(term);
    } else {
      this.state = 'FOLLOWER';
      this.votesReceived = new Set();
    }
    this.leaderId = leaderId;

    const prevLogIndex = prev.prevLogIndex ?? 0;
    const prevLogTerm = prev.prevLogTerm ?? 0;

    if (prevLogIndex > 0) {
      const anchor = this.entryAt(prevLogIndex);
      if (anchor === undefined || anchor.term !== prevLogTerm) {
        return { term: this.currentTerm, success: false };
      }
    }

    // Entries must carry the indices they would occupy directly after prevLogIndex.
    for (const [offset, entry] of entries.entries()) {
      if (entry.index !== prevLogIndex + 1 + offset) {
        return { term: this.currentTerm, success: false };
      }
    }

    for (const entry of entries) {
      const existing = this.entryAt(entry.index);
      if (existing !== undefined) {
        if (existing.term === entry.term) continue;
        this.truncateFrom(entry.index);
      }
      this.log.push(entry);
    }

    const leaderCommit = prev.leaderCommit ?? 0;
    const lastNewIndex = prevLogIndex + entries.length;
    if (leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(leaderCommit, lastNewIndex);
    }

    return { term: this.currentTerm, success: true };
  }

  /**
   * Builds the AppendEntries message for one peer, starting at that peer's nextIndex.
   * An empty entry list is a heartbeat.
   *
   * @throws {Error} If this node is not the leader or the peer is unknown.
   */
  public buildAppendRequest(peerId: string): AppendRequest {
    if (this.state !== 'LEADER') throw new Error(`Node ${this.nodeId} is not the cluster leader`);
    const next = this.nextIndex.get(peerId);
    if (next === undefined) throw new Error(`Node ${peerId} is not a peer of ${this.nodeId}`);

    const prevLogIndex = next - 1;
    const prevEntry = prevLogIndex > 0 ? this.entryAt(prevLogIndex) : undefined;
    return {
      term: this.currentTerm,
      leaderId: this.nodeId,
      prevLogIndex,
      prevLogTerm: prevEntry?.term ?? 0,
      entries: this.log.slice(prevLogIndex),
      leaderCommit: this.commitIndex,
    };
  }

  /**
   * Applies a follower's reply to the request that produced it. Returns the leader's commitIndex
   * after the reply is applied.
   *
   * - A reply with a newer term makes the node step down.
   * - A success moves the peer's matchIndex forward and may advance commitIndex.
   * - A rejection moves nextIndex back by one, but only if the rejected request is still the
   *   one outstanding for that peer. Stale rejections are ignored.
   * - Replies from an older term, or received while not the leader, are ignored.
   */
  public handleAppendReply(peerId: string, request: AppendRequest, reply: AppendReply): number {
    if (reply.term > this.currentTerm) {
      this.stepDown(reply.term);
      return this.commitIndex;
    }
    if (this.state !== 'LEADER' || request.term !== this.currentTerm || reply.term !== this.currentTerm) {
      return this.commitIndex;
    }

    const next = this.nextIndex.get(peerId);
    const matched = this.matchIndex.get(peerId);
    if (next === undefined || matched === undefined) return this.commitIndex;

    if (!reply.success) {
      // The peer has acknowledged up to `matched`, so it cannot genuinely lack an anchor at or below
      // that index. Only a rejection beyond it, for the request still outstanding, moves nextIndex back.
      const outstanding = next === request.prevLogIndex + 1;
      if (outstanding && request.prevLogIndex > matched) {
        this.nextIndex.set(peerId, Math.max(1, request.prevLogIndex));
      }
      return this.commitIndex;
    }

    const acknowledged = request.prevLogIndex + request.entries.length;
    const newMatch = Math.max(this.matchIndex.get(peerId) ?? 0, acknowledged);
    this.matchIndex.set(peerId, newMatch);
    this.nextIndex.set(peerId, newMatch + 1);
    this.advanceCommitIndex();
    return this.commitIndex;
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

  /** Moves to a newer term as a follower: forgets the vote, the leader and any tally. */
  private stepDown(term: number): void {
    this.currentTerm = term;
    this.state = 'FOLLOWER';
    this.votedFor = null;
    this.leaderId = null;
    this.votesReceived = new Set();
  }

  private becomeLeader(): void {
    this.state = 'LEADER';
    this.leaderId = this.nodeId;
    const lastIndex = this.log[this.log.length - 1]?.index ?? 0;
    this.nextIndex = new Map(this.peers.map((peer) => [peer, lastIndex + 1]));
    this.matchIndex = new Map(this.peers.map((peer) => [peer, 0]));
  }

  /** The entry with the given 1-based index, or undefined if the log does not reach it. */
  private entryAt(index: number): LogEntry | undefined {
    const entry = this.log[index - 1];
    return entry !== undefined && entry.index === index ? entry : undefined;
  }

  /** Removes the entry at `index` and every entry after it. */
  private truncateFrom(index: number): void {
    this.log.splice(Math.max(0, index - 1));
  }

  /**
   * Moves commitIndex to the highest index N that a majority holds, where the entry at N is
   * from the current term (section 5.4.2). The leader counts itself as holding its whole log.
   */
  private advanceCommitIndex(): void {
    const lastIndex = this.log[this.log.length - 1]?.index ?? 0;
    for (let n = lastIndex; n > this.commitIndex; n--) {
      const entry = this.entryAt(n);
      if (entry === undefined || entry.term !== this.currentTerm) continue;

      let holders = 1;
      for (const peer of this.peers) {
        if ((this.matchIndex.get(peer) ?? 0) >= n) holders++;
      }
      if (holders >= this.quorumSize) {
        this.commitIndex = n;
        return;
      }
    }
  }
}
