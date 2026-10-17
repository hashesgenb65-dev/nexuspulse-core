/**
 * Drives a {@link RaftConsensusNode} in time: randomized election timeouts, periodic
 * heartbeats, and the RPC exchanges that carry votes and log entries between peers.
 *
 * Time comes from an injected {@link Scheduler} and randomness from an injected function, so
 * the same code runs against a real clock in production and a manual clock in tests. Network
 * calls go through a {@link PeerTransport}. Incoming RPCs enter through
 * {@link ConsensusDriver.handleVoteRequest} and {@link ConsensusDriver.handleAppendRequest}.
 *
 * Timing rules:
 * - Each election timeout is drawn from [electionTimeoutMs, 2 * electionTimeoutMs).
 * - A follower or candidate restarts its timeout when it grants a vote, accepts AppendEntries
 *   from a current leader, or starts an election.
 * - A leader sends heartbeats every heartbeatIntervalMs, and sends one immediately on election.
 * - A node that finds it is no longer leader stops its heartbeats and resumes the election timer.
 *
 * Transport failures are reported through `onError` and never stop the driver.
 */

import {
  RaftConsensusNode,
  type AppendReply,
  type AppendRequest,
  type ElectionStart,
  type InstallSnapshotReply,
  type InstallSnapshotRequest,
  type SnapshotOffer,
  type VoteRequest,
  type VoteResponse,
} from './consensus.js';

/** Identifies a scheduled task so that it can be cancelled. */
export type TimerId = number;

/** Runs tasks after a delay. Production code uses a real clock; tests use a manual one. */
export interface Scheduler {
  schedule(ms: number, task: () => void): TimerId;
  cancel(id: TimerId): void;
}

/** Sends RPCs to peers. Each call resolves with the peer's reply or rejects if the peer is unreachable. */
export interface PeerTransport {
  requestVote(peerId: string, request: VoteRequest): Promise<VoteResponse>;
  appendEntries(peerId: string, request: AppendRequest): Promise<AppendReply>;
  installSnapshot(peerId: string, request: InstallSnapshotRequest): Promise<InstallSnapshotReply>;
}

export interface DriverOptions {
  node: RaftConsensusNode;
  transport: PeerTransport;
  scheduler: Scheduler;
  /** Base election timeout. Each actual timeout is randomized between this value and twice it. */
  electionTimeoutMs: number;
  heartbeatIntervalMs: number;
  /** Returns a number in [0, 1). Defaults to Math.random. */
  random?: () => number;
  /** Receives failures from the transport or the node. `where` says which call failed. */
  onError?: (error: unknown, where: string) => void;
  /**
   * Called after an RPC or reply moves commitIndex, so the host can apply the newly committed
   * entries. A leader in a one-node cluster commits in proposeCommand, which the host must follow
   * with its own apply step.
   */
  onCommitAdvance?: () => void;
  /**
   * Returns the snapshot to send to a peer that is behind the compacted log, or undefined if none
   * exists. Its index must equal the node's snapshotIndex.
   */
  snapshotSource?: () => SnapshotOffer | undefined;
  /**
   * Adopts a snapshot sent by the leader. Returns true if the host now holds the state through the
   * snapshot index. Throwing counts as not installed.
   */
  installSnapshot?: (snapshot: SnapshotOffer) => boolean;
}

function assertPositiveInteger(name: string, value: number): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive integer, got ${value}`);
  }
}

export class ConsensusDriver {
  private readonly node: RaftConsensusNode;
  private readonly transport: PeerTransport;
  private readonly scheduler: Scheduler;
  private readonly electionTimeoutMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly random: () => number;
  private readonly onError: ((error: unknown, where: string) => void) | undefined;
  private readonly onCommitAdvance: (() => void) | undefined;
  private readonly snapshotSource: (() => SnapshotOffer | undefined) | undefined;
  private readonly installSnapshotFn: ((snapshot: SnapshotOffer) => boolean) | undefined;
  private readonly peers: readonly string[];

  private started = false;
  private electionTimer: TimerId | undefined;
  private heartbeatTimer: TimerId | undefined;
  /** Peers that already have a snapshot on the way, so heartbeats do not send it again. */
  private readonly snapshotsInFlight = new Set<string>();

  /**
   * @throws {RangeError} If either interval is not a positive integer, or if the heartbeat
   *   interval is not shorter than the election timeout.
   */
  constructor(options: DriverOptions) {
    assertPositiveInteger('electionTimeoutMs', options.electionTimeoutMs);
    assertPositiveInteger('heartbeatIntervalMs', options.heartbeatIntervalMs);
    if (options.heartbeatIntervalMs >= options.electionTimeoutMs) {
      throw new RangeError('heartbeatIntervalMs must be shorter than electionTimeoutMs');
    }

    this.node = options.node;
    this.transport = options.transport;
    this.scheduler = options.scheduler;
    this.electionTimeoutMs = options.electionTimeoutMs;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs;
    this.random = options.random ?? Math.random;
    this.onError = options.onError;
    this.onCommitAdvance = options.onCommitAdvance;
    this.snapshotSource = options.snapshotSource;
    this.installSnapshotFn = options.installSnapshot;
    this.peers = options.node.peerIds;
  }

  /** True between {@link start} and {@link stop}. */
  get running(): boolean {
    return this.started;
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.node.start();
    this.resetElectionTimer();
  }

  /** Cancels every timer and stops the node. Pending RPC replies are then ignored. */
  stop(): void {
    this.started = false;
    this.cancelElectionTimer();
    this.cancelHeartbeat();
    this.node.stop();
  }

  /** Handles a RequestVote from a candidate and returns the vote decision. */
  handleVoteRequest(request: VoteRequest): VoteResponse {
    const reply = this.node.requestVote(request.term, request.candidateId, request.lastLogIndex, request.lastLogTerm);
    this.syncRole();
    if (reply.voteGranted) this.resetElectionTimer();
    return reply;
  }

  /** Handles an AppendEntries from the leader and returns the reply. */
  handleAppendRequest(request: AppendRequest): AppendReply {
    const before = this.node.commitIndex;
    const reply = this.node.appendEntries(request.term, request.leaderId, request.entries, {
      prevLogIndex: request.prevLogIndex,
      prevLogTerm: request.prevLogTerm,
      leaderCommit: request.leaderCommit,
    });
    if (this.node.commitIndex !== before) this.onCommitAdvance?.();
    this.syncRole();
    // A reply in the leader's own term means the leader is current, even if the entries were rejected.
    if (reply.term === request.term) this.resetElectionTimer();
    return reply;
  }

  /** Handles an InstallSnapshot from the leader: records the leader, then adopts the snapshot. */
  handleSnapshotRequest(request: InstallSnapshotRequest): InstallSnapshotReply {
    if (!this.node.recognizeLeader(request.term, request.leaderId)) {
      return { term: this.node.currentTerm, installed: false };
    }

    let installed = false;
    try {
      installed = this.installSnapshotFn?.(request.snapshot) ?? false;
    } catch (error) {
      this.report(error, 'installSnapshot');
    }
    // The leader is current whether or not the snapshot could be installed.
    this.syncRole();
    this.resetElectionTimer();
    return { term: this.node.currentTerm, installed };
  }

  private resetElectionTimer(): void {
    this.cancelElectionTimer();
    if (!this.started || this.node.state === 'LEADER') return;

    const jitter = Math.floor(this.random() * this.electionTimeoutMs);
    this.electionTimer = this.scheduler.schedule(this.electionTimeoutMs + jitter, () => {
      this.electionTimer = undefined;
      void this.runElection();
    });
  }

  private cancelElectionTimer(): void {
    if (this.electionTimer !== undefined) {
      this.scheduler.cancel(this.electionTimer);
      this.electionTimer = undefined;
    }
  }

  private async runElection(): Promise<void> {
    if (!this.started || this.node.state === 'LEADER') return;

    let election: ElectionStart;
    try {
      election = this.node.startElection();
    } catch (error) {
      this.report(error, 'startElection');
      return;
    }
    if (election.won) {
      this.becomeLeader();
      return;
    }

    // A split vote is retried after another randomized timeout.
    this.resetElectionTimer();
    const { request } = election;
    await Promise.all(this.peers.map((peer) => this.requestVoteFrom(peer, request)));
  }

  private async requestVoteFrom(peer: string, request: VoteRequest): Promise<void> {
    try {
      const reply = await this.transport.requestVote(peer, request);
      if (!this.started) return;
      if (this.node.recordVote(peer, reply)) this.becomeLeader();
    } catch (error) {
      this.report(error, `requestVote ${peer}`);
    }
  }

  private becomeLeader(): void {
    this.cancelElectionTimer();
    // The first heartbeat asserts leadership straight away; the timer then keeps it going.
    void this.replicateToPeers();
    this.scheduleHeartbeat();
  }

  private scheduleHeartbeat(): void {
    this.cancelHeartbeat();
    if (!this.started || this.node.state !== 'LEADER') return;

    this.heartbeatTimer = this.scheduler.schedule(this.heartbeatIntervalMs, () => {
      this.heartbeatTimer = undefined;
      void this.replicateToPeers();
      this.scheduleHeartbeat();
    });
  }

  private cancelHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) {
      this.scheduler.cancel(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  private async replicateToPeers(): Promise<void> {
    if (!this.started || this.node.state !== 'LEADER') return;
    await Promise.all(this.peers.map((peer) => this.appendTo(peer)));
  }

  private async appendTo(peer: string): Promise<void> {
    if (this.node.state !== 'LEADER') return;
    if (this.node.peerNeedsSnapshot(peer)) {
      await this.sendSnapshotTo(peer);
      return;
    }

    let request: AppendRequest;
    try {
      request = this.node.buildAppendRequest(peer);
    } catch (error) {
      this.report(error, `buildAppendRequest ${peer}`);
      return;
    }

    try {
      const reply = await this.transport.appendEntries(peer, request);
      if (!this.started) return;
      const before = this.node.commitIndex;
      this.node.handleAppendReply(peer, request, reply);
      if (this.node.commitIndex !== before) this.onCommitAdvance?.();
      this.syncRole();
    } catch (error) {
      this.report(error, `appendEntries ${peer}`);
    }
  }

  private async sendSnapshotTo(peer: string): Promise<void> {
    if (this.snapshotsInFlight.has(peer)) return;

    const snapshot = this.snapshotSource?.();
    if (snapshot === undefined || snapshot.index !== this.node.snapshotIndex) {
      this.report(new Error(`peer ${peer} needs a snapshot at ${this.node.snapshotIndex}, but none is available`), 'snapshot');
      return;
    }

    const request: InstallSnapshotRequest = { term: this.node.currentTerm, leaderId: this.node.nodeId, snapshot };
    this.snapshotsInFlight.add(peer);
    try {
      const reply = await this.transport.installSnapshot(peer, request);
      if (!this.started) return;
      this.node.handleSnapshotReply(peer, request.term, snapshot.index, reply);
      this.syncRole();
    } catch (error) {
      this.report(error, `installSnapshot ${peer}`);
    } finally {
      this.snapshotsInFlight.delete(peer);
    }
  }

  /** After any RPC, makes the timers agree with the node's role. */
  private syncRole(): void {
    if (this.node.state === 'LEADER') return;
    this.cancelHeartbeat();
    if (this.started && this.electionTimer === undefined) this.resetElectionTimer();
  }

  private report(error: unknown, where: string): void {
    this.onError?.(error, where);
  }
}
