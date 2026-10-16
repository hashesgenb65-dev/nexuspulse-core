/**
 * Tests for the consensus driver: election and heartbeat timing, failover between nodes,
 * replication through the timers, and lifecycle. Every test runs on a manual clock and an
 * in-memory network, so the outcomes do not depend on real time.
 */

import { describe, it, expect } from 'vitest';
import { RaftConsensusNode, type NodeState } from '../../src/mesh/consensus';
import {
  ConsensusDriver,
  type PeerTransport,
  type Scheduler,
  type TimerId,
} from '../../src/mesh/driver';

const ELECTION_MS = 150;
const HEARTBEAT_MS = 50;

/** Lets every resolved promise run before the test continues. */
const flushPromises = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

/** A clock that only moves when told to. Timers run in time order as the clock passes them. */
class ManualScheduler implements Scheduler {
  private clock = 0;
  private nextId = 1;
  private readonly timers = new Map<TimerId, { at: number; task: () => void }>();

  get now(): number {
    return this.clock;
  }

  get pending(): number {
    return this.timers.size;
  }

  schedule(ms: number, task: () => void): TimerId {
    const id = this.nextId++;
    this.timers.set(id, { at: this.clock + ms, task });
    return id;
  }

  cancel(id: TimerId): void {
    this.timers.delete(id);
  }

  /** Moves time forward, running each timer that falls due, earliest first. */
  async advance(ms: number): Promise<void> {
    const target = this.clock + ms;
    for (;;) {
      let nextId: TimerId | undefined;
      let nextAt = Infinity;
      for (const [id, timer] of this.timers) {
        if (timer.at <= target && timer.at < nextAt) {
          nextId = id;
          nextAt = timer.at;
        }
      }
      if (nextId === undefined) break;

      const timer = this.timers.get(nextId);
      this.timers.delete(nextId);
      this.clock = nextAt;
      timer?.task();
      await flushPromises();
    }
    this.clock = target;
    await flushPromises();
  }
}

/** An in-memory network. A cut-off node can neither send nor receive. */
class FakeNetwork {
  readonly drivers = new Map<string, ConsensusDriver>();
  private readonly cutOff = new Set<string>();

  isolate(id: string): void {
    this.cutOff.add(id);
  }

  heal(id: string): void {
    this.cutOff.delete(id);
  }

  transportFor(from: string): PeerTransport {
    return {
      requestVote: async (to, request) => {
        this.checkReachable(from, to);
        return this.target(to).handleVoteRequest(request);
      },
      appendEntries: async (to, request) => {
        this.checkReachable(from, to);
        return this.target(to).handleAppendRequest(request);
      },
    };
  }

  private checkReachable(from: string, to: string): void {
    if (this.cutOff.has(from) || this.cutOff.has(to)) {
      throw new Error(`${from} cannot reach ${to}`);
    }
  }

  private target(id: string): ConsensusDriver {
    const driver = this.drivers.get(id);
    if (driver === undefined) throw new Error(`no node ${id}`);
    return driver;
  }
}

interface ReportedError {
  readonly where: string;
  readonly error: unknown;
}

interface Cluster {
  readonly scheduler: ManualScheduler;
  readonly network: FakeNetwork;
  readonly nodes: Map<string, RaftConsensusNode>;
  readonly drivers: Map<string, ConsensusDriver>;
  readonly errors: ReportedError[];
  stateOf(id: string): NodeState;
  start(): void;
  stopAll(): void;
}

/**
 * Builds a cluster of drivers that share one clock and one network. Each node's election
 * jitter is fixed by `randoms`, so node_1 (default 0) always times out first.
 */
function makeCluster(ids: string[], randoms: Record<string, number> = {}): Cluster {
  const scheduler = new ManualScheduler();
  const network = new FakeNetwork();
  const nodes = new Map<string, RaftConsensusNode>();
  const drivers = new Map<string, ConsensusDriver>();
  const errors: ReportedError[] = [];

  for (const id of ids) {
    const node = new RaftConsensusNode({
      nodeId: id,
      clusterPeers: ids.filter((other) => other !== id),
      electionTimeoutMs: ELECTION_MS,
      heartbeatIntervalMs: HEARTBEAT_MS,
    });
    const driver = new ConsensusDriver({
      node,
      transport: network.transportFor(id),
      scheduler,
      electionTimeoutMs: ELECTION_MS,
      heartbeatIntervalMs: HEARTBEAT_MS,
      random: () => randoms[id] ?? 0,
      onError: (error, where) => errors.push({ where, error }),
    });
    nodes.set(id, node);
    drivers.set(id, driver);
    network.drivers.set(id, driver);
  }

  return {
    scheduler,
    network,
    nodes,
    drivers,
    errors,
    stateOf: (id) => {
      const node = nodes.get(id);
      if (node === undefined) throw new Error(`no node ${id}`);
      return node.state;
    },
    start: () => {
      for (const driver of drivers.values()) driver.start();
    },
    stopAll: () => {
      for (const driver of drivers.values()) driver.stop();
    },
  };
}

function nodeOf(cluster: Cluster, id: string): RaftConsensusNode {
  const node = cluster.nodes.get(id);
  if (node === undefined) throw new Error(`no node ${id}`);
  return node;
}

const RANDOMS = { node_1: 0, node_2: 0.5, node_3: 0.9 };

describe('timing', () => {
  it('does not start an election before the base timeout', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();

    await cluster.scheduler.advance(ELECTION_MS - 1);
    expect(cluster.stateOf('node_1')).toBe('FOLLOWER');

    await cluster.scheduler.advance(1);
    expect(cluster.stateOf('node_1')).toBe('LEADER');
  });

  it('draws the timeout from [base, 2 * base)', async () => {
    const scheduler = new ManualScheduler();
    const node = new RaftConsensusNode({
      nodeId: 'solo',
      clusterPeers: ['other', 'third'],
      electionTimeoutMs: ELECTION_MS,
      heartbeatIntervalMs: HEARTBEAT_MS,
    });
    const driver = new ConsensusDriver({
      node,
      transport: { requestVote: () => new Promise(() => {}), appendEntries: () => new Promise(() => {}) },
      scheduler,
      electionTimeoutMs: ELECTION_MS,
      heartbeatIntervalMs: HEARTBEAT_MS,
      random: () => 0.999,
    });
    driver.start();

    // 0.999 * 150 rounds down to 149, so the timeout is 299 ms.
    await scheduler.advance(2 * ELECTION_MS - 2);
    expect(node.state).toBe('FOLLOWER');
    await scheduler.advance(1);
    expect(node.state).toBe('CANDIDATE');
  });
});

describe('elections', () => {
  it('elects a leader once a majority votes for it, and the followers learn it', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);

    expect(cluster.stateOf('node_1')).toBe('LEADER');
    expect(nodeOf(cluster, 'node_1').currentTerm).toBe(1);
    expect(nodeOf(cluster, 'node_2').leaderId).toBe('node_1');
    expect(nodeOf(cluster, 'node_3').leaderId).toBe('node_1');
  });

  it('keeps the leader in charge while heartbeats arrive', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);

    await cluster.scheduler.advance(2_000);
    expect(cluster.stateOf('node_1')).toBe('LEADER');
    expect(nodeOf(cluster, 'node_1').currentTerm).toBe(1);
    expect(cluster.stateOf('node_2')).toBe('FOLLOWER');
    expect(cluster.stateOf('node_3')).toBe('FOLLOWER');
  });

  it('replaces a leader that cannot reach its peers', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);
    expect(cluster.stateOf('node_1')).toBe('LEADER');

    cluster.network.isolate('node_1');
    await cluster.scheduler.advance(1_000);

    expect(cluster.stateOf('node_2')).toBe('LEADER');
    expect(nodeOf(cluster, 'node_2').currentTerm).toBe(2);
    // The isolated node has not heard about the new term, so it still believes it leads.
    expect(cluster.stateOf('node_1')).toBe('LEADER');
    expect(nodeOf(cluster, 'node_1').currentTerm).toBe(1);
  });

  it('makes a deposed leader step down once it hears the new term', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);
    cluster.network.isolate('node_1');
    await cluster.scheduler.advance(1_000);
    expect(cluster.stateOf('node_2')).toBe('LEADER');

    cluster.network.heal('node_1');
    await cluster.scheduler.advance(200);

    expect(cluster.stateOf('node_1')).toBe('FOLLOWER');
    expect(nodeOf(cluster, 'node_1').currentTerm).toBe(2);
    expect(nodeOf(cluster, 'node_1').leaderId).toBe('node_2');
    expect(cluster.stateOf('node_2')).toBe('LEADER');
  });

  it('lets a single-node cluster elect itself', async () => {
    const cluster = makeCluster(['solo']);
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);

    expect(cluster.stateOf('solo')).toBe('LEADER');
    expect(cluster.errors).toEqual([]);
  });
});

describe('replication through the timers', () => {
  it('replicates a proposed command on the next heartbeat and commits it', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);

    nodeOf(cluster, 'node_1').proposeCommand('SET', { x: 1 });
    await cluster.scheduler.advance(HEARTBEAT_MS);
    expect(nodeOf(cluster, 'node_2').log).toHaveLength(1);
    expect(nodeOf(cluster, 'node_1').commitIndex).toBe(1);

    // Followers learn the commit index from the heartbeat after that.
    await cluster.scheduler.advance(HEARTBEAT_MS);
    expect(nodeOf(cluster, 'node_2').commitIndex).toBe(1);
    expect(nodeOf(cluster, 'node_3').commitIndex).toBe(1);
  });

  it('catches up a follower that was cut off while entries were committed', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);

    nodeOf(cluster, 'node_1').proposeCommand('SET', { n: 1 });
    await cluster.scheduler.advance(100);

    // Short enough that node_3 does not time out while it is cut off.
    cluster.network.isolate('node_3');
    nodeOf(cluster, 'node_1').proposeCommand('SET', { n: 2 });
    nodeOf(cluster, 'node_1').proposeCommand('SET', { n: 3 });
    await cluster.scheduler.advance(100);
    expect(nodeOf(cluster, 'node_3').log).toHaveLength(1);

    cluster.network.heal('node_3');
    await cluster.scheduler.advance(200);
    expect(nodeOf(cluster, 'node_3').log.map((e) => e.index)).toEqual([1, 2, 3]);
    expect(nodeOf(cluster, 'node_1').commitIndex).toBe(3);
    expect(cluster.stateOf('node_1')).toBe('LEADER');
  });
});

describe('lifecycle and errors', () => {
  it('does nothing until started', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    await cluster.scheduler.advance(1_000);

    expect(cluster.stateOf('node_1')).toBe('FOLLOWER');
    expect(cluster.scheduler.pending).toBe(0);
  });

  it('cancels every timer on stop', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);
    expect(cluster.scheduler.pending).toBeGreaterThan(0);

    cluster.stopAll();
    expect(cluster.scheduler.pending).toBe(0);
    await cluster.scheduler.advance(1_000);
    expect(cluster.stateOf('node_1')).toBe('LEADER');
    expect(nodeOf(cluster, 'node_1').currentTerm).toBe(1);
  });

  it('ignores a second start call', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.start();
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);

    expect(nodeOf(cluster, 'node_1').currentTerm).toBe(1);
  });

  it('reports transport failures and keeps trying', async () => {
    const cluster = makeCluster(['node_1', 'node_2', 'node_3'], RANDOMS);
    cluster.network.isolate('node_2');
    cluster.network.isolate('node_3');
    cluster.start();
    await cluster.scheduler.advance(ELECTION_MS);

    expect(cluster.stateOf('node_1')).toBe('CANDIDATE');
    expect(cluster.errors.map((e) => e.where)).toEqual(['requestVote node_2', 'requestVote node_3']);
    expect(cluster.errors[0]?.error).toBeInstanceOf(Error);
  });

  it('rejects a heartbeat interval that is not shorter than the election timeout', () => {
    const node = new RaftConsensusNode({
      nodeId: 'n',
      clusterPeers: [],
      electionTimeoutMs: ELECTION_MS,
      heartbeatIntervalMs: ELECTION_MS,
    });
    expect(
      () =>
        new ConsensusDriver({
          node,
          transport: { requestVote: async () => ({ term: 0, voteGranted: false }), appendEntries: async () => ({ term: 0, success: false }) },
          scheduler: new ManualScheduler(),
          electionTimeoutMs: ELECTION_MS,
          heartbeatIntervalMs: ELECTION_MS,
        }),
    ).toThrowError(RangeError);
  });

  it('rejects intervals that are not positive integers', () => {
    const node = new RaftConsensusNode({
      nodeId: 'n',
      clusterPeers: [],
      electionTimeoutMs: 1,
      heartbeatIntervalMs: 1,
    });
    const base = {
      node,
      transport: { requestVote: async () => ({ term: 0, voteGranted: false }), appendEntries: async () => ({ term: 0, success: false }) },
      scheduler: new ManualScheduler(),
    };
    expect(() => new ConsensusDriver({ ...base, electionTimeoutMs: 0, heartbeatIntervalMs: 0 })).toThrowError(
      /electionTimeoutMs must be a positive integer/,
    );
    expect(() => new ConsensusDriver({ ...base, electionTimeoutMs: 150.5, heartbeatIntervalMs: 50 })).toThrowError(
      RangeError,
    );
    expect(() => new ConsensusDriver({ ...base, electionTimeoutMs: 150, heartbeatIntervalMs: -1 })).toThrowError(
      /heartbeatIntervalMs must be a positive integer/,
    );
  });
});
