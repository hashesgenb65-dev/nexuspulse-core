/**
 * End-to-end tests for the replicated key-value store: a three-node cluster over real TCP
 * sockets and real timers, where every node applies the committed entries to its own store.
 */

import { describe, it, expect, afterEach } from 'vitest';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { ConsensusDriver } from '../../src/mesh/driver';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';
import { PeerServer, systemScheduler, TcpPeerTransport } from '../../src/mesh/peer_transport';

const LOCALHOST = '127.0.0.1';

interface Member {
  readonly id: string;
  readonly node: RaftConsensusNode;
  readonly driver: ConsensusDriver;
  readonly store: KeyValueStore;
  readonly server: PeerServer;
  readonly transport: TcpPeerTransport;
}

const cleanups: Array<() => void | Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(condition: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await sleep(10);
  }
}

async function startCluster(ids: string[]): Promise<Map<string, Member>> {
  const members = new Map<string, Member>();
  const drivers = new Map<string, ConsensusDriver>();
  const servers = new Map<string, PeerServer>();
  const ports = new Map<string, number>();

  // Start every listener first, so each node knows its peers' ports before it starts.
  for (const id of ids) {
    const server = new PeerServer({
      handler: {
        handleVoteRequest: (request) => driverOf(drivers, id).handleVoteRequest(request),
        handleAppendRequest: (request) => driverOf(drivers, id).handleAppendRequest(request),
      },
    });
    cleanups.push(() => server.close());
    ports.set(id, await server.listen(0, LOCALHOST));
    servers.set(id, server);
  }

  for (const id of ids) {
    const peerIds = ids.filter((other) => other !== id);
    const peers: Record<string, { host: string; port: number }> = {};
    for (const other of peerIds) {
      const port = ports.get(other);
      if (port === undefined) throw new Error(`no port for ${other}`);
      peers[other] = { host: LOCALHOST, port };
    }

    const transport = new TcpPeerTransport({ peers, requestTimeoutMs: 500, connectTimeoutMs: 500 });
    cleanups.push(() => transport.close());
    const node = new RaftConsensusNode({ nodeId: id, clusterPeers: peerIds, electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
    const store = new KeyValueStore();
    const applier = new CommitApplier(node, store);
    const driver = new ConsensusDriver({
      node,
      transport,
      scheduler: systemScheduler(),
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
      onCommitAdvance: () => applier.applyCommitted(),
    });
    drivers.set(id, driver);

    const server = servers.get(id);
    if (server === undefined) throw new Error(`no server for ${id}`);
    members.set(id, { id, node, driver, store, server, transport });
  }

  for (const member of members.values()) member.driver.start();
  cleanups.push(() => {
    for (const member of members.values()) member.driver.stop();
  });
  return members;
}

function driverOf(drivers: Map<string, ConsensusDriver>, id: string): ConsensusDriver {
  const driver = drivers.get(id);
  if (driver === undefined) throw new Error(`no driver ${id}`);
  return driver;
}

function leadersOf(members: Iterable<Member>): Member[] {
  return [...members].filter((member) => member.node.state === 'LEADER');
}

async function leaderOf(members: Map<string, Member>): Promise<Member> {
  await waitFor(() => leadersOf(members.values()).length === 1);
  const [leader] = leadersOf(members.values());
  if (leader === undefined) throw new Error('no leader');
  return leader;
}

describe('replicated key-value store over real sockets', () => {
  it('leaves every node with the same state after a mix of commands', async () => {
    const members = await startCluster(['node_1', 'node_2', 'node_3']);
    const leader = await leaderOf(members);

    leader.node.proposeCommand('SET', { key: 'a', value: 1 });
    leader.node.proposeCommand('SET', { key: 'b', value: { list: [1, 'two'] } });
    leader.node.proposeCommand('DELETE', { key: 'a' });
    leader.node.proposeCommand('NOOP', { key: 'x' });

    // Index 1 is the barrier from the leader's election, so the four commands are indexes 2 to 5.
    await waitFor(() => [...members.values()].every((m) => m.store.lastApplied === 5));

    const expected = JSON.stringify({ b: { list: [1, 'two'] } });
    for (const member of members.values()) {
      expect(JSON.stringify(member.store.snapshot())).toBe(expected);
      expect(member.store.rejected.map((r) => r.index)).toEqual([5]);
    }
  });

  it('keeps the state consistent when the leader changes', async () => {
    const members = await startCluster(['node_1', 'node_2', 'node_3']);
    const first = await leaderOf(members);

    first.node.proposeCommand('SET', { key: 'before', value: 'old leader' });
    await waitFor(() => [...members.values()].every((m) => m.store.lastApplied === 2));

    // Stop the leader and its server, so the survivors must elect a new one.
    first.driver.stop();
    await first.server.close();
    first.transport.close();

    const survivors = new Map([...members].filter(([id]) => id !== first.id));
    const second = await leaderOf(survivors);
    second.node.proposeCommand('SET', { key: 'after', value: 'new leader' });

    // Index 3 is the new leader's barrier, so the write after the failover is index 4.
    await waitFor(() => [...survivors.values()].every((m) => m.store.lastApplied === 4));
    const expected = JSON.stringify({ after: 'new leader', before: 'old leader' });
    for (const member of survivors.values()) {
      expect(JSON.stringify(member.store.snapshot())).toBe(expected);
    }
  });
});
