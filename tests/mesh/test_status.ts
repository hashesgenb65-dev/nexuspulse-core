/**
 * Tests for GET /v1/status: the fields for a leader and a follower, the HTTP route and its method
 * check, and a live member answering over real HTTP.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import type { AddressInfo } from 'node:net';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi } from '../../src/mesh/kv_api';
import { nodeStatus, STATUS_PATH } from '../../src/mesh/status';
import { parseNodeConfig } from '../../src/mesh/node_config';
import { startMeshNode, type MeshNodeHandle } from '../../src/mesh/node_runner';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';

const HOST = '127.0.0.1';
const cleanups: Array<() => Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function makeNode(nodeId: string, clusterPeers: string[]): RaftConsensusNode {
  const node = new RaftConsensusNode({ nodeId, clusterPeers, electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
  node.start();
  return node;
}

function apiFor(node: RaftConsensusNode, store: KeyValueStore, durable = false): KvApi {
  return new KvApi({
    node,
    store,
    applier: new CommitApplier(node, store),
    peerHttp: new Map(),
    commitTimeoutMs: 200,
    durable,
  });
}

describe('nodeStatus', () => {
  it('reports a leader with its quorum and peers', () => {
    const node = makeNode('node-a', ['node-b', 'node-c']);
    node.startElection();
    node.recordVote('node-b', { term: node.currentTerm, voteGranted: true });

    const status = nodeStatus(node, new KeyValueStore(), false);
    expect(status).toMatchObject({
      nodeId: 'node-a',
      role: 'LEADER',
      term: 1,
      leaderId: 'node-a',
      quorumSize: 2,
      peers: ['node-b', 'node-c'],
      durable: false,
    });
  });

  it('reports a follower that follows a known leader', () => {
    const node = makeNode('node-a', ['node-b', 'node-c']);
    node.appendEntries(
      2,
      'node-b',
      [{ index: 1, term: 2, command: 'SET', payload: { key: 'k', value: 1 } }],
      { prevLogIndex: 0, prevLogTerm: 0, leaderCommit: 1 },
    );
    const store = new KeyValueStore();
    new CommitApplier(node, store).applyCommitted();

    expect(nodeStatus(node, store, true)).toMatchObject({
      role: 'FOLLOWER',
      term: 2,
      leaderId: 'node-b',
      lastLogIndex: 1,
      commitIndex: 1,
      appliedIndex: 1,
      logLength: 1,
      durable: true,
    });
  });
});

describe('GET /v1/status on the key-value API', () => {
  it('answers 200 with the status', async () => {
    const node = makeNode('solo', []);
    node.startElection();
    const response = await apiFor(node, new KeyValueStore(), true).handle({ method: 'GET', url: STATUS_PATH });

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({ nodeId: 'solo', role: 'LEADER', durable: true });
  });

  it('ignores a query string', async () => {
    const node = makeNode('solo', []);
    const response = await apiFor(node, new KeyValueStore()).handle({ method: 'GET', url: `${STATUS_PATH}?x=1` });
    expect(response.status).toBe(200);
  });

  it('answers 405 to any method but GET', async () => {
    const node = makeNode('solo', []);
    const response = await apiFor(node, new KeyValueStore()).handle({ method: 'POST', url: STATUS_PATH });
    expect(response).toMatchObject({ status: 405, headers: { allow: 'GET' } });
  });
});

describe('live member', () => {
  it('serves its status over HTTP', async () => {
    const port = await new Promise<number>((resolve, reject) => {
      const probe = net.createServer();
      probe.once('error', reject);
      probe.listen(0, HOST, () => {
        const { port: free } = probe.address() as AddressInfo;
        probe.close(() => resolve(free));
      });
    });
    const config = parseNodeConfig({
      nodeId: 'solo-1',
      rpc: { host: HOST, port: 0 },
      http: { host: HOST, port: port },
      peers: [],
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
    });
    const handle: MeshNodeHandle = await startMeshNode(config);
    cleanups.push(() => handle.stop());

    const deadline = Date.now() + 5_000;
    let body: { role?: string; durable?: boolean } = {};
    while (body.role !== 'LEADER') {
      if (Date.now() > deadline) throw new Error('never became leader');
      await new Promise((resolve) => setTimeout(resolve, 20));
      body = (await (await fetch(`http://${HOST}:${handle.httpPort}${STATUS_PATH}`)).json()) as typeof body;
    }
    expect(body).toMatchObject({ nodeId: 'solo-1', role: 'LEADER', durable: false });
  });
});
