/**
 * Tests for the key-value HTTP API: reads, writes through the leader, redirects from followers,
 * validation, commit waits, superseded writes, and the real HTTP server wrapper.
 */

import { describe, it, expect, afterEach } from 'vitest';
import type { AddressInfo } from 'node:net';
import { RaftConsensusNode, type LogEntry } from '../../src/mesh/consensus';
import { KvApi, createKvServer, MAX_KV_BODY_BYTES, type KvPeerHttp } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';

const closers: Array<() => Promise<void>> = [];

afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
});

function makeNode(nodeId: string, clusterPeers: string[]): RaftConsensusNode {
  const node = new RaftConsensusNode({ nodeId, clusterPeers, electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
  node.start();
  return node;
}

interface Harness {
  readonly node: RaftConsensusNode;
  readonly store: KeyValueStore;
  readonly api: KvApi;
}

function harness(node: RaftConsensusNode, options: { commitTimeoutMs?: number; peerHttp?: Map<string, KvPeerHttp> } = {}): Harness {
  const store = new KeyValueStore();
  const applier = new CommitApplier(node, store);
  const api = new KvApi({
    node,
    store,
    applier,
    peerHttp: options.peerHttp ?? new Map<string, KvPeerHttp>(),
    commitTimeoutMs: options.commitTimeoutMs ?? 500,
  });
  return { node, store, api };
}

/** A one-node cluster that is leader at once. */
function soloLeader(): Harness {
  const node = makeNode('solo', []);
  node.startElection();
  return harness(node);
}

/** A three-node member that has won the vote from node-b, so it is leader but nothing is acknowledged. */
function threeNodeLeader(options: { commitTimeoutMs?: number } = {}): Harness {
  const node = makeNode('node-a', ['node-b', 'node-c']);
  node.startElection();
  const won = node.recordVote('node-b', { term: node.currentTerm, voteGranted: true });
  if (!won || node.state !== 'LEADER') throw new Error('test setup: expected leadership');
  return harness(node, options);
}

/** A follower that accepted entries from node-b. */
function follower(entries: LogEntry[] = [], leaderCommit = 0, peerHttp?: Map<string, KvPeerHttp>): Harness {
  const node = makeNode('node-a', ['node-b', 'node-c']);
  node.appendEntries(1, 'node-b', entries, { prevLogIndex: 0, prevLogTerm: 0, leaderCommit });
  return harness(node, peerHttp ? { peerHttp } : {});
}

function put(key: string, value: unknown): { method: string; url: string; body: string } {
  return { method: 'PUT', url: `/v1/kv/${key}`, body: JSON.stringify({ value }) };
}

describe('reads', () => {
  it('returns 404 for a missing key', async () => {
    const { api } = soloLeader();
    expect(await api.handle({ method: 'GET', url: '/v1/kv/missing' })).toMatchObject({ status: 404 });
  });

  it('returns the value with the index it was applied at', async () => {
    const { api } = soloLeader();
    await api.handle(put('a', { n: 1 }));
    const response = await api.handle({ method: 'GET', url: '/v1/kv/a' });
    expect(response).toMatchObject({ status: 200, body: { key: 'a', value: { n: 1 }, appliedIndex: 1 } });
  });

  it('decodes percent-encoded keys and ignores the query string', async () => {
    const { api } = soloLeader();
    await api.handle(put('a%20b', 'spaced'));
    const response = await api.handle({ method: 'GET', url: '/v1/kv/a%20b?trace=1' });
    expect(response).toMatchObject({ status: 200, body: { key: 'a b', value: 'spaced' } });
  });

  it('serves a follower from what it has applied', async () => {
    const { api } = follower([{ index: 1, term: 1, command: 'SET', payload: { key: 'k', value: 'v' } }], 1);
    expect(await api.handle({ method: 'GET', url: '/v1/kv/k' })).toMatchObject({
      status: 200,
      body: { value: 'v', appliedIndex: 1 },
    });
  });
});

describe('writes on the leader', () => {
  it('stores a value once it has committed, and deletes it', async () => {
    const { api, store } = soloLeader();

    const written = await api.handle(put('a', 1));
    expect(written).toMatchObject({ status: 200, body: { key: 'a', index: 1 } });
    expect(store.get('a')).toBe(1);

    const removed = await api.handle({ method: 'DELETE', url: '/v1/kv/a' });
    expect(removed).toMatchObject({ status: 200, body: { key: 'a', index: 2 } });
    expect(await api.handle({ method: 'GET', url: '/v1/kv/a' })).toMatchObject({ status: 404 });
  });

  it('accepts a missing key on delete', async () => {
    const { api } = soloLeader();
    expect(await api.handle({ method: 'DELETE', url: '/v1/kv/never' })).toMatchObject({ status: 200 });
  });

  it('stores JSON null as a real value', async () => {
    const { api } = soloLeader();
    await api.handle(put('nothing', null));
    expect(await api.handle({ method: 'GET', url: '/v1/kv/nothing' })).toMatchObject({
      status: 200,
      body: { value: null },
    });
  });

  it('answers 504 when the write does not commit in time, and says the outcome is unknown', async () => {
    const { api } = threeNodeLeader({ commitTimeoutMs: 30 });
    const response = await api.handle(put('a', 1));
    expect(response.status).toBe(504);
    expect(response.body).toMatchObject({ error: expect.stringContaining('outcome is unknown') });
  });

  it('answers 409 when a later leader replaces the write before it commits', async () => {
    const { node, store, api } = threeNodeLeader({ commitTimeoutMs: 1_000 });

    // The write is proposed in this call, before the first await.
    const pending = api.handle(put('k', 'ours'));

    // A term-2 leader overwrites index 1 and commits it.
    node.appendEntries(
      2,
      'node-c',
      [{ index: 1, term: 2, command: 'SET', payload: { key: 'k', value: 'theirs' } }],
      { prevLogIndex: 0, prevLogTerm: 0, leaderCommit: 1 },
    );

    const response = await pending;
    expect(response.status).toBe(409);
    expect(store.get('k')).toBe('theirs');
  });
});

describe('writes on a follower', () => {
  it('redirects a write to the leader, keeping the path', async () => {
    const peerHttp = new Map([['node-b', { host: '10.0.0.2', httpPort: 8002 }]]);
    const { api } = follower([], 0, peerHttp);
    const response = await api.handle(put('a%20b', 1));
    expect(response.status).toBe(307);
    expect(response.headers.location).toBe('http://10.0.0.2:8002/v1/kv/a%20b');
  });

  it('redirects a delete too', async () => {
    const peerHttp = new Map([['node-b', { host: '10.0.0.2', httpPort: 8002 }]]);
    const { api } = follower([], 0, peerHttp);
    const response = await api.handle({ method: 'DELETE', url: '/v1/kv/a' });
    expect(response).toMatchObject({ status: 307, headers: { location: 'http://10.0.0.2:8002/v1/kv/a' } });
  });

  it('brackets an IPv6 leader address', async () => {
    const peerHttp = new Map([['node-b', { host: '::1', httpPort: 8002 }]]);
    const { api } = follower([], 0, peerHttp);
    const response = await api.handle(put('a', 1));
    expect(response.headers.location).toBe('http://[::1]:8002/v1/kv/a');
  });

  it('answers 503 when no leader is known', async () => {
    const { api } = harness(makeNode('node-a', ['node-b']));
    const response = await api.handle(put('a', 1));
    expect(response).toMatchObject({ status: 503, headers: { 'retry-after': '1' } });
  });

  it('answers 503 when the leader has no configured HTTP address', async () => {
    const { api } = follower();
    expect(await api.handle(put('a', 1))).toMatchObject({ status: 503 });
  });

  it('validates the request before it looks for a leader', async () => {
    const { api } = follower();
    expect(await api.handle({ method: 'PUT', url: '/v1/kv/a', body: 'nope' })).toMatchObject({ status: 400 });
  });
});

describe('request validation', () => {
  it('rejects a body that is missing, not JSON, or has no value field', async () => {
    const { api } = soloLeader();
    expect(await api.handle({ method: 'PUT', url: '/v1/kv/a' })).toMatchObject({ status: 400 });
    expect(await api.handle({ method: 'PUT', url: '/v1/kv/a', body: '{oops' })).toMatchObject({
      status: 400,
      body: { error: 'body is not valid JSON' },
    });
    expect(await api.handle({ method: 'PUT', url: '/v1/kv/a', body: '{"val":1}' })).toMatchObject({
      status: 400,
      body: { error: 'body must be an object with a value field' },
    });
    expect(await api.handle({ method: 'PUT', url: '/v1/kv/a', body: '[1]' })).toMatchObject({ status: 400 });
  });

  it('rejects an empty, overlong, or badly encoded key', async () => {
    const { api } = soloLeader();
    expect(await api.handle({ method: 'GET', url: '/v1/kv/' })).toMatchObject({ status: 400 });
    expect(await api.handle({ method: 'GET', url: `/v1/kv/${'k'.repeat(257)}` })).toMatchObject({ status: 400 });
    expect(await api.handle({ method: 'GET', url: '/v1/kv/%E0%A4%A' })).toMatchObject({ status: 400 });
  });

  it('accepts a key of exactly the maximum length', async () => {
    const { api } = soloLeader();
    const key = 'k'.repeat(256);
    expect(await api.handle(put(key, 1))).toMatchObject({ status: 200 });
  });

  it('answers 405 with the allowed methods for anything else', async () => {
    const { api } = soloLeader();
    const response = await api.handle({ method: 'PATCH', url: '/v1/kv/a' });
    expect(response).toMatchObject({ status: 405, headers: { allow: 'GET, PUT, DELETE' } });
  });

  it('answers 404 for paths outside the key-value prefix', async () => {
    const { api } = soloLeader();
    expect(await api.handle({ method: 'GET', url: '/health' })).toMatchObject({ status: 404 });
  });
});

describe('HTTP server wrapper', () => {
  async function listen(): Promise<{ base: string; api: KvApi }> {
    const { api } = soloLeader();
    const server = createKvServer(api);
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    closers.push(() => new Promise<void>((resolve) => {
      server.close(() => resolve());
      server.closeAllConnections();
    }));
    const { port } = server.address() as AddressInfo;
    return { base: `http://127.0.0.1:${port}`, api };
  }

  it('serves a write and a read over real HTTP', async () => {
    const { base } = await listen();
    const written = await fetch(`${base}/v1/kv/greeting`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ value: 'hello' }),
    });
    expect(written.status).toBe(200);
    expect(written.headers.get('content-type')).toBe('application/json');

    const read = await fetch(`${base}/v1/kv/greeting`);
    expect(await read.json()).toMatchObject({ key: 'greeting', value: 'hello' });
  });

  it('answers 413 for a body over the limit', async () => {
    const { base } = await listen();
    const big = JSON.stringify({ value: 'x'.repeat(MAX_KV_BODY_BYTES + 1) });
    const response = await fetch(`${base}/v1/kv/big`, { method: 'PUT', body: big });
    expect(response.status).toBe(413);
  });

  it('answers a DELETE with no body', async () => {
    const { base } = await listen();
    const response = await fetch(`${base}/v1/kv/none`, { method: 'DELETE' });
    expect(response.status).toBe(200);
  });
});
