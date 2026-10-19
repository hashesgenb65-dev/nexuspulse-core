/**
 * Bearer-token checks on the key-value API: no token gets 401, a token with too few rights gets
 * 403, and a token with the right role passes the check. Without a token store the API is open.
 */

import { describe, it, expect } from 'vitest';
import { TokenAuth, type TokenSpec } from '../../src/api/auth';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';

const READER = 'reader-secret-0123456789abcd';
const WRITER = 'writer-secret-0123456789abcd';

const specs: TokenSpec[] = [
  { name: 'dash', role: 'reader', secret: READER },
  { name: 'edge', role: 'writer', secret: WRITER },
];

function apiWith(auth?: TokenAuth): KvApi {
  const node = new RaftConsensusNode({
    nodeId: 'node-1',
    clusterPeers: ['node-1'],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
  const store = new KeyValueStore();
  return new KvApi({
    node,
    store,
    applier: new CommitApplier(node, store),
    peerHttp: new Map(),
    ...(auth === undefined ? {} : { auth }),
  });
}

const bearer = (secret: string): string => `Bearer ${secret}`;

describe('key-value API with a token store', () => {
  const auth = new TokenAuth(specs);

  it('answers 401 with a challenge when no token is sent', async () => {
    const response = await apiWith(auth).handle({ method: 'GET', url: '/v1/kv/a' });
    expect(response.status).toBe(401);
    expect(response.headers['www-authenticate']).toBe('Bearer realm="nexuspulse"');
  });

  it('answers 401 for an unknown token', async () => {
    const response = await apiWith(auth).handle({
      method: 'GET',
      url: '/v1/kv/a',
      authorization: bearer('not-a-real-token-0123456789abcdef'),
    });
    expect(response.status).toBe(401);
  });

  it('lets a reader read but not write', async () => {
    const api = apiWith(auth);
    const read = await api.handle({ method: 'GET', url: '/v1/kv/a', authorization: bearer(READER) });
    expect(read.status).not.toBe(401);
    expect(read.status).not.toBe(403);

    const write = await api.handle({
      method: 'PUT',
      url: '/v1/kv/a',
      body: '{"value":1}',
      authorization: bearer(READER),
    });
    expect(write.status).toBe(403);
    expect(write.body).toEqual({ error: 'this request needs the writer role' });
  });

  it('lets a writer through the check for writes', async () => {
    const write = await apiWith(auth).handle({
      method: 'PUT',
      url: '/v1/kv/a',
      body: '{"value":1}',
      authorization: bearer(WRITER),
    });
    expect(write.status).not.toBe(401);
    expect(write.status).not.toBe(403);
  });

  it('checks the status endpoint too', async () => {
    const api = apiWith(auth);
    expect((await api.handle({ method: 'GET', url: '/v1/status' })).status).toBe(401);
    expect((await api.handle({ method: 'GET', url: '/v1/status', authorization: bearer(READER) })).status).toBe(200);
  });

  it('refuses a write with a token that is only a reader, even with the right secret format', async () => {
    const response = await apiWith(auth).handle({
      method: 'DELETE',
      url: '/v1/kv/a',
      authorization: bearer(READER),
    });
    expect(response.status).toBe(403);
  });
});

describe('key-value API without a token store', () => {
  it('stays open, as before', async () => {
    const response = await apiWith().handle({ method: 'GET', url: '/v1/status' });
    expect(response.status).toBe(200);
  });
});
