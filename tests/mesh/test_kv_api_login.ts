/**
 * POST /v1/sessions: an API key is exchanged for a session id, which then works as a bearer token with
 * the key's role. Bad input and unknown keys are refused, and a revoked key's session stops working.
 */

import { describe, it, expect } from 'vitest';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi, type KvPeerHttp, type KvRequest, type KvResponse } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';
import { SessionAuthenticator } from '../../src/api/session_auth';
import { AuthManager, type ScopePermission } from '../../src/auth/manager';

const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];

function apiWith(manager: AuthManager | undefined): KvApi {
  const node = new RaftConsensusNode({
    nodeId: 'solo',
    clusterPeers: [],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
  node.start();
  node.startElection();
  const store = new KeyValueStore();
  return new KvApi({
    node,
    store,
    applier: new CommitApplier(node, store),
    peerHttp: new Map<string, KvPeerHttp>(),
    commitTimeoutMs: 500,
    ...(manager === undefined
      ? {}
      : { sessions: manager, auth: new SessionAuthenticator(undefined, manager) }),
  });
}

function login(apiKey: string): KvRequest {
  return { method: 'POST', url: '/v1/sessions', body: JSON.stringify({ apiKey }) };
}

function bearer(sessionId: string, method: string, url: string, body?: string): KvRequest {
  return { method, url, authorization: `Bearer ${sessionId}`, ...(body === undefined ? {} : { body }) };
}

function sessionFor(api: KvApi, rawApiKey: string): Promise<KvResponse> {
  return api.handle(login(rawApiKey));
}

describe('logging in', () => {
  it('returns a session id and expiry for a valid key, and does not cache the reply', async () => {
    const manager = new AuthManager();
    const { rawApiKey } = manager.createApiKey('t1', 'ANALYST', scopes);
    const response = await sessionFor(apiWith(manager), rawApiKey);

    expect(response.status).toBe(201);
    expect(response.headers['cache-control']).toBe('no-store');
    const body = response.body as { sessionId: string; expiresAt: number };
    expect(body.sessionId).toMatch(/^sess_[0-9a-f]{32}$/);
    expect(body.expiresAt).toBeGreaterThan(Date.now());
    expect(JSON.stringify(response.body)).not.toContain(rawApiKey);
  });

  it('refuses an unknown or wrong key with 401', async () => {
    const manager = new AuthManager();
    manager.createApiKey('t1', 'ANALYST', scopes);
    const api = apiWith(manager);
    expect((await sessionFor(api, 'np_id_0000000000000000.np_sec_wrong')).status).toBe(401);
    expect((await sessionFor(api, 'nonsense')).status).toBe(401);
  });

  it('refuses a body that is not JSON, has no apiKey, or has an empty key with 400', async () => {
    const manager = new AuthManager();
    const api = apiWith(manager);
    const bad: KvRequest[] = [
      { method: 'POST', url: '/v1/sessions', body: '{not json' },
      { method: 'POST', url: '/v1/sessions', body: JSON.stringify({ key: 'x' }) },
      { method: 'POST', url: '/v1/sessions', body: JSON.stringify({ apiKey: '' }) },
      { method: 'POST', url: '/v1/sessions', body: JSON.stringify(['x']) },
      { method: 'POST', url: '/v1/sessions' },
    ];
    for (const request of bad) expect((await api.handle(request)).status).toBe(400);
  });

  it('answers 405 to anything but POST', async () => {
    const api = apiWith(new AuthManager());
    const response = await api.handle({ method: 'GET', url: '/v1/sessions' });
    expect(response.status).toBe(405);
    expect(response.headers.allow).toBe('POST');
  });

  it('answers 404 when no session store is configured', async () => {
    const api = apiWith(undefined);
    expect((await api.handle(login('np_id_0000000000000000.np_sec_x'))).status).toBe(404);
  });
});

describe('using a session', () => {
  it('lets a writer session write and a reader session read, and refuses a reader on a write', async () => {
    const manager = new AuthManager();
    const writerKey = manager.createApiKey('t1', 'INGEST_SERVICE', scopes).rawApiKey;
    const readerKey = manager.createApiKey('t1', 'READ_ONLY', scopes).rawApiKey;
    const api = apiWith(manager);
    const writer = ((await sessionFor(api, writerKey)).body as { sessionId: string }).sessionId;
    const reader = ((await sessionFor(api, readerKey)).body as { sessionId: string }).sessionId;

    const written = await api.handle(bearer(writer, 'PUT', '/v1/kv/sensor', JSON.stringify({ value: 21 })));
    expect(written.status).toBe(200);
    expect((await api.handle(bearer(reader, 'GET', '/v1/kv/sensor'))).status).toBe(200);
    expect((await api.handle(bearer(reader, 'PUT', '/v1/kv/sensor', JSON.stringify({ value: 22 })))).status).toBe(403);
  });

  it('stops working when its key is revoked', async () => {
    const manager = new AuthManager();
    const { rawApiKey, keyId } = manager.createApiKey('t1', 'INGEST_SERVICE', scopes);
    const api = apiWith(manager);
    const sessionId = ((await sessionFor(api, rawApiKey)).body as { sessionId: string }).sessionId;
    expect((await api.handle(bearer(sessionId, 'GET', '/v1/kv/sensor'))).status).not.toBe(401);

    manager.revokeApiKey(keyId);
    expect((await api.handle(bearer(sessionId, 'GET', '/v1/kv/sensor'))).status).toBe(401);
    expect((await sessionFor(api, rawApiKey)).status).toBe(401);
  });
});
