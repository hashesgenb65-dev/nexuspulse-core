/**
 * POST /v1/sessions with failed-login limiting: a key id with too many wrong secrets gets a 429 with
 * retry-after, even for its correct secret, and a malformed body does not count as a failure.
 */

import { describe, it, expect } from 'vitest';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi, type KvRequest } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';
import { SessionAuthenticator } from '../../src/api/session_auth';
import { AuthManager, type ScopePermission } from '../../src/auth/manager';
import { LoginFailureLimiter } from '../../src/auth/login_limiter';

const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];

function apiWith(manager: AuthManager, limiter: LoginFailureLimiter): KvApi {
  const node = new RaftConsensusNode({ nodeId: 'solo', clusterPeers: [], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
  node.start();
  node.startElection();
  const store = new KeyValueStore();
  return new KvApi({
    node,
    store,
    applier: new CommitApplier(node, store),
    peerHttp: new Map(),
    commitTimeoutMs: 500,
    sessions: manager,
    auth: new SessionAuthenticator(undefined, manager),
    loginLimiter: limiter,
  });
}

function login(apiKey: string): KvRequest {
  return { method: 'POST', url: '/v1/sessions', body: JSON.stringify({ apiKey }) };
}

describe('login limiting', () => {
  it('refuses a key id with 429 after too many wrong secrets, even with the right one', async () => {
    const manager = new AuthManager();
    const { rawApiKey } = manager.createApiKey('t1', 'ANALYST', scopes);
    const keyId = rawApiKey.split('.')[0]!;
    const api = apiWith(manager, new LoginFailureLimiter({ maxFailures: 2, windowMs: 60_000 }));

    const wrong = `${keyId}.${'0'.repeat(43)}`;
    expect((await api.handle(login(wrong))).status).toBe(401);
    expect((await api.handle(login(wrong))).status).toBe(401);

    const blocked = await api.handle(login(rawApiKey));
    expect(blocked.status).toBe(429);
    expect(Number(blocked.headers['retry-after'])).toBeGreaterThanOrEqual(1);
  });

  it('does not block other key ids', async () => {
    const manager = new AuthManager();
    const first = manager.createApiKey('t1', 'ANALYST', scopes).rawApiKey;
    const second = manager.createApiKey('t2', 'ANALYST', scopes).rawApiKey;
    const api = apiWith(manager, new LoginFailureLimiter({ maxFailures: 1, windowMs: 60_000 }));

    await api.handle(login(`${first.split('.')[0]!}.${'0'.repeat(43)}`));
    expect((await api.handle(login(second))).status).toBe(201);
  });

  it('clears the failures after a correct login', async () => {
    const manager = new AuthManager();
    const { rawApiKey } = manager.createApiKey('t1', 'ANALYST', scopes);
    const keyId = rawApiKey.split('.')[0]!;
    const api = apiWith(manager, new LoginFailureLimiter({ maxFailures: 2, windowMs: 60_000 }));

    await api.handle(login(`${keyId}.${'0'.repeat(43)}`));
    expect((await api.handle(login(rawApiKey))).status).toBe(201);
    await api.handle(login(`${keyId}.${'0'.repeat(43)}`));
    expect((await api.handle(login(rawApiKey))).status).toBe(201);
  });

  it('does not count a malformed body as a failure', async () => {
    const manager = new AuthManager();
    const { rawApiKey } = manager.createApiKey('t1', 'ANALYST', scopes);
    const api = apiWith(manager, new LoginFailureLimiter({ maxFailures: 1, windowMs: 60_000 }));

    for (let i = 0; i < 3; i++) {
      expect((await api.handle({ method: 'POST', url: '/v1/sessions', body: 'nope' })).status).toBe(400);
    }
    expect((await api.handle(login(rawApiKey))).status).toBe(201);
  });
});
