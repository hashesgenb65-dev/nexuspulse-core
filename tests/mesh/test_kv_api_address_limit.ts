/**
 * Failed logins are also limited per client address, across key ids. A client that tries many key ids
 * is refused once its address is over the limit, and a different address is not affected. Requests with
 * no address, as in-process calls have, are not limited by address.
 */

import { describe, it, expect, afterEach } from 'vitest';
import type { AddressInfo } from 'node:net';
import type * as http from 'node:http';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi, createKvServer, type KvRequest } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';
import { SessionAuthenticator } from '../../src/api/session_auth';
import { AuthManager, type ScopePermission } from '../../src/auth/manager';
import { LoginFailureLimiter } from '../../src/auth/login_limiter';

const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];
const servers: http.Server[] = [];

afterEach(async () => {
  for (const server of servers.splice(0)) await new Promise<void>((resolve) => server.close(() => resolve()));
});

function apiWith(manager: AuthManager, maxAddressFailures: number): KvApi {
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
    loginLimiter: new LoginFailureLimiter({ maxFailures: 1000, windowMs: 60_000 }),
    addressLoginLimiter: new LoginFailureLimiter({ maxFailures: maxAddressFailures, windowMs: 60_000 }),
  });
}

function login(apiKey: string, remoteAddress?: string): KvRequest {
  return {
    method: 'POST',
    url: '/v1/sessions',
    body: JSON.stringify({ apiKey }),
    ...(remoteAddress === undefined ? {} : { remoteAddress }),
  };
}

const wrongKey = (id: string): string => `np_id_${id}.${'0'.repeat(43)}`;

describe('login limiting per client address', () => {
  it('refuses a client that has failed too often, even for a different key id', async () => {
    const manager = new AuthManager();
    const valid = manager.createApiKey('t1', 'ANALYST', scopes).rawApiKey;
    const api = apiWith(manager, 2);

    expect((await api.handle(login(wrongKey('aaaa'), '10.0.0.5'))).status).toBe(401);
    expect((await api.handle(login(wrongKey('bbbb'), '10.0.0.5'))).status).toBe(401);

    const blocked = await api.handle(login(valid, '10.0.0.5'));
    expect(blocked.status).toBe(429);
    expect(Number(blocked.headers['retry-after'])).toBeGreaterThanOrEqual(1);
  });

  it('does not refuse another client address', async () => {
    const manager = new AuthManager();
    const valid = manager.createApiKey('t1', 'ANALYST', scopes).rawApiKey;
    const api = apiWith(manager, 1);

    await api.handle(login(wrongKey('aaaa'), '10.0.0.5'));
    expect((await api.handle(login(valid, '10.0.0.9'))).status).toBe(201);
  });

  it('does not limit requests that have no address', async () => {
    const manager = new AuthManager();
    const valid = manager.createApiKey('t1', 'ANALYST', scopes).rawApiKey;
    const api = apiWith(manager, 1);

    await api.handle(login(wrongKey('aaaa')));
    await api.handle(login(wrongKey('bbbb')));
    expect((await api.handle(login(valid))).status).toBe(201);
  });

  it('applies the limit to real HTTP requests, using the socket address', async () => {
    const manager = new AuthManager();
    const valid = manager.createApiKey('t1', 'ANALYST', scopes).rawApiKey;
    const server = createKvServer(apiWith(manager, 1));
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    const { port } = server.address() as AddressInfo;

    const post = (apiKey: string) =>
      fetch(`http://127.0.0.1:${port}/v1/sessions`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ apiKey }),
      });

    expect((await post(wrongKey('aaaa'))).status).toBe(401);
    expect((await post(valid)).status).toBe(429);
  });
});
