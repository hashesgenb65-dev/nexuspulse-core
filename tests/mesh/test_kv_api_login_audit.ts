/**
 * Login attempts are written to the audit ledger: success, a wrong secret, and a blocked attempt. The
 * ledger holds the key id only, never the API key or the session id.
 */

import { describe, it, expect } from 'vitest';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi, type KvRequest } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';
import { SessionAuthenticator } from '../../src/api/session_auth';
import { AuthManager, type ScopePermission } from '../../src/auth/manager';
import { LoginFailureLimiter } from '../../src/auth/login_limiter';
import { AuditLedgerEngine } from '../../src/audit/ledger';

const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];

function apiWith(manager: AuthManager, audit: AuditLedgerEngine): KvApi {
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
    loginLimiter: new LoginFailureLimiter({ maxFailures: 1, windowMs: 60_000 }),
    audit,
  });
}

function login(apiKey: string): KvRequest {
  return { method: 'POST', url: '/v1/sessions', body: JSON.stringify({ apiKey }) };
}

describe('login audit', () => {
  it('records success, a wrong secret, and a blocked attempt, by key id', async () => {
    const manager = new AuthManager();
    const { rawApiKey } = manager.createApiKey('t1', 'ANALYST', scopes);
    const keyId = rawApiKey.split('.')[0]!;
    const audit = new AuditLedgerEngine();
    const api = apiWith(manager, audit);

    await api.handle(login(`${keyId}.${'0'.repeat(43)}`));
    await api.handle(login(`${keyId}.${'0'.repeat(43)}`));
    await api.handle(login(rawApiKey));

    const actions = audit.query({ actorId: keyId }).map((record) => record.action);
    expect(actions).toEqual(['LOGIN_FAILED', 'LOGIN_BLOCKED', 'LOGIN_BLOCKED']);
    expect(audit.verifyIntegrity().isValid).toBe(true);
  });

  it('records a success, and never stores the API key or session id', async () => {
    const manager = new AuthManager();
    const { rawApiKey } = manager.createApiKey('t1', 'ANALYST', scopes);
    const keyId = rawApiKey.split('.')[0]!;
    const audit = new AuditLedgerEngine();
    const api = apiWith(manager, audit);

    const response = await api.handle(login(rawApiKey));
    const sessionId = (response.body as { sessionId: string }).sessionId;

    const records = audit.query({ actorId: keyId });
    expect(records.map((record) => record.action)).toEqual(['LOGIN_OK']);
    const stored = JSON.stringify(audit.query({}));
    expect(stored).not.toContain(rawApiKey);
    expect(stored).not.toContain(sessionId);
  });
});
