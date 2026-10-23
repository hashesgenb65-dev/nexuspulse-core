/**
 * The audit endpoints on the key-value API: listing with filters and limits, and verifying the chain.
 * They are read-only, need a reader token when auth is on, and are absent without a ledger.
 */

import { describe, it, expect } from 'vitest';
import { TokenAuth, type TokenSpec } from '../../src/api/auth';
import { AuditLedgerEngine } from '../../src/audit/ledger';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi, MAX_AUDIT_LIMIT, type KvPeerHttp } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';

const WRITER = 'writer-secret-0123456789abcdefgh';
const READER = 'reader-secret-0123456789abcdefgh';
const specs: TokenSpec[] = [
  { name: 'edge-01', role: 'writer', secret: WRITER },
  { name: 'dash', role: 'reader', secret: READER },
];

function api(options: { audit?: AuditLedgerEngine; auth?: TokenAuth }): KvApi {
  const node = new RaftConsensusNode({ nodeId: 'solo', clusterPeers: [], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
  node.start();
  node.startElection();
  const store = new KeyValueStore();
  return new KvApi({
    node,
    store,
    applier: new CommitApplier(node, store),
    peerHttp: new Map<string, KvPeerHttp>(),
    commitTimeoutMs: 500,
    ...(options.audit === undefined ? {} : { audit: options.audit }),
    ...(options.auth === undefined ? {} : { auth: options.auth }),
  });
}

function seeded(): AuditLedgerEngine {
  const audit = new AuditLedgerEngine();
  audit.append('edge-01', 'SET', 'kv:a', { index: 1, term: 1 });
  audit.append('edge-01', 'DELETE', 'kv:a', { index: 2, term: 1 });
  audit.append('dash', 'SET', 'kv:b', { index: 3, term: 1 });
  return audit;
}

const get = (url: string, authorization?: string): { method: string; url: string; authorization?: string } => ({
  method: 'GET',
  url,
  ...(authorization === undefined ? {} : { authorization }),
});

describe('GET /v1/audit', () => {
  it('lists every record with a total', async () => {
    const response = await api({ audit: seeded() }).handle(get('/v1/audit'));
    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({ total: 3 });
    expect((response.body as { records: unknown[] }).records).toHaveLength(3);
  });

  it('filters by actor and by action', async () => {
    const handler = api({ audit: seeded() });
    const byActor = await handler.handle(get('/v1/audit?actor=edge-01'));
    expect(byActor.body).toMatchObject({ total: 2 });

    const byAction = await handler.handle(get('/v1/audit?action=DELETE'));
    expect(byAction.body).toMatchObject({ total: 1 });
  });

  it('filters by a time window', async () => {
    const audit = new AuditLedgerEngine();
    audit.append('u', 'SET', 'kv:a', {});
    const first = audit.query({})[0];
    if (first === undefined) throw new Error('expected a record');
    const response = await api({ audit }).handle(get(`/v1/audit?since=${first.timestamp}&until=${first.timestamp}`));
    expect(response.body).toMatchObject({ total: 1 });

    const none = await api({ audit }).handle(get(`/v1/audit?until=${first.timestamp - 1}`));
    expect(none.body).toMatchObject({ total: 0 });
  });

  it('caps the number of records returned but still reports the total', async () => {
    const audit = new AuditLedgerEngine();
    for (let i = 0; i < 5; i++) audit.append('u', 'SET', 'kv:a', { i });
    const response = await api({ audit }).handle(get('/v1/audit?limit=2'));
    expect(response.body).toMatchObject({ total: 5 });
    expect((response.body as { records: unknown[] }).records).toHaveLength(2);
  });

  it('uses a default limit and refuses limits outside the accepted range', async () => {
    const handler = api({ audit: seeded() });
    expect((await handler.handle(get('/v1/audit'))).status).toBe(200);
    expect((await handler.handle(get('/v1/audit?limit=0'))).status).toBe(400);
    expect((await handler.handle(get(`/v1/audit?limit=${MAX_AUDIT_LIMIT + 1}`))).status).toBe(400);
    expect((await handler.handle(get('/v1/audit?limit=2.5'))).status).toBe(400);
  });

  it('refuses a non-numeric time bound', async () => {
    const response = await api({ audit: seeded() }).handle(get('/v1/audit?since=yesterday'));
    expect(response.status).toBe(400);
  });

  it('returns copies, so a caller cannot change the ledger through the response', async () => {
    const audit = seeded();
    const handler = api({ audit });
    const response = await handler.handle(get('/v1/audit'));
    const records = (response.body as { records: Array<{ actorId: string }> }).records;
    if (records[0] !== undefined) records[0].actorId = 'forged';
    expect(audit.query({})[0]?.actorId).toBe('edge-01');
  });
});

describe('GET /v1/audit/verify', () => {
  it('reports a valid chain', async () => {
    const response = await api({ audit: seeded() }).handle(get('/v1/audit/verify'));
    expect(response.body).toEqual({ isValid: true, brokenAtIndex: null });
  });
});

describe('audit endpoints access', () => {
  it('needs a reader token when auth is on', async () => {
    const handler = api({ audit: seeded(), auth: new TokenAuth(specs) });
    expect((await handler.handle(get('/v1/audit'))).status).toBe(401);
    expect((await handler.handle(get('/v1/audit', `Bearer ${READER}`))).status).toBe(200);
    expect((await handler.handle(get('/v1/audit/verify', `Bearer ${WRITER}`))).status).toBe(200);
  });

  it('is absent when no ledger is configured', async () => {
    expect((await api({}).handle(get('/v1/audit'))).status).toBe(404);
    expect((await api({}).handle(get('/v1/audit/verify'))).status).toBe(404);
  });

  it('refuses writes to the audit paths', async () => {
    const handler = api({ audit: seeded() });
    const response = await handler.handle({ method: 'DELETE', url: '/v1/audit' });
    expect(response).toMatchObject({ status: 405, headers: { allow: 'GET' } });
  });

  it('does not treat the audit path as a key', async () => {
    const handler = api({ audit: seeded() });
    expect((await handler.handle(get('/v1/kv/audit'))).status).toBe(404);
  });
});
