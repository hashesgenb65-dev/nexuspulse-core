/**
 * The key-value API writes committed changes to the audit ledger. Only successful writes are
 * recorded, the actor is the token name, and values are never stored in the ledger.
 */

import { describe, it, expect } from 'vitest';
import { TokenAuth, type TokenSpec } from '../../src/api/auth';
import { AuditLedgerEngine } from '../../src/audit/ledger';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi, type KvPeerHttp } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';

const WRITER = 'writer-secret-0123456789abcdefgh';
const READER = 'reader-secret-0123456789abcdefgh';
const specs: TokenSpec[] = [
  { name: 'edge-01', role: 'writer', secret: WRITER },
  { name: 'dash', role: 'reader', secret: READER },
];

function soloLeader(options: { audit?: AuditLedgerEngine; auth?: TokenAuth }): KvApi {
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
    ...(options.audit === undefined ? {} : { audit: options.audit }),
    ...(options.auth === undefined ? {} : { auth: options.auth }),
  });
}

const put = (key: string, value: unknown, authorization?: string): { method: string; url: string; body: string; authorization?: string } => ({
  method: 'PUT',
  url: `/v1/kv/${key}`,
  body: JSON.stringify({ value }),
  ...(authorization === undefined ? {} : { authorization }),
});

describe('key-value writes in the audit ledger', () => {
  it('records a committed PUT with the key, index, term and value size', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit });

    await api.handle(put('sensor/1', { temp: 21 }));
    const [record] = audit.query({});
    expect(record?.action).toBe('SET');
    expect(record?.resource).toBe('kv:sensor/1');
    expect(record?.changes).toMatchObject({ index: 1, valueBytes: Buffer.byteLength('{"temp":21}') });
  });

  it('records a committed DELETE', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit });

    await api.handle(put('a', 1));
    await api.handle({ method: 'DELETE', url: '/v1/kv/a' });
    const records = audit.query({ action: 'DELETE' });
    expect(records).toHaveLength(1);
    expect(records[0]?.resource).toBe('kv:a');
  });

  it('does not store the value itself in the ledger', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit });

    await api.handle(put('secret', 'hunter2-very-sensitive'));
    expect(JSON.stringify(audit.query({}))).not.toContain('hunter2-very-sensitive');
  });

  it('names the token that made the write', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit, auth: new TokenAuth(specs) });

    await api.handle(put('a', 1, `Bearer ${WRITER}`));
    expect(audit.query({})[0]?.actorId).toBe('edge-01');
  });

  it('names writes made without a token store as unauthenticated', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit });

    await api.handle(put('a', 1));
    expect(audit.query({})[0]?.actorId).toBe('unauthenticated');
  });

  it('does not record a write refused for lack of a token', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit, auth: new TokenAuth(specs) });

    expect((await api.handle(put('a', 1))).status).toBe(401);
    expect(audit.size()).toBe(0);
  });

  it('does not record a write refused for lack of role', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit, auth: new TokenAuth(specs) });

    expect((await api.handle(put('a', 1, `Bearer ${READER}`))).status).toBe(403);
    expect(audit.size()).toBe(0);
  });

  it('does not record a request that fails validation', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit });

    expect((await api.handle({ method: 'PUT', url: '/v1/kv/a', body: 'nope' })).status).toBe(400);
    expect(audit.size()).toBe(0);
  });

  it('keeps the ledger verifiable after several writes', async () => {
    const audit = new AuditLedgerEngine();
    const api = soloLeader({ audit });

    for (let i = 0; i < 5; i++) await api.handle(put(`k${i}`, i));
    expect(audit.size()).toBe(5);
    expect(audit.verifyIntegrity().isValid).toBe(true);
  });

  it('is off when no ledger is configured', async () => {
    const api = soloLeader({});
    expect((await api.handle(put('a', 1))).status).toBe(200);
  });
});
