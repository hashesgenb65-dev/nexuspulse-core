import { describe, it, expect, beforeEach } from 'vitest';
import { AuditLedgerEngine } from '../../src/audit/ledger';

describe('AuditLedgerEngine Module', () => {
  let ledger: AuditLedgerEngine;

  beforeEach(() => {
    ledger = new AuditLedgerEngine();
  });

  it('should append immutable audit records with cryptographic hash chaining', () => {
    const rec1 = ledger.append('usr_admin', 'CREATE', 'config_cluster', { replicas: 3 });
    const rec2 = ledger.append('usr_admin', 'UPDATE', 'config_cluster', { replicas: 5 });

    expect(rec1.previousHash).toBe('0000000000000000000000000000000000000000000000000000000000000000');
    expect(rec2.previousHash).toBe(rec1.recordHash);
    expect(ledger.size()).toBe(2);
  });

  it('should verify ledger chain integrity successfully', () => {
    ledger.append('usr_1', 'LOGIN', 'auth_service', { ip: '127.0.0.1' });
    ledger.append('usr_1', 'WRITE', 'metrics_table', { count: 100 });

    const integrity = ledger.verifyIntegrity();
    expect(integrity.isValid).toBe(true);
    expect(integrity.brokenAtIndex).toBeNull();
  });

  it('should filter audit logs by actor and action criteria', () => {
    ledger.append('usr_alice', 'CREATE', 'table_a', {});
    ledger.append('usr_bob', 'DELETE', 'table_b', {});
    ledger.append('usr_alice', 'UPDATE', 'table_c', {});

    const aliceLogs = ledger.query({ actorId: 'usr_alice' });
    expect(aliceLogs).toHaveLength(2);

    const deleteLogs = ledger.query({ action: 'DELETE' });
    expect(deleteLogs).toHaveLength(1);
    expect(deleteLogs[0]?.actorId).toBe('usr_bob');
  });
});
