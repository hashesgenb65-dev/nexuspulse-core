/**
 * Audit ledger hardening: the hash covers unambiguous bytes, record ids are unique, stored records
 * cannot be changed through references the caller holds, and a zero timestamp filter is honoured.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { AuditLedgerEngine, canonicalPayload } from '../../src/audit/ledger';

const base = {
  recordId: 'aud_1',
  timestamp: 1,
  changes: {},
  previousHash: '0'.repeat(64),
};

let ledger: AuditLedgerEngine;

beforeEach(() => {
  ledger = new AuditLedgerEngine();
});

describe('canonical payload', () => {
  it('does not let a colon in one field produce the same bytes as another field split', () => {
    const a = canonicalPayload({ ...base, actorId: 'x:y', action: 'z', resource: 'r' });
    const b = canonicalPayload({ ...base, actorId: 'x', action: 'y:z', resource: 'r' });
    expect(a).not.toBe(b);
  });

  it('covers every field, so changing any one of them changes the payload', () => {
    const original = { ...base, actorId: 'u', action: 'A', resource: 'R' };
    const reference = canonicalPayload(original);
    expect(canonicalPayload({ ...original, timestamp: 2 })).not.toBe(reference);
    expect(canonicalPayload({ ...original, actorId: 'v' })).not.toBe(reference);
    expect(canonicalPayload({ ...original, action: 'B' })).not.toBe(reference);
    expect(canonicalPayload({ ...original, resource: 'S' })).not.toBe(reference);
    expect(canonicalPayload({ ...original, changes: { n: 1 } })).not.toBe(reference);
    expect(canonicalPayload({ ...original, previousHash: 'f'.repeat(64) })).not.toBe(reference);
  });
});

describe('record ids', () => {
  it('are unique across many appends', () => {
    const ids = new Set<string>();
    for (let i = 0; i < 5_000; i++) ids.add(ledger.append('u', 'A', 'R', { i }).recordId);
    expect(ids.size).toBe(5_000);
  });

  it('are long enough that they are not guessable', () => {
    expect(ledger.append('u', 'A', 'R', {}).recordId).toMatch(/^aud_[0-9a-f]{24}$/);
  });
});

describe('stored records are not shared with the caller', () => {
  it('ignores later changes to the object passed to append', () => {
    const changes: Record<string, unknown> = { replicas: 3 };
    ledger.append('u', 'UPDATE', 'cluster', changes);
    changes.replicas = 999;

    expect(ledger.query({})[0]?.changes).toEqual({ replicas: 3 });
    expect(ledger.verifyIntegrity().isValid).toBe(true);
  });

  it('returns copies from append, so changing the result does not change history', () => {
    const returned = ledger.append('u', 'UPDATE', 'cluster', { replicas: 3 });
    returned.changes.replicas = 999;
    expect(ledger.query({})[0]?.changes).toEqual({ replicas: 3 });
    expect(ledger.verifyIntegrity().isValid).toBe(true);
  });

  it('returns copies from query, so changing a result does not change history', () => {
    ledger.append('u', 'UPDATE', 'cluster', { replicas: 3 });
    const [first] = ledger.query({});
    if (first === undefined) throw new Error('expected a record');
    (first.changes as Record<string, unknown>).replicas = 999;
    first.actorId = 'someone-else';

    expect(ledger.query({})[0]?.changes).toEqual({ replicas: 3 });
    expect(ledger.query({})[0]?.actorId).toBe('u');
    expect(ledger.verifyIntegrity().isValid).toBe(true);
  });

  it('refuses values that cannot be copied, such as functions', () => {
    expect(() => ledger.append('u', 'A', 'R', { fn: () => 1 })).toThrow();
    expect(ledger.size()).toBe(0);
  });
});

describe('query filters', () => {
  it('honours a start time of zero', () => {
    ledger.append('u', 'A', 'R', {});
    expect(ledger.query({ startTime: 0 })).toHaveLength(1);
  });

  it('honours an end time of zero', () => {
    ledger.append('u', 'A', 'R', {});
    expect(ledger.query({ endTime: 0 })).toHaveLength(0);
  });
});
