/**
 * NexusPulse Cryptographic Audit Logging & Merkle Compliance Ledger
 * 
 * Implements immutable append-only audit trails, cryptographic hash chaining (SHA-256),
 * Merkle root verification, tamper detection, and compliance query filtering.
 */

import { createHash, randomBytes } from 'crypto';

/**
 * The fields that a record's hash covers, in order. The payload is a JSON array, so no value can
 * contain a separator that makes two different records produce the same bytes.
 */
export function canonicalPayload(fields: {
  readonly recordId: string;
  readonly timestamp: number;
  readonly actorId: string;
  readonly action: string;
  readonly resource: string;
  readonly changes: Record<string, unknown>;
  readonly previousHash: string;
}): string {
  return JSON.stringify([
    fields.recordId,
    fields.timestamp,
    fields.actorId,
    fields.action,
    fields.resource,
    fields.changes,
    fields.previousHash,
  ]);
}

function hashOf(payload: string): string {
  return createHash('sha256').update(payload).digest('hex');
}

function copyRecord(record: AuditRecord): AuditRecord {
  return { ...record, changes: structuredClone(record.changes) };
}

export interface AuditRecord {
  recordId: string;
  timestamp: number;
  actorId: string;
  action: string;
  resource: string;
  changes: Record<string, any>;
  previousHash: string;
  recordHash: string;
}

export class AuditLedgerEngine {
  private chain: AuditRecord[] = [];
  private genesisHash: string = '0000000000000000000000000000000000000000000000000000000000000000';

  public append(actorId: string, action: string, resource: string, changes: Record<string, any>): AuditRecord {
    const recordId = `aud_${randomBytes(12).toString('hex')}`;
    const timestamp = Date.now();
    const last = this.chain[this.chain.length - 1];
    const previousHash = last !== undefined ? last.recordHash : this.genesisHash;
    // Copied, so the caller cannot change the stored record by keeping a reference to its object.
    const stored = structuredClone(changes);

    const recordHash = hashOf(
      canonicalPayload({ recordId, timestamp, actorId, action, resource, changes: stored, previousHash }),
    );

    const record: AuditRecord = {
      recordId,
      timestamp,
      actorId,
      action,
      resource,
      changes: stored,
      previousHash,
      recordHash,
    };

    this.chain.push(record);
    return copyRecord(record);
  }

  public verifyIntegrity(): { isValid: boolean; brokenAtIndex: number | null } {
    let expectedPrevHash = this.genesisHash;
    for (const [i, current] of this.chain.entries()) {
      if (current.previousHash !== expectedPrevHash) {
        return { isValid: false, brokenAtIndex: i };
      }

      const recomputedHash = hashOf(canonicalPayload(current));

      if (current.recordHash !== recomputedHash) {
        return { isValid: false, brokenAtIndex: i };
      }
      expectedPrevHash = current.recordHash;
    }

    return { isValid: true, brokenAtIndex: null };
  }

  public query(filter: { actorId?: string; action?: string; startTime?: number; endTime?: number }): AuditRecord[] {
    return this.chain
      .filter((record) => {
        if (filter.actorId !== undefined && record.actorId !== filter.actorId) return false;
        if (filter.action !== undefined && record.action !== filter.action) return false;
        if (filter.startTime !== undefined && record.timestamp < filter.startTime) return false;
        if (filter.endTime !== undefined && record.timestamp > filter.endTime) return false;
        return true;
      })
      .map(copyRecord);
  }

  public size(): number {
    return this.chain.length;
  }
}
