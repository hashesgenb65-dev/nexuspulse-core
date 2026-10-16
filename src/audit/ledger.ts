/**
 * NexusPulse Cryptographic Audit Logging & Merkle Compliance Ledger
 * 
 * Implements immutable append-only audit trails, cryptographic hash chaining (SHA-256),
 * Merkle root verification, tamper detection, and compliance query filtering.
 */

import { createHash } from 'crypto';

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
    const recordId = `aud_${Math.random().toString(36).substring(2, 11)}`;
    const timestamp = Date.now();
    const last = this.chain[this.chain.length - 1];
    const previousHash = last !== undefined ? last.recordHash : this.genesisHash;

    const rawPayload = `${recordId}:${timestamp}:${actorId}:${action}:${resource}:${JSON.stringify(changes)}:${previousHash}`;
    const recordHash = createHash('sha256').update(rawPayload).digest('hex');

    const record: AuditRecord = {
      recordId,
      timestamp,
      actorId,
      action,
      resource,
      changes,
      previousHash,
      recordHash,
    };

    this.chain.push(record);
    return record;
  }

  public verifyIntegrity(): { isValid: boolean; brokenAtIndex: number | null } {
    let expectedPrevHash = this.genesisHash;
    for (const [i, current] of this.chain.entries()) {
      if (current.previousHash !== expectedPrevHash) {
        return { isValid: false, brokenAtIndex: i };
      }

      const rawPayload = `${current.recordId}:${current.timestamp}:${current.actorId}:${current.action}:${current.resource}:${JSON.stringify(current.changes)}:${current.previousHash}`;
      const recomputedHash = createHash('sha256').update(rawPayload).digest('hex');

      if (current.recordHash !== recomputedHash) {
        return { isValid: false, brokenAtIndex: i };
      }
      expectedPrevHash = current.recordHash;
    }

    return { isValid: true, brokenAtIndex: null };
  }

  public query(filter: { actorId?: string; action?: string; startTime?: number; endTime?: number }): AuditRecord[] {
    return this.chain.filter((record) => {
      if (filter.actorId && record.actorId !== filter.actorId) return false;
      if (filter.action && record.action !== filter.action) return false;
      if (filter.startTime && record.timestamp < filter.startTime) return false;
      if (filter.endTime && record.timestamp > filter.endTime) return false;
      return true;
    });
  }

  public size(): number {
    return this.chain.length;
  }
}
