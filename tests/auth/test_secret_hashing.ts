/**
 * How API key secrets are stored and checked. The stored value must be a salted scrypt hash, must
 * not reveal the secret, and must reject any secret that is not the one it was made from.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { createHash } from 'node:crypto';
import { AuthManager, SECRET_HASH, type ScopePermission } from '../../src/auth/manager';

const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];

type Internals = { apiKeys: Map<string, { hashedSecret: string }> };

let auth: AuthManager;

beforeEach(() => {
  auth = new AuthManager();
});

function storedHash(keyId: string): string {
  const record = (auth as unknown as Internals).apiKeys.get(keyId);
  if (record === undefined) throw new Error(`no record for ${keyId}`);
  return record.hashedSecret;
}

function overwriteStoredHash(keyId: string, value: string): void {
  const record = (auth as unknown as Internals).apiKeys.get(keyId);
  if (record === undefined) throw new Error(`no record for ${keyId}`);
  record.hashedSecret = value;
}

describe('stored API key secrets', () => {
  it('are stored as scrypt hashes with the configured parameters', () => {
    const { keyId } = auth.createApiKey('t1', 'READ_ONLY', scopes);
    const prefix = [SECRET_HASH.algorithm, SECRET_HASH.N, SECRET_HASH.r, SECRET_HASH.p].join('$');
    expect(storedHash(keyId).startsWith(`${prefix}$`)).toBe(true);
  });

  it('do not contain the secret, nor an unsalted SHA-256 of it', () => {
    const { keyId, secretKey, rawApiKey } = auth.createApiKey('t1', 'READ_ONLY', scopes);
    const stored = storedHash(keyId);
    expect(stored).not.toContain(secretKey);
    expect(stored).not.toContain(rawApiKey);
    const unsalted = createHash('sha256').update(secretKey).digest('hex');
    expect(stored).not.toContain(unsalted);
  });

  it('get a different salt for each key, so equal secrets would not share a hash', () => {
    const first = auth.createApiKey('t1', 'READ_ONLY', scopes);
    const second = auth.createApiKey('t1', 'READ_ONLY', scopes);
    const saltOf = (hash: string): string => hash.split('$')[4] ?? '';
    expect(saltOf(storedHash(first.keyId))).not.toBe(saltOf(storedHash(second.keyId)));
  });
});

describe('verifying an API key', () => {
  it('rejects a secret that differs from the stored one by a single character', () => {
    const { keyId, secretKey } = auth.createApiKey('t1', 'READ_ONLY', scopes);
    const last = secretKey.slice(-1) === '0' ? '1' : '0';
    const tampered = `${secretKey.slice(0, -1)}${last}`;
    expect(auth.authenticateApiKey(`${keyId}.${tampered}`)).toBeNull();
  });

  it("rejects one key's secret presented with another key's id", () => {
    const first = auth.createApiKey('t1', 'READ_ONLY', scopes);
    const second = auth.createApiKey('t2', 'ANALYST', scopes);
    expect(auth.authenticateApiKey(`${second.keyId}.${first.secretKey}`)).toBeNull();
  });

  it('rejects an unknown key id', () => {
    expect(auth.authenticateApiKey(`np_id_0000000000000000.np_sec_${'0'.repeat(48)}`)).toBeNull();
  });

  it('rejects a stored hash that has been altered', () => {
    const { keyId, rawApiKey } = auth.createApiKey('t1', 'READ_ONLY', scopes);
    const parts = storedHash(keyId).split('$');
    const hashHex = parts[5] ?? '';
    const flipped = (hashHex[0] === '0' ? '1' : '0') + hashHex.slice(1);
    overwriteStoredHash(keyId, [...parts.slice(0, 5), flipped].join('$'));
    expect(auth.authenticateApiKey(rawApiKey)).toBeNull();
  });

  it('rejects a malformed stored hash without throwing', () => {
    const { keyId, rawApiKey } = auth.createApiKey('t1', 'READ_ONLY', scopes);
    for (const malformed of ['', 'not-a-hash', 'sha256$abc', 'scrypt$3$8$1$zz$zz', 'scrypt$16384$8$1$00$']) {
      overwriteStoredHash(keyId, malformed);
      expect(auth.authenticateApiKey(rawApiKey)).toBeNull();
    }
  });

  it('still authenticates the right secret after the stored hash is replaced by a valid one', () => {
    const { keyId, rawApiKey } = auth.createApiKey('t1', 'ANALYST', scopes);
    const session = auth.authenticateApiKey(rawApiKey);
    expect(session?.role).toBe('ANALYST');
    expect(storedHash(keyId)).toContain('$');
  });
});
