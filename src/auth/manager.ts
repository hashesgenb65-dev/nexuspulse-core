/**
 * NexusPulse Authentication & Role-Based Access Control (RBAC) Engine
 * 
 * Provides API key generation, cryptographic signature verification,
 * token bucket authorization, permission scoping, and session lifecycle management.
 */

import { randomBytes, scryptSync, timingSafeEqual } from 'crypto';

/**
 * Parameters for hashing API key secrets with scrypt. Each key gets its own random salt, and the
 * parameters are stored with the hash so they can change later without invalidating old keys.
 */
export const SECRET_HASH = Object.freeze({
  algorithm: 'scrypt',
  N: 16_384,
  r: 8,
  p: 1,
  keyLength: 32,
  saltBytes: 16,
});

const SCRYPT_MAX_MEMORY_BYTES = 64 * 1024 * 1024;
const MAX_STORED_COST = 1 << 20;

export type UserRole = 'ADMIN' | 'ANALYST' | 'INGEST_SERVICE' | 'READ_ONLY';

export interface ScopePermission {
  resource: string; // e.g., 'metrics', 'users', 'system'
  actions: Array<'read' | 'write' | 'delete' | 'admin'>;
}

export interface ApiKeyRecord {
  keyId: string;
  hashedSecret: string;
  tenantId: string;
  role: UserRole;
  scopes: ScopePermission[];
  createdAt: number;
  expiresAt: number | null;
  isActive: boolean;
  rateLimitPerMin: number;
}

export interface AuthSession {
  sessionId: string;
  tenantId: string;
  role: UserRole;
  permissions: ScopePermission[];
  authenticatedAt: number;
}

export class AuthManager {
  private apiKeys: Map<string, ApiKeyRecord> = new Map(); // keyId -> Record
  private activeSessions: Map<string, AuthSession> = new Map();
  /** A hash that no real secret matches. Checked when a key id is unknown, so timing does not reveal which ids exist. */
  private readonly decoyHash: string = this.hashSecret(randomBytes(24).toString('hex'));

  /**
   * Generates a new cryptographically secure API key pair (Key ID + Secret).
   */
  public createApiKey(
    tenantId: string,
    role: UserRole,
    scopes: ScopePermission[],
    rateLimitPerMin: number = 1000,
    ttlSeconds?: number
  ): { keyId: string; secretKey: string; rawApiKey: string } {
    const keyId = `np_id_${randomBytes(8).toString('hex')}`;
    const secretKey = `np_sec_${randomBytes(24).toString('hex')}`;
    const rawApiKey = `${keyId}.${secretKey}`;

    const hashedSecret = this.hashSecret(secretKey);
    const now = Date.now();
    const expiresAt = ttlSeconds ? now + ttlSeconds * 1000 : null;

    const record: ApiKeyRecord = {
      keyId,
      hashedSecret,
      tenantId,
      role,
      scopes,
      createdAt: now,
      expiresAt,
      isActive: true,
      rateLimitPerMin,
    };

    this.apiKeys.set(keyId, record);

    return { keyId, secretKey, rawApiKey };
  }

  /**
   * Authenticates an incoming raw API key string ("keyId.secretKey").
   */
  public authenticateApiKey(rawApiKey: string): AuthSession | null {
    const parts = rawApiKey.split('.');
    if (parts.length !== 2) return null;

    const keyId = parts[0];
    const secretKey = parts[1];
    if (keyId === undefined || secretKey === undefined) return null;
    const record = this.apiKeys.get(keyId);

    // Always run one verification, even for an unknown or inactive key, so the response time does
    // not show which key ids exist.
    const secretMatches = this.verifySecret(secretKey, record?.hashedSecret ?? this.decoyHash);

    if (!record || !record.isActive || !secretMatches) return null;

    // Check expiration
    if (record.expiresAt && Date.now() > record.expiresAt) {
      return null;
    }

    // Create session token
    const sessionId = `sess_${randomBytes(16).toString('hex')}`;
    const session: AuthSession = {
      sessionId,
      tenantId: record.tenantId,
      role: record.role,
      permissions: record.scopes,
      authenticatedAt: Date.now(),
    };

    this.activeSessions.set(sessionId, session);
    return session;
  }

  /**
   * Validates if a session/role possesses permissions to perform an action on a resource.
   */
  public authorize(
    session: AuthSession,
    resource: string,
    action: 'read' | 'write' | 'delete' | 'admin'
  ): boolean {
    if (session.role === 'ADMIN') return true; // Global admin bypass

    const matchedScope = session.permissions.find(
      (s) => s.resource === resource || s.resource === '*'
    );

    if (!matchedScope) return false;

    return matchedScope.actions.includes(action) || matchedScope.actions.includes('admin');
  }

  /**
   * Revokes an existing API key immediately.
   */
  public revokeApiKey(keyId: string): boolean {
    const record = this.apiKeys.get(keyId);
    if (!record) return false;

    record.isActive = false;
    return true;
  }

  /**
   * Hashes a secret with scrypt and a fresh random salt. The result has the form
   * `scrypt$N$r$p$saltHex$hashHex`, which holds everything needed to verify it later.
   */
  private hashSecret(secret: string): string {
    const salt = randomBytes(SECRET_HASH.saltBytes);
    const derived = scryptSync(secret, salt, SECRET_HASH.keyLength, {
      N: SECRET_HASH.N,
      r: SECRET_HASH.r,
      p: SECRET_HASH.p,
      maxmem: SCRYPT_MAX_MEMORY_BYTES,
    });
    return [SECRET_HASH.algorithm, SECRET_HASH.N, SECRET_HASH.r, SECRET_HASH.p, salt.toString('hex'), derived.toString('hex')].join('$');
  }

  /**
   * Checks a secret against a stored hash in constant time. Returns false for any stored value that
   * is malformed or uses parameters outside the accepted range, rather than throwing.
   */
  private verifySecret(secret: string, stored: string): boolean {
    const parts = stored.split('$');
    if (parts.length !== 6 || parts[0] !== SECRET_HASH.algorithm) return false;

    const [, nText, rText, pText, saltHex, hashHex] = parts;
    const N = Number(nText);
    const r = Number(rText);
    const p = Number(pText);
    if (!Number.isInteger(N) || N < 2 || N > MAX_STORED_COST || (N & (N - 1)) !== 0) return false;
    if (!Number.isInteger(r) || r < 1 || r > 32) return false;
    if (!Number.isInteger(p) || p < 1 || p > 16) return false;
    if (saltHex === undefined || hashHex === undefined || !/^[0-9a-f]+$/.test(saltHex) || !/^[0-9a-f]+$/.test(hashHex)) {
      return false;
    }

    const expected = Buffer.from(hashHex, 'hex');
    try {
      const derived = scryptSync(secret, Buffer.from(saltHex, 'hex'), expected.length, {
        N,
        r,
        p,
        maxmem: SCRYPT_MAX_MEMORY_BYTES,
      });
      return timingSafeEqual(derived, expected);
    } catch {
      return false;
    }
  }
}
