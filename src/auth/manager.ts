/**
 * NexusPulse Authentication & Role-Based Access Control (RBAC) Engine
 * 
 * Provides API key generation, cryptographic signature verification,
 * token bucket authorization, permission scoping, and session lifecycle management.
 */

import { createHash, randomBytes } from 'crypto';

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

    if (!record || !record.isActive) return null;

    // Check expiration
    if (record.expiresAt && Date.now() > record.expiresAt) {
      return null;
    }

    // Verify cryptographic secret hash
    const hashedAttempt = this.hashSecret(secretKey);
    if (hashedAttempt !== record.hashedSecret) {
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
   * Helper hash function for key secret protection using SHA-256.
   */
  private hashSecret(secret: string): string {
    return createHash('sha256').update(secret).digest('hex');
  }
}
