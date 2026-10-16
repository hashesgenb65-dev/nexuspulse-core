import { describe, it, expect, beforeEach } from 'vitest';
import { AuthManager, ScopePermission } from '../src/auth/manager';

describe('AuthManager Module', () => {
  let auth: AuthManager;

  beforeEach(() => {
    auth = new AuthManager();
  });

  const defaultScopes: ScopePermission[] = [
    { resource: 'metrics', actions: ['read', 'write'] },
    { resource: 'reports', actions: ['read'] },
  ];

  it('should generate valid API key pairs and authenticate successfully', () => {
    const { rawApiKey, keyId } = auth.createApiKey('tenant_alpha', 'ANALYST', defaultScopes);

    expect(rawApiKey).toContain(keyId);

    const session = auth.authenticateApiKey(rawApiKey);
    expect(session).not.toBeNull();
    expect(session?.tenantId).toBe('tenant_alpha');
    expect(session?.role).toBe('ANALYST');
  });

  it('should reject malformed or incorrect API key secrets', () => {
    const { keyId } = auth.createApiKey('tenant_alpha', 'ANALYST', defaultScopes);

    const invalidKey = `${keyId}.wrong_secret_payload`;
    const session = auth.authenticateApiKey(invalidKey);

    expect(session).toBeNull();
  });

  it('should correctly authorize permitted actions and reject unauthorized access', () => {
    const { rawApiKey } = auth.createApiKey('tenant_beta', 'ANALYST', defaultScopes);
    const session = auth.authenticateApiKey(rawApiKey)!;

    expect(auth.authorize(session, 'metrics', 'read')).toBe(true);
    expect(auth.authorize(session, 'metrics', 'write')).toBe(true);
    expect(auth.authorize(session, 'metrics', 'delete')).toBe(false);
    expect(auth.authorize(session, 'system', 'admin')).toBe(false);
  });

  it('should allow global ADMIN role full authorization override', () => {
    const { rawApiKey } = auth.createApiKey('tenant_admin', 'ADMIN', []);
    const session = auth.authenticateApiKey(rawApiKey)!;

    expect(auth.authorize(session, 'any_resource', 'delete')).toBe(true);
  });

  it('should enforce API key revocation', () => {
    const { rawApiKey, keyId } = auth.createApiKey('tenant_gamma', 'INGEST_SERVICE', defaultScopes);

    // Revoke key
    const revoked = auth.revokeApiKey(keyId);
    expect(revoked).toBe(true);

    // Authentication attempt after revocation should fail
    const session = auth.authenticateApiKey(rawApiKey);
    expect(session).toBeNull();
  });

  it('should reject expired API keys', async () => {
    // Create key with 1-second TTL
    const { rawApiKey } = auth.createApiKey('tenant_ttl', 'READ_ONLY', defaultScopes, 100, 1);

    // Fast-forward simulated expiration (or small timeout test)
    await new Promise((resolve) => setTimeout(resolve, 1100));

    const session = auth.authenticateApiKey(rawApiKey);
    expect(session).toBeNull();
  });
});
