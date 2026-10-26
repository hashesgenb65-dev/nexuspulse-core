/**
 * The combined authenticator: a token from the token file or a live session both authenticate, auth
 * roles map onto reader and writer, and a session dies with its API key.
 */

import { describe, it, expect } from 'vitest';
import { TokenAuth, generateSecret, principalOf, type TokenSpec } from '../../src/api/auth';
import { apiRoleOf, SessionAuthenticator } from '../../src/api/session_auth';
import { AuthManager, type ScopePermission, type UserRole } from '../../src/auth/manager';
import type { RequestContext } from '../../src/api/http_router';

const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];

function tokenAuth(): TokenAuth {
  const specs: TokenSpec[] = [{ name: 'ci', secret: generateSecret(), role: 'reader' }];
  return new TokenAuth(specs);
}

function login(manager: AuthManager, role: UserRole): { sessionId: string; rawApiKey: string; keyId: string } {
  const { keyId, rawApiKey } = manager.createApiKey('t1', role, scopes);
  const session = manager.authenticateApiKey(rawApiKey);
  if (session === null) throw new Error('login failed');
  return { sessionId: session.sessionId, rawApiKey, keyId };
}

function statusOf(run: () => unknown): number | undefined {
  try {
    run();
    return undefined;
  } catch (error) {
    return (error as { status?: number }).status;
  }
}

describe('role mapping', () => {
  it('maps ADMIN and INGEST_SERVICE to writer, and ANALYST and READ_ONLY to reader', () => {
    expect(apiRoleOf('ADMIN')).toBe('writer');
    expect(apiRoleOf('INGEST_SERVICE')).toBe('writer');
    expect(apiRoleOf('ANALYST')).toBe('reader');
    expect(apiRoleOf('READ_ONLY')).toBe('reader');
  });
});

describe('verifying a secret', () => {
  it('accepts a token from the token file, and describes it', () => {
    const secret = generateSecret();
    const tokens = new TokenAuth([{ name: 'ci', secret, role: 'reader' }]);
    const auth = new SessionAuthenticator(tokens, new AuthManager());
    expect(auth.verifySecret(secret)).toEqual({ name: 'ci', role: 'reader' });
    expect(auth.size).toBe(1);
    expect(auth.describe()).toEqual([{ name: 'ci', role: 'reader' }]);
  });

  it('accepts a session id from a login, with the key id as the principal name', () => {
    const manager = new AuthManager();
    const { sessionId, keyId } = login(manager, 'ADMIN');
    const auth = new SessionAuthenticator(undefined, manager);
    expect(auth.verifySecret(sessionId)).toEqual({ name: keyId, role: 'writer' });
  });

  it('refuses a session once its API key is revoked', () => {
    const manager = new AuthManager();
    const { sessionId, keyId } = login(manager, 'ANALYST');
    manager.revokeApiKey(keyId);
    const auth = new SessionAuthenticator(undefined, manager);
    expect(auth.verifySecret(sessionId)).toBeUndefined();
  });

  it('refuses an unknown secret', () => {
    const auth = new SessionAuthenticator(tokenAuth(), new AuthManager());
    expect(auth.verifySecret('sess_' + '0'.repeat(32))).toBeUndefined();
    expect(auth.verifySecret('not-a-session')).toBeUndefined();
  });

  it('works with no token file, using sessions alone', () => {
    const manager = new AuthManager();
    const { sessionId } = login(manager, 'READ_ONLY');
    const auth = new SessionAuthenticator(undefined, manager);
    expect(auth.size).toBe(0);
    expect(auth.describe()).toEqual([]);
    expect(auth.verifySecret(sessionId)?.role).toBe('reader');
  });
});

describe('the guard', () => {
  function request(authorization: string | undefined): RequestContext {
    const headers: Record<string, string> = authorization === undefined ? {} : { authorization };
    return { req: { headers } } as unknown as RequestContext;
  }

  it('answers 401 for a missing or unknown credential, and 403 for a reader on a writer route', () => {
    const manager = new AuthManager();
    const reader = login(manager, 'READ_ONLY');
    const auth = new SessionAuthenticator(undefined, manager);
    const guarded = auth.guard('writer', () => undefined);

    expect(statusOf(() => guarded(request(undefined)))).toBe(401);
    expect(statusOf(() => guarded(request('Bearer sess_' + '1'.repeat(32))))).toBe(401);
    expect(statusOf(() => guarded(request(`Bearer ${reader.sessionId}`)))).toBe(403);
  });

  it('lets a writer session through and records the principal', () => {
    const manager = new AuthManager();
    const writer = login(manager, 'INGEST_SERVICE');
    const auth = new SessionAuthenticator(undefined, manager);
    const ctx = request(`Bearer ${writer.sessionId}`);
    const guarded = auth.guard('writer', () => 'done' as never);
    expect(guarded(ctx)).toBe('done');
    expect(principalOf(ctx)).toEqual({ name: writer.keyId, role: 'writer' });
  });
});
