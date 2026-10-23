/**
 * Session lifetime in AuthManager: sessions expire, expired sessions are removed, revoking a key
 * ends its sessions, and the store does not grow without bound. A fake clock drives time.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { AuthManager, DEFAULT_SESSION_TTL_MS, type ScopePermission } from '../../src/auth/manager';

const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];
const HOUR = 60 * 60 * 1000;

let nowMs: number;
let auth: AuthManager;
const clock = (): number => nowMs;

beforeEach(() => {
  nowMs = 1_700_000_000_000;
  auth = new AuthManager({ now: clock });
});

describe('session lifetime', () => {
  it('gives a new session an expiry one TTL after login', () => {
    const { rawApiKey } = auth.createApiKey('t1', 'ANALYST', scopes);
    const session = auth.authenticateApiKey(rawApiKey);
    expect(session?.authenticatedAt).toBe(nowMs);
    expect(session?.expiresAt).toBe(nowMs + DEFAULT_SESSION_TTL_MS);
  });

  it('keeps a session valid until its expiry, then refuses it', () => {
    const { rawApiKey } = auth.createApiKey('t1', 'ANALYST', scopes);
    const session = auth.authenticateApiKey(rawApiKey)!;

    nowMs = session.expiresAt - 1;
    expect(auth.validateSession(session.sessionId)).not.toBeNull();

    nowMs = session.expiresAt;
    expect(auth.validateSession(session.sessionId)).toBeNull();
  });

  it('removes an expired session on the first check after it expires', () => {
    const { rawApiKey } = auth.createApiKey('t1', 'ANALYST', scopes);
    const session = auth.authenticateApiKey(rawApiKey)!;
    expect(auth.sessionCount).toBe(1);

    nowMs = session.expiresAt + 1;
    expect(auth.validateSession(session.sessionId)).toBeNull();
    expect(auth.sessionCount).toBe(0);
  });

  it('honours a custom TTL', () => {
    const short = new AuthManager({ sessionTtlMs: 5_000, now: clock });
    const { rawApiKey } = short.createApiKey('t1', 'ANALYST', scopes);
    const session = short.authenticateApiKey(rawApiKey)!;
    expect(session.expiresAt - session.authenticatedAt).toBe(5_000);
  });

  it('rejects a TTL outside 1 ms to 24 hours', () => {
    expect(() => new AuthManager({ sessionTtlMs: 0 })).toThrow(RangeError);
    expect(() => new AuthManager({ sessionTtlMs: 1.5 })).toThrow(RangeError);
    expect(() => new AuthManager({ sessionTtlMs: 25 * HOUR })).toThrow(RangeError);
    expect(() => new AuthManager({ sessionTtlMs: 24 * HOUR })).not.toThrow();
  });
});

describe('revoking sessions', () => {
  it('ends one session without affecting others', () => {
    const { rawApiKey } = auth.createApiKey('t1', 'ANALYST', scopes);
    const first = auth.authenticateApiKey(rawApiKey)!;
    const second = auth.authenticateApiKey(rawApiKey)!;

    expect(auth.revokeSession(first.sessionId)).toBe(true);
    expect(auth.validateSession(first.sessionId)).toBeNull();
    expect(auth.validateSession(second.sessionId)).not.toBeNull();
  });

  it('returns false when the session does not exist', () => {
    expect(auth.revokeSession('sess_missing')).toBe(false);
  });

  it('ends every session opened with a key when that key is revoked', () => {
    const target = auth.createApiKey('t1', 'ANALYST', scopes);
    const other = auth.createApiKey('t2', 'ANALYST', scopes);
    const a = auth.authenticateApiKey(target.rawApiKey)!;
    const b = auth.authenticateApiKey(target.rawApiKey)!;
    const unrelated = auth.authenticateApiKey(other.rawApiKey)!;

    expect(auth.revokeApiKey(target.keyId)).toBe(true);

    expect(auth.validateSession(a.sessionId)).toBeNull();
    expect(auth.validateSession(b.sessionId)).toBeNull();
    expect(auth.validateSession(unrelated.sessionId)).not.toBeNull();
  });

  it('records the key id on each session', () => {
    const { rawApiKey, keyId } = auth.createApiKey('t1', 'ANALYST', scopes);
    expect(auth.authenticateApiKey(rawApiKey)?.keyId).toBe(keyId);
  });
});

describe('bounding the session store', () => {
  it('purges expired sessions and reports how many it removed', () => {
    const { rawApiKey } = auth.createApiKey('t1', 'ANALYST', scopes);
    auth.authenticateApiKey(rawApiKey);
    auth.authenticateApiKey(rawApiKey);
    expect(auth.sessionCount).toBe(2);

    nowMs += DEFAULT_SESSION_TTL_MS;
    expect(auth.purgeExpiredSessions()).toBe(2);
    expect(auth.sessionCount).toBe(0);
  });

  it('keeps sessions that have not expired when purging', () => {
    const { rawApiKey } = auth.createApiKey('t1', 'ANALYST', scopes);
    const old = auth.authenticateApiKey(rawApiKey)!;
    nowMs += DEFAULT_SESSION_TTL_MS - 1_000;
    const fresh = auth.authenticateApiKey(rawApiKey)!;

    nowMs = old.expiresAt + 1;
    expect(auth.purgeExpiredSessions()).toBe(1);
    expect(auth.validateSession(fresh.sessionId)).not.toBeNull();
  });

  it('purges during login, so abandoned sessions do not pile up', () => {
    const { rawApiKey } = auth.createApiKey('t1', 'ANALYST', scopes);
    for (let i = 0; i < 50; i++) auth.authenticateApiKey(rawApiKey);
    expect(auth.sessionCount).toBe(50);

    // Once a full TTL has passed, the next login purges the abandoned sessions.
    nowMs += DEFAULT_SESSION_TTL_MS + 1;
    auth.authenticateApiKey(rawApiKey);
    expect(auth.sessionCount).toBe(1);
  });
});
