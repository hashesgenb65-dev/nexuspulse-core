/**
 * Token rotation: a replaced secret stays valid for its grace period, a removed name loses access
 * at once, roles follow the new set, and an invalid reload changes nothing.
 */

import { describe, it, expect } from 'vitest';
import { AuthConfigError, principalOf, TokenAuth, type TokenSpec } from '../../src/api/auth';
import { RotatingTokenAuth, MAX_ROTATION_GRACE_MS } from '../../src/api/token_rotation';
import type { RequestContext } from '../../src/api/http_router';

const SECRET_A = 'alpha-secret-with-enough-length-1';
const SECRET_B = 'bravo-secret-with-enough-length-2';
const SECRET_C = 'charlie-secret-with-enough-length-3';
const SECRET_D = 'delta-secret-with-enough-length-4';

function spec(name: string, secret: string, role: TokenSpec['role'] = 'writer'): TokenSpec {
  return { name, secret, role };
}

function clock(start = 1_000) {
  let now = start;
  return {
    now: () => now,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

describe('initial state', () => {
  it('authenticates the configured tokens and describes them without secrets', () => {
    const auth = new RotatingTokenAuth([spec('ci', SECRET_A, 'reader'), spec('ops', SECRET_B)]);
    expect(auth.size).toBe(2);
    expect(auth.describe()).toEqual([
      { name: 'ci', role: 'reader' },
      { name: 'ops', role: 'writer' },
    ]);
    expect(auth.verifySecret(SECRET_A)).toEqual({ name: 'ci', role: 'reader' });
    expect(auth.verifySecret(SECRET_C)).toBeUndefined();
  });

  it('refuses an invalid token list', () => {
    expect(() => new RotatingTokenAuth([])).toThrow(AuthConfigError);
  });

  it('refuses a grace period that is negative, fractional or too long', () => {
    for (const graceMs of [-1, 1.5, MAX_ROTATION_GRACE_MS + 1]) {
      expect(() => new RotatingTokenAuth([spec('a', SECRET_A)], { graceMs })).toThrow(AuthConfigError);
    }
  });
});

describe('rotating a secret', () => {
  it('accepts the new secret at once and keeps the old one during the grace period', () => {
    const time = clock();
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A)], { graceMs: 10_000, now: time.now });
    auth.reload([spec('ops', SECRET_B)]);

    expect(auth.verifySecret(SECRET_B)).toEqual({ name: 'ops', role: 'writer' });
    expect(auth.verifySecret(SECRET_A)).toEqual({ name: 'ops', role: 'writer' });
  });

  it('refuses the old secret once the grace period has ended', () => {
    const time = clock();
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A)], { graceMs: 10_000, now: time.now });
    auth.reload([spec('ops', SECRET_B)]);

    time.advance(9_999);
    expect(auth.verifySecret(SECRET_A)).toBeDefined();
    time.advance(1);
    expect(auth.verifySecret(SECRET_A)).toBeUndefined();
  });

  it('refuses the old secret at once when the grace period is zero', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A)], { graceMs: 0 });
    auth.reload([spec('ops', SECRET_B)]);
    expect(auth.verifySecret(SECRET_A)).toBeUndefined();
  });

  it('applies a demotion to a retired secret at once', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A, 'writer')], { graceMs: 60_000 });
    auth.reload([spec('ops', SECRET_B, 'reader')]);
    expect(auth.verifySecret(SECRET_A)).toEqual({ name: 'ops', role: 'reader' });
  });

  it('keeps the grace period for a second rotation of the same name', () => {
    const time = clock();
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A)], { graceMs: 10_000, now: time.now });
    auth.reload([spec('ops', SECRET_B)]);
    time.advance(5_000);
    auth.reload([spec('ops', SECRET_C)]);

    expect(auth.verifySecret(SECRET_A)).toBeDefined();
    expect(auth.verifySecret(SECRET_B)).toBeDefined();
    time.advance(5_000);
    expect(auth.verifySecret(SECRET_A)).toBeUndefined();
    expect(auth.verifySecret(SECRET_B)).toBeDefined();
  });
});

describe('removing and adding names', () => {
  it('refuses a removed name immediately, even inside the grace period', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A), spec('ci', SECRET_B, 'reader')], { graceMs: 60_000 });
    auth.reload([spec('ci', SECRET_B, 'reader')]);

    expect(auth.verifySecret(SECRET_A)).toBeUndefined();
    expect(auth.verifySecret(SECRET_B)).toEqual({ name: 'ci', role: 'reader' });
  });

  it('does not revive the old secret of a removed name when the name is re-added', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A), spec('ci', SECRET_B, 'reader')], { graceMs: 60_000 });
    auth.reload([spec('ci', SECRET_B, 'reader')]);
    auth.reload([spec('ci', SECRET_B, 'reader'), spec('ops', SECRET_C)]);
    expect(auth.verifySecret(SECRET_A)).toBeUndefined();
    expect(auth.verifySecret(SECRET_C)).toEqual({ name: 'ops', role: 'writer' });
  });

  it('reports what changed in each reload', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A), spec('ci', SECRET_B, 'reader')], { graceMs: 60_000 });
    const report = auth.reload([spec('ops', SECRET_C), spec('new', SECRET_D, 'reader')]);
    expect(report).toEqual({
      added: ['new'],
      removed: ['ci'],
      rotated: ['ops'],
      retiring: 1,
    });
  });

  it('reports no rotation when the same tokens are reloaded', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A)]);
    const report = auth.reload([spec('ops', SECRET_A)]);
    expect(report).toEqual({ added: [], removed: [], rotated: [], retiring: 0 });
    expect(auth.verifySecret(SECRET_A)).toEqual({ name: 'ops', role: 'writer' });
  });
});

describe('invalid reloads', () => {
  it('keeps the current tokens when the new set is invalid', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A)], { graceMs: 60_000 });
    expect(() => auth.reload([spec('ops', SECRET_B), spec('ops', SECRET_C)])).toThrow(AuthConfigError);
    expect(auth.verifySecret(SECRET_A)).toEqual({ name: 'ops', role: 'writer' });
    expect(auth.verifySecret(SECRET_B)).toBeUndefined();
  });

  it('keeps the current tokens when the new set is empty', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A)]);
    expect(() => auth.reload([])).toThrow(AuthConfigError);
    expect(auth.size).toBe(1);
  });
});

describe('the guard and the request path', () => {
  function request(authorization: string | undefined): RequestContext {
    const headers: Record<string, string> = authorization === undefined ? {} : { authorization };
    return { req: { headers } } as unknown as RequestContext;
  }

  function statusOf(run: () => unknown): number | undefined {
    try {
      run();
      return undefined;
    } catch (error) {
      return (error as { status?: number }).status;
    }
  }

  it('answers 401 for an unknown token and 403 for a reader on a writer route', () => {
    const auth = new RotatingTokenAuth([spec('ci', SECRET_A, 'reader')]);
    const guarded = auth.guard('writer', () => undefined);

    expect(statusOf(() => guarded(request('Bearer ' + SECRET_C)))).toBe(401);
    expect(statusOf(() => guarded(request('Bearer ' + SECRET_A)))).toBe(403);
  });

  it('lets a rotated secret through the guard and records the principal', () => {
    const auth = new RotatingTokenAuth([spec('ops', SECRET_A)], { graceMs: 60_000 });
    auth.reload([spec('ops', SECRET_B)]);

    const ctx = request('Bearer ' + SECRET_B);
    const guarded = auth.guard('writer', () => 'done' as never);
    expect(guarded(ctx)).toBe('done');
    expect(principalOf(ctx)).toEqual({ name: 'ops', role: 'writer' });
  });

  it('behaves like TokenAuth for the same tokens', () => {
    const specs = [spec('ci', SECRET_A, 'reader'), spec('ops', SECRET_B)];
    const plain = new TokenAuth(specs);
    const rotating = new RotatingTokenAuth(specs);
    for (const secret of [SECRET_A, SECRET_B, SECRET_C]) {
      expect(rotating.verifySecret(secret)).toEqual(plain.verifySecret(secret));
    }
  });
});
