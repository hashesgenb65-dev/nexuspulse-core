/**
 * Tests for bearer-token authentication and role guards.
 */

import { describe, it, expect } from 'vitest';
import type { IncomingMessage } from 'node:http';
import {
  AuthConfigError,
  GENERATED_SECRET_LENGTH,
  generateSecret,
  MAX_SECRET_LENGTH,
  MAX_TOKENS,
  MIN_SECRET_LENGTH,
  parseBearer,
  principalOf,
  roleSatisfies,
  TokenAuth,
  validateTokenSpecs,
  type TokenSpec,
} from '../../src/api/auth.js';
import { ApiError, type ApiResponse, type RequestContext } from '../../src/api/http_router.js';

const READER_SECRET = 'reader-secret-0123456789abcdef';
const WRITER_SECRET = 'writer-secret-0123456789abcdef';

const specs: TokenSpec[] = [
  { name: 'ci-reader', secret: READER_SECRET, role: 'reader' },
  { name: 'ops-writer', secret: WRITER_SECRET, role: 'writer' },
];

/** A request context with only the fields the guard reads. */
function ctxWith(authorization?: string): RequestContext {
  const headers: Record<string, string> = {};
  if (authorization !== undefined) headers['authorization'] = authorization;
  const req = { headers } as unknown as IncomingMessage;
  return { req } as unknown as RequestContext;
}

function apiErrorOf(fn: () => unknown): ApiError {
  try {
    fn();
  } catch (err) {
    if (err instanceof ApiError) return err;
    throw err;
  }
  throw new Error('expected an ApiError');
}

describe('parseBearer', () => {
  it('extracts the token from a well-formed header', () => {
    expect(parseBearer(`Bearer ${READER_SECRET}`)).toBe(READER_SECRET);
  });

  it('accepts any casing of the scheme', () => {
    expect(parseBearer(`bearer ${READER_SECRET}`)).toBe(READER_SECRET);
    expect(parseBearer(`BEARER ${READER_SECRET}`)).toBe(READER_SECRET);
  });

  it('ignores surrounding whitespace', () => {
    expect(parseBearer(`   Bearer ${READER_SECRET}  `)).toBe(READER_SECRET);
  });

  it('returns undefined when the header is absent', () => {
    expect(parseBearer(undefined)).toBeUndefined();
  });

  it('returns undefined for other schemes', () => {
    expect(parseBearer(`Basic ${READER_SECRET}`)).toBeUndefined();
  });

  it('returns undefined when the token is missing or contains spaces', () => {
    expect(parseBearer('Bearer')).toBeUndefined();
    expect(parseBearer('Bearer ')).toBeUndefined();
    expect(parseBearer('Bearer a b')).toBeUndefined();
  });

  it('returns undefined for characters outside the token alphabet', () => {
    expect(parseBearer('Bearer abc"def')).toBeUndefined();
    expect(parseBearer('Bearer abc,def')).toBeUndefined();
  });
});

describe('validateTokenSpecs', () => {
  it('accepts a valid list', () => {
    expect(validateTokenSpecs(specs)).toEqual([]);
  });

  it('reports a non-array', () => {
    expect(validateTokenSpecs({})).toEqual([{ token: '<config>', message: 'must be an array of token specs' }]);
  });

  it('requires at least one token', () => {
    expect(validateTokenSpecs([])).toEqual([{ token: '<config>', message: 'at least one token is required' }]);
  });

  it('limits the number of tokens', () => {
    const many = Array.from({ length: MAX_TOKENS + 1 }, (_, i) => ({
      name: `t-${i}`,
      secret: `secret-${String(i).padStart(3, '0')}-0123456789abcdef`,
      role: 'reader',
    }));
    expect(validateTokenSpecs(many).map((i) => i.message)).toContain(`at most ${MAX_TOKENS} tokens are allowed`);
  });

  it('rejects names that are not lowercase slugs', () => {
    const issues = validateTokenSpecs([{ name: 'CI_Bot', secret: READER_SECRET, role: 'reader' }]);
    expect(issues).toEqual([{ token: 'CI_Bot', message: 'name must be 1-63 lowercase letters, digits or hyphens' }]);
  });

  it('rejects duplicate names', () => {
    const issues = validateTokenSpecs([
      { name: 'dup', secret: READER_SECRET, role: 'reader' },
      { name: 'dup', secret: WRITER_SECRET, role: 'writer' },
    ]);
    expect(issues).toEqual([{ token: 'dup', message: 'name is used more than once' }]);
  });

  it('rejects secrets that are too short or too long', () => {
    const short = 'x'.repeat(MIN_SECRET_LENGTH - 1);
    const long = 'x'.repeat(MAX_SECRET_LENGTH + 1);
    expect(validateTokenSpecs([{ name: 'a', secret: short, role: 'reader' }])[0]?.message).toBe(
      `secret must be ${MIN_SECRET_LENGTH}-${MAX_SECRET_LENGTH} characters`,
    );
    expect(validateTokenSpecs([{ name: 'a', secret: long, role: 'reader' }])[0]?.message).toBe(
      `secret must be ${MIN_SECRET_LENGTH}-${MAX_SECRET_LENGTH} characters`,
    );
  });

  it('rejects secrets with characters outside the allowed set', () => {
    const issues = validateTokenSpecs([{ name: 'a', secret: 'has spaces in it 0123456789', role: 'reader' }]);
    expect(issues[0]?.message).toBe('secret may only contain letters, digits and . _ ~ + / = -');
  });

  it('rejects two tokens that share a secret', () => {
    const issues = validateTokenSpecs([
      { name: 'first', secret: READER_SECRET, role: 'reader' },
      { name: 'second', secret: READER_SECRET, role: 'writer' },
    ]);
    expect(issues).toEqual([{ token: 'second', message: 'secret is the same as token first' }]);
  });

  it('rejects unknown roles', () => {
    const issues = validateTokenSpecs([{ name: 'a', secret: READER_SECRET, role: 'admin' }]);
    expect(issues[0]?.message).toBe('role must be reader or writer');
  });

  it('rejects entries that are not objects, without crashing', () => {
    expect(validateTokenSpecs([null, 'x']).map((i) => i.message)).toEqual(['must be an object', 'must be an object']);
  });

  it('reports every problem in one pass', () => {
    const issues = validateTokenSpecs([
      { name: 'Bad Name', secret: 'short', role: 'owner' },
    ]);
    expect(issues.length).toBeGreaterThanOrEqual(3);
  });
});

describe('TokenAuth construction', () => {
  it('throws AuthConfigError with every issue listed', () => {
    try {
      new TokenAuth([{ name: 'Bad', secret: 'short', role: 'owner' as never }]);
      expect.unreachable('expected AuthConfigError');
    } catch (err) {
      expect(err).toBeInstanceOf(AuthConfigError);
      const e = err as AuthConfigError;
      expect(e.code).toBe('AUTH_CONFIG');
      expect(e.issues.length).toBeGreaterThanOrEqual(3);
      expect(Object.isFrozen(e.issues)).toBe(true);
    }
  });

  it('reports its size', () => {
    expect(new TokenAuth(specs).size).toBe(2);
  });
});

describe('TokenAuth verification', () => {
  const auth = new TokenAuth(specs);

  it('returns the principal for a matching secret', () => {
    expect(auth.verifySecret(READER_SECRET)).toEqual({ name: 'ci-reader', role: 'reader' });
    expect(auth.verifySecret(WRITER_SECRET)).toEqual({ name: 'ops-writer', role: 'writer' });
  });

  it('returns undefined for an unknown or near-miss secret', () => {
    expect(auth.verifySecret('reader-secret-0123456789abcdeX')).toBeUndefined();
    expect(auth.verifySecret(READER_SECRET.slice(0, -1))).toBeUndefined();
    expect(auth.verifySecret('')).toBeUndefined();
  });

  it('authenticates a valid header', () => {
    expect(auth.authenticateHeader(`Bearer ${WRITER_SECRET}`)).toEqual({ name: 'ops-writer', role: 'writer' });
  });

  it('rejects a missing header with 401 and a challenge', () => {
    const err = apiErrorOf(() => auth.authenticateHeader(undefined));
    expect(err.status).toBe(401);
    expect(err.code).toBe('UNAUTHORIZED');
    expect(err.message).toBe('missing bearer token');
    expect(err.headers?.['www-authenticate']).toBe('Bearer realm="nexuspulse"');
  });

  it('rejects a malformed header with 401', () => {
    const err = apiErrorOf(() => auth.authenticateHeader('Basic dXNlcjpwYXNz'));
    expect(err.status).toBe(401);
    expect(err.message).toBe('malformed authorization header');
  });

  it('rejects an unknown token with 401', () => {
    const err = apiErrorOf(() => auth.authenticateHeader('Bearer not-a-real-token-0123456789'));
    expect(err.status).toBe(401);
    expect(err.message).toBe('invalid token');
  });

  it('never exposes the secret in the error', () => {
    const presented = 'Bearer not-a-real-token-0123456789';
    const err = apiErrorOf(() => auth.authenticateHeader(presented));
    expect(JSON.stringify(err.details)).not.toContain('not-a-real-token');
    expect(err.message).not.toContain('not-a-real-token');
  });
});

describe('TokenAuth.guard', () => {
  const auth = new TokenAuth(specs);
  const ok: ApiResponse = { status: 200, body: { ok: true } };

  it('runs the handler for a sufficient role and records the principal', async () => {
    const handler = auth.guard('reader', () => ok);
    const ctx = ctxWith(`Bearer ${READER_SECRET}`);
    await expect(Promise.resolve(handler(ctx))).resolves.toBe(ok);
    expect(principalOf(ctx)).toEqual({ name: 'ci-reader', role: 'reader' });
  });

  it('lets a writer through a reader route', () => {
    const handler = auth.guard('reader', () => ok);
    expect(handler(ctxWith(`Bearer ${WRITER_SECRET}`))).toBe(ok);
  });

  it('refuses a reader on a writer route with 403', () => {
    const handler = auth.guard('writer', () => ok);
    const err = apiErrorOf(() => handler(ctxWith(`Bearer ${READER_SECRET}`)));
    expect(err.status).toBe(403);
    expect(err.code).toBe('FORBIDDEN');
    expect(err.details).toEqual({ role: 'reader', required: 'writer' });
  });

  it('does not call the handler when authentication fails', () => {
    let called = false;
    const handler = auth.guard('reader', () => {
      called = true;
      return ok;
    });
    expect(apiErrorOf(() => handler(ctxWith()))).toBeInstanceOf(ApiError);
    expect(called).toBe(false);
  });

  it('does not call the handler when the role is too low', () => {
    let called = false;
    const handler = auth.guard('writer', () => {
      called = true;
      return ok;
    });
    apiErrorOf(() => handler(ctxWith(`Bearer ${READER_SECRET}`)));
    expect(called).toBe(false);
  });

  it('returns no principal for a request that never passed the guard', () => {
    expect(principalOf(ctxWith(`Bearer ${READER_SECRET}`))).toBeUndefined();
  });

  it('passes an async handler result through unchanged', async () => {
    const handler = auth.guard('reader', async () => ok);
    await expect(Promise.resolve(handler(ctxWith(`Bearer ${READER_SECRET}`)))).resolves.toBe(ok);
  });
});

describe('roleSatisfies', () => {
  it('orders writer above reader', () => {
    expect(roleSatisfies('writer', 'reader')).toBe(true);
    expect(roleSatisfies('writer', 'writer')).toBe(true);
    expect(roleSatisfies('reader', 'reader')).toBe(true);
    expect(roleSatisfies('reader', 'writer')).toBe(false);
  });
});

describe('generateSecret', () => {
  it('produces a token that passes validation', () => {
    const secret = generateSecret();
    expect(secret).toHaveLength(GENERATED_SECRET_LENGTH);
    expect(validateTokenSpecs([{ name: 'gen', secret, role: 'reader' }])).toEqual([]);
  });

  it('produces different tokens on each call', () => {
    const seen = new Set(Array.from({ length: 50 }, () => generateSecret()));
    expect(seen.size).toBe(50);
  });

  it('can be used as a working credential', () => {
    const secret = generateSecret();
    const auth = new TokenAuth([{ name: 'gen', secret, role: 'writer' }]);
    expect(auth.authenticateHeader(`Bearer ${secret}`)).toEqual({ name: 'gen', role: 'writer' });
  });
});
