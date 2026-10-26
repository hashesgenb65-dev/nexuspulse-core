/**
 * Failed-login limiting: after too many wrong secrets for one key id the key id is refused before its
 * secret is checked, the block ends when the window passes, a correct login clears the record, and the
 * table of key ids stays bounded.
 */

import { describe, it, expect } from 'vitest';
import { LoginFailureLimiter } from '../../src/auth/login_limiter';

function clock(start = 1_000): { now: () => number; advance: (ms: number) => void } {
  let t = start;
  return { now: () => t, advance: (ms) => (t += ms) };
}

describe('LoginFailureLimiter', () => {
  it('allows attempts until the failure limit, then refuses until the window passes', () => {
    const c = clock();
    const limiter = new LoginFailureLimiter({ maxFailures: 3, windowMs: 60_000, now: c.now });
    for (let i = 0; i < 3; i++) {
      expect(limiter.check('np_id_a')).toEqual({ allowed: true });
      limiter.recordFailure('np_id_a');
    }
    expect(limiter.check('np_id_a')).toEqual({ allowed: false, retryAfterMs: 60_000 });

    c.advance(20_000);
    expect(limiter.check('np_id_a')).toEqual({ allowed: false, retryAfterMs: 40_000 });

    c.advance(40_000);
    expect(limiter.check('np_id_a')).toEqual({ allowed: true });
  });

  it('counts each key id separately', () => {
    const limiter = new LoginFailureLimiter({ maxFailures: 1, windowMs: 60_000, now: clock().now });
    limiter.recordFailure('np_id_a');
    expect(limiter.check('np_id_a').allowed).toBe(false);
    expect(limiter.check('np_id_b').allowed).toBe(true);
  });

  it('forgets the failures when cleared', () => {
    const limiter = new LoginFailureLimiter({ maxFailures: 1, windowMs: 60_000, now: clock().now });
    limiter.recordFailure('np_id_a');
    limiter.clear('np_id_a');
    expect(limiter.check('np_id_a')).toEqual({ allowed: true });
  });

  it('starts a fresh window after an old one has passed, instead of adding to it', () => {
    const c = clock();
    const limiter = new LoginFailureLimiter({ maxFailures: 2, windowMs: 1_000, now: c.now });
    limiter.recordFailure('np_id_a');
    c.advance(1_000);
    limiter.recordFailure('np_id_a');
    expect(limiter.check('np_id_a')).toEqual({ allowed: true });
  });

  it('keeps no more than maxRecords key ids, dropping the oldest', () => {
    const limiter = new LoginFailureLimiter({ maxFailures: 1, windowMs: 60_000, maxRecords: 2, now: clock().now });
    limiter.recordFailure('np_id_a');
    limiter.recordFailure('np_id_b');
    limiter.recordFailure('np_id_c');
    expect(limiter.check('np_id_a').allowed).toBe(true);
    expect(limiter.check('np_id_b').allowed).toBe(false);
    expect(limiter.check('np_id_c').allowed).toBe(false);
  });

  it('rejects nonsensical options', () => {
    expect(() => new LoginFailureLimiter({ maxFailures: 0 })).toThrow(RangeError);
    expect(() => new LoginFailureLimiter({ windowMs: 0 })).toThrow(RangeError);
    expect(() => new LoginFailureLimiter({ maxRecords: 0 })).toThrow(RangeError);
  });
});
