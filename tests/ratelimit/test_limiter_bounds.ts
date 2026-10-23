/**
 * Bounds on the token-bucket limiter: quota and cost validation, refill that never exceeds capacity,
 * a refused request that takes nothing, and least-recently-used eviction of tracked clients.
 */

import { describe, it, expect } from 'vitest';
import { DistributedRateLimiterEngine, type RateLimitQuota } from '../../src/ratelimit/limiter';

const quota: RateLimitQuota = { tierName: 'TEST', capacity: 4, refillRatePerSec: 2 };

function clockAt(start: number): { now: () => number; advance: (ms: number) => void } {
  let t = start;
  return { now: () => t, advance: (ms) => { t += ms; } };
}

describe('quota validation', () => {
  it('rejects a capacity that is not a positive integer', () => {
    for (const capacity of [0, -1, 1.5, Number.NaN, 2_000_000]) {
      expect(() => new DistributedRateLimiterEngine({ ...quota, capacity })).toThrow(RangeError);
    }
  });

  it('rejects a refill rate that is zero, negative, infinite or NaN', () => {
    for (const refillRatePerSec of [0, -1, Number.POSITIVE_INFINITY, Number.NaN]) {
      expect(() => new DistributedRateLimiterEngine({ ...quota, refillRatePerSec })).toThrow(RangeError);
    }
  });

  it('validates quotas passed to setQuota too', () => {
    const limiter = new DistributedRateLimiterEngine(quota);
    expect(() => limiter.setQuota('x', { ...quota, capacity: 0 })).toThrow(RangeError);
  });

  it('rejects a cost that is not a positive integer', () => {
    const limiter = new DistributedRateLimiterEngine(quota);
    for (const cost of [0, -1, 1.5, Number.NaN]) {
      expect(() => limiter.consume('c', cost)).toThrow(RangeError);
    }
  });

  it('rejects a maxClients outside the accepted range', () => {
    expect(() => new DistributedRateLimiterEngine(quota, { maxClients: 0 })).toThrow(RangeError);
    expect(() => new DistributedRateLimiterEngine(quota, { maxClients: 1.5 })).toThrow(RangeError);
  });
});

describe('refill and refusal', () => {
  it('refills at the configured rate, and never above capacity', () => {
    const clock = clockAt(1_000);
    const limiter = new DistributedRateLimiterEngine(quota, { now: clock.now });
    expect(limiter.consume('c', 4).allowed).toBe(true);
    expect(limiter.consume('c', 1).allowed).toBe(false);

    clock.advance(500); // 2 tokens per second, so one token refills
    expect(limiter.consume('c', 1).allowed).toBe(true);

    clock.advance(60_000); // far longer than needed to refill
    const result = limiter.consume('c', 1);
    expect(result.allowed).toBe(true);
    expect(result.remainingTokens).toBe(3);
  });

  it('reports how long until a refused request would fit', () => {
    const clock = clockAt(0);
    const limiter = new DistributedRateLimiterEngine(quota, { now: clock.now });
    limiter.consume('c', 4);
    const refused = limiter.consume('c', 2);
    expect(refused.allowed).toBe(false);
    expect(refused.resetInMs).toBe(1_000);
  });

  it('does not take tokens for a refused request', () => {
    const clock = clockAt(0);
    const limiter = new DistributedRateLimiterEngine(quota, { now: clock.now });
    limiter.consume('c', 3);
    expect(limiter.consume('c', 2).allowed).toBe(false);
    expect(limiter.consume('c', 1).allowed).toBe(true);
  });
});

describe('bounded client tracking', () => {
  it('never tracks more clients than maxClients', () => {
    const limiter = new DistributedRateLimiterEngine(quota, { maxClients: 3 });
    for (let i = 0; i < 50; i++) limiter.consume(`client-${i}`, 1);
    expect(limiter.trackedClients).toBe(3);
  });

  it('evicts the least recently used client, not the most recent', () => {
    const limiter = new DistributedRateLimiterEngine(quota, { maxClients: 2 });
    limiter.consume('a', 4); // a is empty
    limiter.consume('b', 1);
    limiter.consume('a', 1); // a is used again, so b is now the least recently used
    limiter.consume('c', 1); // evicts b
    expect(limiter.trackedClients).toBe(2);
    // b starts again with a full bucket.
    expect(limiter.consume('b', 4).allowed).toBe(true);
  });

  it('restarts an evicted client with a full bucket', () => {
    const limiter = new DistributedRateLimiterEngine(quota, { maxClients: 1 });
    limiter.consume('a', 4);
    expect(limiter.consume('a', 1).allowed).toBe(false);
    limiter.consume('b', 1); // evicts a
    expect(limiter.consume('a', 4).allowed).toBe(true);
  });

  it('resetClient removes a client and frees its place', () => {
    const limiter = new DistributedRateLimiterEngine(quota, { maxClients: 2 });
    limiter.consume('a', 1);
    limiter.consume('b', 1);
    expect(limiter.resetClient('a')).toBe(true);
    expect(limiter.trackedClients).toBe(1);
    expect(limiter.resetClient('a')).toBe(false);
  });
});
