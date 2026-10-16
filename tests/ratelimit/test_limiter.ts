import { describe, it, expect, beforeEach } from 'vitest';
import { DistributedRateLimiterEngine } from '../../src/ratelimit/limiter';

describe('DistributedRateLimiterEngine Module', () => {
  let limiter: DistributedRateLimiterEngine;

  beforeEach(() => {
    limiter = new DistributedRateLimiterEngine({ tierName: 'BASIC', capacity: 5, refillRatePerSec: 2 });
  });

  it('should allow requests within capacity limits', () => {
    const res1 = limiter.consume('client_1', 2);
    expect(res1.allowed).toBe(true);
    expect(res1.remainingTokens).toBe(3);
  });

  it('should block requests exceeding bucket capacity and calculate reset window', () => {
    const res1 = limiter.consume('client_2', 10); // Exceeds capacity of 5
    expect(res1.allowed).toBe(false);
    expect(res1.resetInMs).toBeGreaterThan(0);
  });

  it('should apply custom tier quotas per client', () => {
    limiter.setQuota('vip_client', { tierName: 'VIP', capacity: 20, refillRatePerSec: 5 });

    const res = limiter.consume('vip_client', 15);
    expect(res.allowed).toBe(true);
    expect(res.remainingTokens).toBe(5);
  });
});
