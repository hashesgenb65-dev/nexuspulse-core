/**
 * NexusPulse Distributed Rate-Limiter & Token Bucket Cluster Syncer
 * 
 * Implements sliding token bucket rate limiting, multi-tenant tier quotas,
 * cluster synchronization states, and burst allowance policies.
 */

export interface RateLimitQuota {
  tierName: string;
  capacity: number;
  refillRatePerSec: number;
}

export interface ClientBucket {
  tokens: number;
  lastRefill: number;
  capacity: number;
  refillRate: number;
}

export class DistributedRateLimiterEngine {
  private buckets: Map<string, ClientBucket> = new Map();
  private defaultQuota: RateLimitQuota;

  constructor(defaultQuota: RateLimitQuota = { tierName: 'STANDARD', capacity: 100, refillRatePerSec: 10 }) {
    this.defaultQuota = defaultQuota;
  }

  public setQuota(clientId: string, quota: RateLimitQuota): void {
    const now = Date.now();
    this.buckets.set(clientId, {
      tokens: quota.capacity,
      lastRefill: now,
      capacity: quota.capacity,
      refillRate: quota.refillRatePerSec,
    });
  }

  public consume(clientId: string, cost: number = 1): { allowed: boolean; remainingTokens: number; resetInMs: number } {
    const now = Date.now();
    let bucket = this.buckets.get(clientId);

    if (!bucket) {
      bucket = {
        tokens: this.defaultQuota.capacity,
        lastRefill: now,
        capacity: this.defaultQuota.capacity,
        refillRate: this.defaultQuota.refillRatePerSec,
      };
      this.buckets.set(clientId, bucket);
    }

    // Refill tokens based on elapsed time
    const elapsedSec = (now - bucket.lastRefill) / 1000;
    bucket.tokens = Math.min(bucket.capacity, bucket.tokens + elapsedSec * bucket.refillRate);
    bucket.lastRefill = now;

    if (bucket.tokens >= cost) {
      bucket.tokens -= cost;
      return {
        allowed: true,
        remainingTokens: Number(bucket.tokens.toFixed(2)),
        resetInMs: 0,
      };
    }

    const deficit = cost - bucket.tokens;
    const resetInMs = Math.ceil((deficit / bucket.refillRate) * 1000);

    return {
      allowed: false,
      remainingTokens: Number(bucket.tokens.toFixed(2)),
      resetInMs,
    };
  }

  public resetClient(clientId: string): boolean {
    return this.buckets.delete(clientId);
  }
}
