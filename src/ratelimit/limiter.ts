/**
 * Token-bucket rate limiter, keyed by client.
 *
 * Each client has a bucket that holds up to `capacity` tokens and refills at `refillRatePerSec`.
 * A request that costs more tokens than the bucket holds is refused, and the response says how long
 * until enough tokens have refilled.
 *
 * The number of tracked clients is bounded. When the bound is reached, the least recently used
 * bucket is dropped, so a stream of distinct client ids cannot grow memory without limit. A client
 * whose bucket was dropped starts again with a full bucket.
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

export interface RateLimiterOptions {
  /** Most clients tracked at once. Default 10 000. */
  readonly maxClients?: number;
  /** Clock in epoch milliseconds. Defaults to Date.now. */
  readonly now?: () => number;
}

export interface ConsumeResult {
  readonly allowed: boolean;
  readonly remainingTokens: number;
  readonly resetInMs: number;
}

export const DEFAULT_MAX_CLIENTS = 10_000;
const MAX_CAPACITY = 1_000_000;
const MAX_REFILL_PER_SEC = 1_000_000;
const MAX_CLIENTS_LIMIT = 1_000_000;

function assertQuota(quota: RateLimitQuota): void {
  if (!Number.isInteger(quota.capacity) || quota.capacity < 1 || quota.capacity > MAX_CAPACITY) {
    throw new RangeError(`capacity must be an integer from 1 to ${MAX_CAPACITY}`);
  }
  if (!Number.isFinite(quota.refillRatePerSec) || quota.refillRatePerSec <= 0 || quota.refillRatePerSec > MAX_REFILL_PER_SEC) {
    throw new RangeError(`refillRatePerSec must be above 0 and at most ${MAX_REFILL_PER_SEC}`);
  }
}

export class DistributedRateLimiterEngine {
  /** Insertion order is recency order: the first entry is the least recently used. */
  private buckets: Map<string, ClientBucket> = new Map();
  private readonly defaultQuota: RateLimitQuota;
  private readonly maxClients: number;
  private readonly now: () => number;

  constructor(
    defaultQuota: RateLimitQuota = { tierName: 'STANDARD', capacity: 100, refillRatePerSec: 10 },
    options: RateLimiterOptions = {},
  ) {
    assertQuota(defaultQuota);
    const maxClients = options.maxClients ?? DEFAULT_MAX_CLIENTS;
    if (!Number.isInteger(maxClients) || maxClients < 1 || maxClients > MAX_CLIENTS_LIMIT) {
      throw new RangeError(`maxClients must be an integer from 1 to ${MAX_CLIENTS_LIMIT}`);
    }
    this.defaultQuota = defaultQuota;
    this.maxClients = maxClients;
    this.now = options.now ?? Date.now;
  }

  /** Number of clients currently tracked. */
  public get trackedClients(): number {
    return this.buckets.size;
  }

  public setQuota(clientId: string, quota: RateLimitQuota): void {
    assertQuota(quota);
    this.touch(clientId, {
      tokens: quota.capacity,
      lastRefill: this.now(),
      capacity: quota.capacity,
      refillRate: quota.refillRatePerSec,
    });
  }

  /**
   * Takes `cost` tokens from the client's bucket if there are enough. A refused request takes nothing.
   *
   * @throws {RangeError} If `cost` is not a positive integer.
   */
  public consume(clientId: string, cost: number = 1): ConsumeResult {
    if (!Number.isInteger(cost) || cost < 1) {
      throw new RangeError('cost must be a positive integer');
    }

    const now = this.now();
    let bucket = this.buckets.get(clientId);
    if (bucket === undefined) {
      bucket = {
        tokens: this.defaultQuota.capacity,
        lastRefill: now,
        capacity: this.defaultQuota.capacity,
        refillRate: this.defaultQuota.refillRatePerSec,
      };
    }
    this.touch(clientId, bucket);

    // Refill tokens based on elapsed time, never above capacity.
    const elapsedSec = Math.max(0, now - bucket.lastRefill) / 1000;
    bucket.tokens = Math.min(bucket.capacity, bucket.tokens + elapsedSec * bucket.refillRate);
    bucket.lastRefill = now;

    if (bucket.tokens >= cost) {
      bucket.tokens -= cost;
      return { allowed: true, remainingTokens: Number(bucket.tokens.toFixed(2)), resetInMs: 0 };
    }

    const deficit = cost - bucket.tokens;
    const resetInMs = Math.ceil((deficit / bucket.refillRate) * 1000);
    return { allowed: false, remainingTokens: Number(bucket.tokens.toFixed(2)), resetInMs };
  }

  public resetClient(clientId: string): boolean {
    return this.buckets.delete(clientId);
  }

  /** Stores a bucket as the most recently used, evicting the least recently used if the bound is exceeded. */
  private touch(clientId: string, bucket: ClientBucket): void {
    this.buckets.delete(clientId);
    this.buckets.set(clientId, bucket);
    if (this.buckets.size > this.maxClients) {
      const oldest = this.buckets.keys().next().value;
      if (oldest !== undefined) this.buckets.delete(oldest);
    }
  }
}
