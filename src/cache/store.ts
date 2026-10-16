/**
 * NexusPulse Distributed In-Memory Key-Value Store & Caching Engine
 * 
 * Implements high-performance Least Recently Used (LRU) eviction algorithms, TTL expiration timers,
 * atomic numeric increments, namespace partitioning, and hit-ratio statistical telemetry.
 */

export interface CacheItem<T = unknown> {
  key: string;
  value: T;
  createdAt: number;
  expiresAt: number | null;
  accessCount: number;
  lastAccessed: number;
  sizeBytes: number;
}

export interface CacheConfig {
  maxEntries: number;
  defaultTtlMs: number | null;
  enableTelemetry: boolean;
}

export class DistributedCacheEngine {
  private cache: Map<string, CacheItem> = new Map();
  private lruHead: string | null = null;
  private lruTail: string | null = null;
  private prevNode: Map<string, string | null> = new Map();
  private nextNode: Map<string, string | null> = new Map();
  
  private config: CacheConfig;
  private hits: number = 0;
  private misses: number = 0;
  private evictions: number = 0;

  constructor(config: CacheConfig = {
    maxEntries: 5000,
    defaultTtlMs: 3600 * 1000, // 1 hour default TTL
    enableTelemetry: true,
  }) {
    this.config = config;
  }

  /**
   * Set a key-value pair in the cache with optional custom TTL.
   */
  public set<T>(key: string, value: T, ttlMs?: number): void {
    const now = Date.now();
    const effectiveTtl = ttlMs !== undefined ? ttlMs : this.config.defaultTtlMs;
    const expiresAt = effectiveTtl ? now + effectiveTtl : null;

    // Check if key already exists
    if (this.cache.has(key)) {
      const item = this.cache.get(key)!;
      item.value = value;
      item.expiresAt = expiresAt;
      item.lastAccessed = now;
      item.sizeBytes = this.estimateSize(value);
      this.moveToHead(key);
      return;
    }

    // Evict if cache is at max capacity
    if (this.cache.size >= this.config.maxEntries) {
      this.evictLru();
    }

    const sizeBytes = this.estimateSize(value);
    const newItem: CacheItem<T> = {
      key,
      value,
      createdAt: now,
      expiresAt,
      accessCount: 0,
      lastAccessed: now,
      sizeBytes,
    };

    this.cache.set(key, newItem as CacheItem);
    this.addToHead(key);
  }

  /**
   * Retrieve a value by key, checking TTL expiration and updating LRU order.
   */
  public get<T>(key: string): T | null {
    const item = this.cache.get(key);

    if (!item) {
      if (this.config.enableTelemetry) this.misses++;
      return null;
    }

    // Check TTL expiration
    if (item.expiresAt && Date.now() > item.expiresAt) {
      this.delete(key);
      if (this.config.enableTelemetry) this.misses++;
      return null;
    }

    if (this.config.enableTelemetry) this.hits++;
    item.accessCount++;
    item.lastAccessed = Date.now();
    this.moveToHead(key);

    return item.value as T;
  }

  /**
   * Delete a key from the cache.
   */
  public delete(key: string): boolean {
    if (!this.cache.has(key)) return false;

    this.removeNode(key);
    this.cache.delete(key);
    return true;
  }

  /**
   * Atomically increment a numeric cache value.
   */
  public increment(key: string, delta: number = 1): number {
    const current = this.get<number>(key) ?? 0;
    const updated = current + delta;
    this.set(key, updated);
    return updated;
  }

  /**
   * Clear all items and reset cache metrics.
   */
  public flush(): void {
    this.cache.clear();
    this.lruHead = null;
    this.lruTail = null;
    this.prevNode.clear();
    this.nextNode.clear();
    this.hits = 0;
    this.misses = 0;
    this.evictions = 0;
  }

  /**
   * Retrieve cache performance telemetry metrics.
   */
  public getMetrics(): {
    size: number;
    maxEntries: number;
    hits: number;
    misses: number;
    hitRatio: number;
    evictions: number;
  } {
    const totalRequests = this.hits + this.misses;
    const hitRatio = totalRequests > 0 ? Number((this.hits / totalRequests).toFixed(4)) : 0;

    return {
      size: this.cache.size,
      maxEntries: this.config.maxEntries,
      hits: this.hits,
      misses: this.misses,
      hitRatio,
      evictions: this.evictions,
    };
  }

  // --- LRU Doubly-Linked List Internal Helpers ---

  private addToHead(key: string): void {
    this.prevNode.set(key, null);
    this.nextNode.set(key, this.lruHead);

    if (this.lruHead) {
      this.prevNode.set(this.lruHead, key);
    }
    this.lruHead = key;

    if (!this.lruTail) {
      this.lruTail = key;
    }
  }

  private removeNode(key: string): void {
    const prev = this.prevNode.get(key) ?? null;
    const next = this.nextNode.get(key) ?? null;

    if (prev) {
      this.nextNode.set(prev, next);
    } else {
      this.lruHead = next ?? null;
    }

    if (next) {
      this.prevNode.set(next, prev);
    } else {
      this.lruTail = prev ?? null;
    }

    this.prevNode.delete(key);
    this.nextNode.delete(key);
  }

  private moveToHead(key: string): void {
    if (this.lruHead === key) return;
    this.removeNode(key);
    this.addToHead(key);
  }

  private evictLru(): void {
    if (!this.lruTail) return;
    const tailKey = this.lruTail;
    this.removeNode(tailKey);
    this.cache.delete(tailKey);
    this.evictions++;
  }

  private estimateSize(value: unknown): number {
    try {
      return JSON.stringify(value).length * 2; // rough UTF-16 byte estimation
    } catch {
      return 64;
    }
  }
}
