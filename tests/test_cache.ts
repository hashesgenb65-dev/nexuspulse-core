import { describe, it, expect, beforeEach } from 'vitest';
import { DistributedCacheEngine } from '../src/cache/store';

describe('DistributedCacheEngine Module', () => {
  let cache: DistributedCacheEngine;

  beforeEach(() => {
    cache = new DistributedCacheEngine({
      maxEntries: 3, // small capacity for LRU eviction testing
      defaultTtlMs: 5000,
      enableTelemetry: true,
    });
  });

  it('should set and retrieve values correctly', () => {
    cache.set('user:1', { name: 'Alice', role: 'Engineer' });
    const result = cache.get<{ name: string }>('user:1');

    expect(result).not.toBeNull();
    expect(result?.name).toBe('Alice');
  });

  it('should handle cache misses and track hit/miss ratios', () => {
    cache.set('key_a', 'value_a');

    cache.get('key_a'); // Hit
    cache.get('missing_key'); // Miss

    const metrics = cache.getMetrics();
    expect(metrics.hits).toBe(1);
    expect(metrics.misses).toBe(1);
    expect(metrics.hitRatio).toBe(0.5);
  });

  it('should evict least recently used (LRU) items when capacity is exceeded', () => {
    cache.set('item_1', 'val_1');
    cache.set('item_2', 'val_2');
    cache.set('item_3', 'val_3');

    // Access item_1 and item_2, making item_3 the least recently used
    cache.get('item_1');
    cache.get('item_2');

    // Insert item_4, which should trigger eviction of item_3
    cache.set('item_4', 'val_4');

    expect(cache.get('item_3')).toBeNull(); // Evicted
    expect(cache.get('item_1')).not.toBeNull(); // Retained
    expect(cache.get('item_2')).not.toBeNull(); // Retained
    expect(cache.get('item_4')).not.toBeNull(); // Newly inserted
  });

  it('should support atomic numeric increments', () => {
    cache.increment('counter', 10);
    cache.increment('counter', 5);

    const val = cache.get<number>('counter');
    expect(val).toBe(15);
  });

  it('should expire keys exceeding their TTL window', async () => {
    cache.set('ephemeral', 'temp_data', 50); // 50ms TTL

    expect(cache.get('ephemeral')).toBe('temp_data');

    // Wait for TTL expiration
    await new Promise((resolve) => setTimeout(resolve, 80));

    expect(cache.get('ephemeral')).toBeNull();
  });
});
