/**
 * @file tests/analytics/test_analytics.ts
 * @description Vitest suite for /src/analytics/metrics_engine.ts.
 *
 * Strategy:
 * - Accuracy of the t-digest is measured against exact quantiles computed by
 *   sorting the same data. Input comes from a seeded PRNG so the suite is
 *   deterministic and never flaky.
 * - Sliding windows use a manually advanced fake clock, so expiry and slot
 *   rotation are checked at exact millisecond boundaries.
 * - Every validation branch and every error code is exercised.
 */

import { describe, it, expect } from 'vitest';
import {
  TDigest,
  SlidingWindowHistogram,
  CounterRegistry,
  MetricsEngine,
  downsample,
  lttb,
  MetricsError,
  InvalidMetricError,
  CardinalityError,
  DEFAULT_LATENCY_BOUNDARIES_MS,
  type Point,
} from '../../src/analytics/metrics_engine.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Deterministic 32-bit PRNG (mulberry32). Same seed, same sequence, every run. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Exact quantile by linear interpolation over a sorted array. */
function exactQuantile(sorted: readonly number[], q: number): number {
  const pos = q * (sorted.length - 1);
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  const frac = pos - lo;
  return sorted[lo]! + frac * (sorted[hi]! - sorted[lo]!);
}

/** A manually advanced clock for window tests. */
function fakeClock(start = 0): { now: () => number; set: (t: number) => void } {
  let t = start;
  return {
    now: () => t,
    set: (next: number) => {
      t = next;
    },
  };
}

// ---------------------------------------------------------------------------
// TDigest — basics
// ---------------------------------------------------------------------------

describe('TDigest basics', () => {
  it('is empty by default', () => {
    const d = new TDigest();
    expect(d.count).toBe(0);
    expect(d.min).toBeNull();
    expect(d.max).toBeNull();
    expect(d.mean).toBeNull();
    expect(d.quantile(0.5)).toBeNull();
    expect(d.percentiles()).toEqual({ p50: null, p90: null, p95: null, p99: null, p999: null });
  });

  it('returns the single value for every quantile', () => {
    const d = new TDigest();
    d.add(42);
    for (const q of [0, 0.25, 0.5, 0.99, 1]) {
      expect(d.quantile(q)).toBe(42);
    }
    expect(d.mean).toBe(42);
    expect(d.count).toBe(1);
  });

  it('tracks exact min and max, and returns them at q = 0 and q = 1', () => {
    const d = new TDigest();
    for (const v of [5, -3, 17, 2.5, 100]) d.add(v);
    expect(d.min).toBe(-3);
    expect(d.max).toBe(100);
    expect(d.quantile(0)).toBe(-3);
    expect(d.quantile(1)).toBe(100);
  });

  it('computes the weighted mean exactly for small inputs', () => {
    const d = new TDigest();
    d.add(10, 3);
    d.add(20, 1);
    expect(d.count).toBe(4);
    expect(d.mean).toBeCloseTo(12.5, 12);
  });

  it('rejects non-finite values', () => {
    const d = new TDigest();
    expect(() => d.add(Number.NaN)).toThrow(InvalidMetricError);
    expect(() => d.add(Number.POSITIVE_INFINITY)).toThrow(InvalidMetricError);
    expect(() => d.add(Number.NEGATIVE_INFINITY)).toThrow(InvalidMetricError);
    expect(d.count).toBe(0);
  });

  it('rejects non-positive or non-finite weights', () => {
    const d = new TDigest();
    expect(() => d.add(1, 0)).toThrow(InvalidMetricError);
    expect(() => d.add(1, -1)).toThrow(InvalidMetricError);
    expect(() => d.add(1, Number.NaN)).toThrow(InvalidMetricError);
  });

  it.each([-0.01, 1.01, Number.NaN])('rejects quantile %s outside [0, 1]', (q) => {
    const d = new TDigest();
    d.add(1);
    expect(() => d.quantile(q)).toThrow(InvalidMetricError);
  });

  it.each([19, 1001, 50.5])('rejects compression %s', (c) => {
    expect(() => new TDigest(c)).toThrow(InvalidMetricError);
  });

  it('quantile estimates never fall outside the observed [min, max]', () => {
    const d = new TDigest();
    const rnd = mulberry32(3);
    for (let i = 0; i < 5000; i++) d.add(rnd() * 10 + 100);
    for (let q = 0; q <= 1; q += 0.01) {
      const v = d.quantile(q)!;
      expect(v).toBeGreaterThanOrEqual(d.min!);
      expect(v).toBeLessThanOrEqual(d.max!);
    }
  });

  it('quantile estimates are monotonically non-decreasing in q', () => {
    const d = new TDigest();
    const rnd = mulberry32(11);
    for (let i = 0; i < 20_000; i++) d.add(-Math.log(1 - rnd()) * 50);
    let prev = -Infinity;
    for (let q = 0; q <= 1; q += 0.005) {
      const v = d.quantile(q)!;
      expect(v).toBeGreaterThanOrEqual(prev);
      prev = v;
    }
  });
});

// ---------------------------------------------------------------------------
// TDigest — accuracy
// ---------------------------------------------------------------------------

describe('TDigest accuracy against exact quantiles', () => {
  const N = 100_000;

  function uniformData(seed: number): number[] {
    const rnd = mulberry32(seed);
    return Array.from({ length: N }, () => rnd() * 1000);
  }

  it('estimates p50, p90, p95, p99 and p99.9 of uniform data within tight error', () => {
    const data = uniformData(7);
    const d = new TDigest();
    for (const v of data) d.add(v);
    const sorted = [...data].sort((a, b) => a - b);
    const range = 1000;

    const cases: Array<[number, number]> = [
      [0.5, 0.01],     // 1% of range at the median
      [0.9, 0.005],
      [0.95, 0.004],
      [0.99, 0.002],   // tails tighten
      [0.999, 0.001],
    ];
    for (const [q, tol] of cases) {
      const est = d.quantile(q)!;
      const exact = exactQuantile(sorted, q);
      expect(Math.abs(est - exact), `q=${q} est=${est} exact=${exact}`).toBeLessThanOrEqual(tol * range);
    }
  });

  it('stays accurate when the input arrives in sorted order (adversarial order)', () => {
    const data = uniformData(19).sort((a, b) => a - b);
    const d = new TDigest();
    for (const v of data) d.add(v);
    const exact99 = exactQuantile(data, 0.99);
    expect(Math.abs(d.quantile(0.99)! - exact99)).toBeLessThanOrEqual(0.002 * 1000);
  });

  it('stays accurate on a heavy-tailed (exponential) latency-like distribution', () => {
    const rnd = mulberry32(23);
    const data = Array.from({ length: N }, () => -Math.log(1 - rnd()) * 20);
    const d = new TDigest();
    for (const v of data) d.add(v);
    const sorted = [...data].sort((a, b) => a - b);
    const exact99 = exactQuantile(sorted, 0.99);
    // Relative error on the tail: within 3% of the exact p99.
    expect(Math.abs(d.quantile(0.99)! - exact99) / exact99).toBeLessThanOrEqual(0.03);
  });

  it('keeps centroid count bounded regardless of sample count', () => {
    const d = new TDigest(100);
    const rnd = mulberry32(31);
    for (let i = 0; i < N; i++) d.add(rnd());
    expect(d.centroidCount).toBeLessThanOrEqual(2 * 100);
    expect(d.count).toBe(N);
  });

  it('reports a mean matching the exact arithmetic mean', () => {
    const data = uniformData(41);
    const d = new TDigest();
    for (const v of data) d.add(v);
    const exactMean = data.reduce((s, v) => s + v, 0) / data.length;
    expect(d.mean!).toBeCloseTo(exactMean, 6);
  });
});

// ---------------------------------------------------------------------------
// TDigest — merge
// ---------------------------------------------------------------------------

describe('TDigest merge', () => {
  it('merging two halves approximates a single digest of the whole', () => {
    const rnd = mulberry32(53);
    const data = Array.from({ length: 40_000 }, () => rnd() * 500);
    const whole = new TDigest();
    const left = new TDigest();
    const right = new TDigest();
    data.forEach((v, i) => {
      whole.add(v);
      (i % 2 === 0 ? left : right).add(v);
    });
    left.merge(right);
    expect(left.count).toBe(whole.count);
    expect(left.min).toBe(whole.min);
    expect(left.max).toBe(whole.max);
    for (const q of [0.5, 0.9, 0.99]) {
      expect(Math.abs(left.quantile(q)! - whole.quantile(q)!)).toBeLessThanOrEqual(0.01 * 500);
    }
  });

  it('does not modify the digest being merged in', () => {
    const a = new TDigest();
    const b = new TDigest();
    for (let i = 1; i <= 100; i++) b.add(i);
    const before = b.count;
    a.merge(b);
    expect(b.count).toBe(before);
    expect(b.quantile(0.5)).toBeCloseTo(50.5, 0);
  });

  it('merging an empty digest is a no-op', () => {
    const a = new TDigest();
    a.add(7);
    a.merge(new TDigest());
    expect(a.count).toBe(1);
    expect(a.quantile(0.5)).toBe(7);
  });

  it('merging into an empty digest copies the source', () => {
    const src = new TDigest();
    for (let i = 1; i <= 10; i++) src.add(i);
    const dst = new TDigest();
    dst.merge(src);
    expect(dst.count).toBe(10);
    expect(dst.min).toBe(1);
    expect(dst.max).toBe(10);
  });
});

// ---------------------------------------------------------------------------
// SlidingWindowHistogram — configuration and recording
// ---------------------------------------------------------------------------

describe('SlidingWindowHistogram configuration', () => {
  it('uses default latency boundaries', () => {
    const h = new SlidingWindowHistogram({ clock: fakeClock().now });
    const s = h.snapshot();
    expect(s.buckets.map((b) => b.le)).toEqual([...DEFAULT_LATENCY_BOUNDARIES_MS, Number.POSITIVE_INFINITY]);
  });

  it('requires windowMs divisible by slices', () => {
    expect(() => new SlidingWindowHistogram({ windowMs: 1000, slices: 7 })).toThrow(InvalidMetricError);
  });

  it('rejects empty, unsorted, duplicate, and negative boundaries', () => {
    expect(() => new SlidingWindowHistogram({ boundaries: [] })).toThrow(InvalidMetricError);
    expect(() => new SlidingWindowHistogram({ boundaries: [10, 5] })).toThrow(InvalidMetricError);
    expect(() => new SlidingWindowHistogram({ boundaries: [10, 10] })).toThrow(InvalidMetricError);
    expect(() => new SlidingWindowHistogram({ boundaries: [-1, 5] })).toThrow(InvalidMetricError);
  });

  it.each([
    { windowMs: 0 },
    { slices: 0 },
    { slices: 257 },
    { compression: 5 },
  ])('rejects out-of-range option %o', (opts) => {
    expect(() => new SlidingWindowHistogram(opts)).toThrow(InvalidMetricError);
  });

  it('rejects negative and non-finite observations', () => {
    const h = new SlidingWindowHistogram({ clock: fakeClock().now });
    expect(() => h.record(-1)).toThrow(InvalidMetricError);
    expect(() => h.record(Number.NaN)).toThrow(InvalidMetricError);
    expect(() => h.record(Number.POSITIVE_INFINITY)).toThrow(InvalidMetricError);
    expect(h.snapshot().count).toBe(0);
  });

  it('accepts zero as an observation', () => {
    const h = new SlidingWindowHistogram({ clock: fakeClock().now });
    h.record(0);
    expect(h.snapshot().count).toBe(1);
    expect(h.snapshot().min).toBe(0);
  });
});

describe('SlidingWindowHistogram summary', () => {
  it('reports null statistics when empty', () => {
    const h = new SlidingWindowHistogram({ clock: fakeClock().now });
    const s = h.snapshot();
    expect(s.count).toBe(0);
    expect(s.sum).toBe(0);
    expect(s.mean).toBeNull();
    expect(s.min).toBeNull();
    expect(s.max).toBeNull();
    expect(s.p50).toBeNull();
    expect(s.p999).toBeNull();
  });

  it('reports exact count, sum, mean, min and max', () => {
    const h = new SlidingWindowHistogram({ clock: fakeClock().now });
    for (const v of [4, 8, 15, 16, 23, 42]) h.record(v);
    const s = h.snapshot();
    expect(s.count).toBe(6);
    expect(s.sum).toBe(108);
    expect(s.mean).toBe(18);
    expect(s.min).toBe(4);
    expect(s.max).toBe(42);
  });

  it('computes percentiles that fall between min and max', () => {
    const h = new SlidingWindowHistogram({ clock: fakeClock().now });
    for (let i = 1; i <= 1000; i++) h.record(i);
    const s = h.snapshot();
    expect(s.p50!).toBeGreaterThan(s.min!);
    expect(s.p999!).toBeLessThanOrEqual(s.max!);
    expect(Math.abs(s.p50! - 500)).toBeLessThanOrEqual(10);
    expect(Math.abs(s.p99! - 990)).toBeLessThanOrEqual(10);
  });

  it('exposes the window length it was configured with', () => {
    const h = new SlidingWindowHistogram({ windowMs: 5000, slices: 5, clock: fakeClock().now });
    expect(h.snapshot().windowMs).toBe(5000);
  });
});

describe('SlidingWindowHistogram buckets', () => {
  it('produces cumulative counts ending at the total count', () => {
    const h = new SlidingWindowHistogram({ boundaries: [10, 100], clock: fakeClock().now });
    h.record(5);
    h.record(50);
    h.record(500);
    expect(h.snapshot().buckets).toEqual([
      { le: 10, count: 1 },
      { le: 100, count: 2 },
      { le: Number.POSITIVE_INFINITY, count: 3 },
    ]);
  });

  it('places a value equal to a boundary in that boundary (inclusive upper bound)', () => {
    const h = new SlidingWindowHistogram({ boundaries: [10, 100], clock: fakeClock().now });
    h.record(10);
    h.record(100);
    h.record(100.0001);
    const b = h.snapshot().buckets;
    expect(b[0]!.count).toBe(1);
    expect(b[1]!.count).toBe(2);
    expect(b[2]!.count).toBe(3);
  });

  it('counts a value above every boundary only in the overflow bucket', () => {
    const h = new SlidingWindowHistogram({ boundaries: [10], clock: fakeClock().now });
    h.record(1e9);
    const b = h.snapshot().buckets;
    expect(b[0]!.count).toBe(0);
    expect(b[1]!.count).toBe(1);
  });

  it('final bucket always equals the total count', () => {
    const h = new SlidingWindowHistogram({ clock: fakeClock().now });
    const rnd = mulberry32(61);
    for (let i = 0; i < 500; i++) h.record(rnd() * 20_000);
    const s = h.snapshot();
    expect(s.buckets[s.buckets.length - 1]!.count).toBe(s.count);
  });
});

describe('SlidingWindowHistogram windowing (fake clock)', () => {
  it('counts samples recorded inside the window', () => {
    const clock = fakeClock(0);
    const h = new SlidingWindowHistogram({ windowMs: 1000, slices: 10, clock: clock.now });
    h.record(1);
    clock.set(500);
    h.record(2);
    expect(h.snapshot().count).toBe(2);
  });

  it('expires samples once their slice falls out of the window', () => {
    const clock = fakeClock(0);
    const h = new SlidingWindowHistogram({ windowMs: 1000, slices: 10, clock: clock.now });
    h.record(1); // slice 0
    clock.set(900);
    expect(h.snapshot().count).toBe(1); // slice 0 still live
    clock.set(1000);
    expect(h.snapshot().count).toBe(0); // slice 0 has aged out
  });

  it('keeps newer samples while older ones expire', () => {
    const clock = fakeClock(0);
    const h = new SlidingWindowHistogram({ windowMs: 1000, slices: 10, clock: clock.now });
    h.record(1); // slice 0
    clock.set(600);
    h.record(2); // slice 6
    clock.set(1050); // slice 10: slice 0 gone, slice 6 still live
    const s = h.snapshot();
    expect(s.count).toBe(1);
    expect(s.min).toBe(2);
  });

  it('reuses ring slots correctly after many full rotations', () => {
    const clock = fakeClock(0);
    const h = new SlidingWindowHistogram({ windowMs: 1000, slices: 4, clock: clock.now });
    for (let cycle = 0; cycle < 50; cycle++) {
      clock.set(cycle * 1000 + 10);
      h.record(cycle);
    }
    const s = h.snapshot();
    expect(s.count).toBe(1);
    expect(s.min).toBe(49);
  });

  it('a fresh slot at a reused index does not inherit old counts', () => {
    const clock = fakeClock(0);
    const h = new SlidingWindowHistogram({ windowMs: 1000, slices: 10, clock: clock.now });
    h.record(5);
    clock.set(1000); // same ring index as slice 0, new slice id
    h.record(7);
    const s = h.snapshot();
    expect(s.count).toBe(1);
    expect(s.sum).toBe(7);
  });

  it('rejects a clock that moves backwards past a live slice', () => {
    const clock = fakeClock(0);
    const h = new SlidingWindowHistogram({ windowMs: 1000, slices: 10, clock: clock.now });
    clock.set(1000); // slice 10 -> ring index 0
    h.record(1);
    clock.set(0); // slice 0 -> same ring index, older id
    expect(() => h.record(2)).toThrow(InvalidMetricError);
  });
});

// ---------------------------------------------------------------------------
// CounterRegistry
// ---------------------------------------------------------------------------

describe('CounterRegistry', () => {
  it('accumulates increments and returns the running value', () => {
    const c = new CounterRegistry();
    expect(c.increment('requests')).toBe(1);
    expect(c.increment('requests', 4)).toBe(5);
    expect(c.value('requests')).toBe(5);
  });

  it('returns 0 for a series that was never incremented', () => {
    expect(new CounterRegistry().value('absent')).toBe(0);
  });

  it('canonicalises label order so {a,b} and {b,a} are the same series', () => {
    const c = new CounterRegistry();
    c.increment('hits', 1, { route: '/a', method: 'GET' });
    c.increment('hits', 1, { method: 'GET', route: '/a' });
    expect(c.value('hits', { route: '/a', method: 'GET' })).toBe(2);
    expect(c.seriesCount).toBe(1);
  });

  it('keeps distinct label sets as distinct series', () => {
    const c = new CounterRegistry();
    c.increment('hits', 1, { route: '/a' });
    c.increment('hits', 3, { route: '/b' });
    expect(c.value('hits', { route: '/a' })).toBe(1);
    expect(c.value('hits', { route: '/b' })).toBe(3);
    expect(c.value('hits')).toBe(0); // reading an absent series does not create it
    expect(c.seriesCount).toBe(2);
  });

  it('treats an empty label object as unlabelled', () => {
    const c = new CounterRegistry();
    c.increment('x', 1, {});
    expect(c.value('x')).toBe(1);
  });

  it('accepts zero delta', () => {
    const c = new CounterRegistry();
    expect(c.increment('z', 0)).toBe(0);
  });

  it('rejects negative, non-finite, and non-numeric deltas (counters are monotonic)', () => {
    const c = new CounterRegistry();
    expect(() => c.increment('x', -1)).toThrow(InvalidMetricError);
    expect(() => c.increment('x', Number.NaN)).toThrow(InvalidMetricError);
    expect(() => c.increment('x', Number.POSITIVE_INFINITY)).toThrow(InvalidMetricError);
    expect(c.value('x')).toBe(0);
  });

  it.each(['', '1leading-digit', 'has space', 'x'.repeat(129)])('rejects invalid metric name %j', (name) => {
    expect(() => new CounterRegistry().increment(name)).toThrow(InvalidMetricError);
  });

  it('rejects invalid label keys', () => {
    const c = new CounterRegistry();
    expect(() => c.increment('x', 1, { 'bad-key': 'v' })).toThrow(InvalidMetricError);
    expect(() => c.increment('x', 1, { '1abc': 'v' })).toThrow(InvalidMetricError);
  });

  it('rejects over-long label values', () => {
    expect(() => new CounterRegistry().increment('x', 1, { k: 'v'.repeat(129) })).toThrow(InvalidMetricError);
  });

  it('rejects more than 16 labels', () => {
    const labels: Record<string, string> = {};
    for (let i = 0; i < 17; i++) labels[`k${i}`] = 'v';
    expect(() => new CounterRegistry().increment('x', 1, labels)).toThrow(InvalidMetricError);
  });

  it('enforces the series cardinality limit but keeps existing series writable', () => {
    const c = new CounterRegistry(2);
    c.increment('a');
    c.increment('b');
    expect(() => c.increment('c')).toThrow(CardinalityError);
    expect(c.increment('a', 4)).toBe(5);
    expect(c.seriesCount).toBe(2);
  });

  it('snapshot is sorted by name then labels and carries values', () => {
    const c = new CounterRegistry();
    c.increment('zeta');
    c.increment('alpha', 2, { k: 'b' });
    c.increment('alpha', 1, { k: 'a' });
    const snap = c.snapshot();
    expect(snap.map((s) => `${s.name}:${JSON.stringify(s.labels)}=${s.value}`)).toEqual([
      'alpha:{"k":"a"}=1',
      'alpha:{"k":"b"}=2',
      'zeta:{}=1',
    ]);
  });

  it('CardinalityError and InvalidMetricError are MetricsError subclasses with codes', () => {
    const c = new CounterRegistry(1);
    c.increment('a');
    const cardErr = (() => {
      try {
        c.increment('b');
      } catch (e) {
        return e;
      }
      return undefined;
    })();
    expect(cardErr).toBeInstanceOf(MetricsError);
    expect((cardErr as MetricsError).code).toBe('CARDINALITY_LIMIT');
    expect(() => c.increment('b', -1)).toThrow(InvalidMetricError);
  });
});

// ---------------------------------------------------------------------------
// Downsampling — downsample()
// ---------------------------------------------------------------------------

const series: Point[] = [
  { t: 0, v: 1 },
  { t: 3, v: 5 },
  { t: 7, v: 3 },
  { t: 12, v: 10 },
  { t: 18, v: 20 },
  { t: 40, v: 7 },
];

describe('downsample', () => {
  it('avg aggregates each bucket and stamps it at the bucket start', () => {
    expect(downsample(series, { bucketMs: 10, aggregation: 'avg' })).toEqual([
      { t: 0, v: 3 },   // (1+5+3)/3
      { t: 10, v: 15 }, // (10+20)/2
      { t: 40, v: 7 },
    ]);
  });

  it('min and max pick extremes per bucket', () => {
    expect(downsample(series, { bucketMs: 10, aggregation: 'min' })).toEqual([
      { t: 0, v: 1 },
      { t: 10, v: 10 },
      { t: 40, v: 7 },
    ]);
    expect(downsample(series, { bucketMs: 10, aggregation: 'max' })).toEqual([
      { t: 0, v: 5 },
      { t: 10, v: 20 },
      { t: 40, v: 7 },
    ]);
  });

  it('sum and count aggregate per bucket', () => {
    expect(downsample(series, { bucketMs: 10, aggregation: 'sum' })[0]).toEqual({ t: 0, v: 9 });
    expect(downsample(series, { bucketMs: 10, aggregation: 'count' })).toEqual([
      { t: 0, v: 3 },
      { t: 10, v: 2 },
      { t: 40, v: 1 },
    ]);
  });

  it('last takes the final value in time order within each bucket', () => {
    expect(downsample(series, { bucketMs: 10, aggregation: 'last' })).toEqual([
      { t: 0, v: 3 },
      { t: 10, v: 20 },
      { t: 40, v: 7 },
    ]);
  });

  it('omits empty buckets rather than emitting gaps', () => {
    const out = downsample(series, { bucketMs: 10, aggregation: 'count' });
    expect(out.map((p) => p.t)).toEqual([0, 10, 40]);
  });

  it('returns an empty array for empty input', () => {
    expect(downsample([], { bucketMs: 10, aggregation: 'avg' })).toEqual([]);
  });

  it('a bucket larger than the whole series yields a single point', () => {
    const out = downsample(series, { bucketMs: 1000, aggregation: 'max' });
    expect(out).toEqual([{ t: 0, v: 20 }]);
  });

  it('floors negative timestamps to the correct bucket', () => {
    const out = downsample(
      [
        { t: -15, v: 1 },
        { t: -12, v: 3 },
        { t: -1, v: 5 },
      ],
      { bucketMs: 10, aggregation: 'sum' },
    );
    // -15 and -12 both floor to the -20 bucket (1 + 3); -1 floors to -10 (5).
    expect(out).toEqual([
      { t: -20, v: 4 },
      { t: -10, v: 5 },
    ]);
  });

  it('rejects an unsorted series', () => {
    expect(() =>
      downsample([{ t: 5, v: 1 }, { t: 2, v: 2 }], { bucketMs: 10, aggregation: 'avg' }),
    ).toThrow(InvalidMetricError);
  });

  it.each([0, -5, 1.5])('rejects bucketMs %s', (bucketMs) => {
    expect(() => downsample(series, { bucketMs, aggregation: 'avg' })).toThrow(InvalidMetricError);
  });

  it('rejects non-finite point values and timestamps', () => {
    expect(() => downsample([{ t: 0, v: Number.NaN }], { bucketMs: 1, aggregation: 'avg' })).toThrow(
      InvalidMetricError,
    );
    expect(() => downsample([{ t: Number.NaN, v: 1 }], { bucketMs: 1, aggregation: 'avg' })).toThrow(
      InvalidMetricError,
    );
  });

  it('does not mutate its input', () => {
    const copy = series.map((p) => ({ ...p }));
    downsample(series, { bucketMs: 10, aggregation: 'avg' });
    expect(series).toEqual(copy);
  });
});

// ---------------------------------------------------------------------------
// Downsampling — lttb()
// ---------------------------------------------------------------------------

describe('lttb', () => {
  const flatWithSpike = (n: number, spikeAt: number): Point[] =>
    Array.from({ length: n }, (_, i) => ({ t: i, v: i === spikeAt ? 1000 : 1 }));

  it('returns a copy when the series is already within the threshold', () => {
    const pts = flatWithSpike(10, 5);
    const out = lttb(pts, 50);
    expect(out).toEqual(pts);
    expect(out).not.toBe(pts);
  });

  it('returns exactly `threshold` points for a longer series', () => {
    const pts = flatWithSpike(10_000, 4321);
    expect(lttb(pts, 200)).toHaveLength(200);
  });

  it('always keeps the first and last points', () => {
    const pts = flatWithSpike(1000, 500);
    const out = lttb(pts, 50);
    expect(out[0]).toEqual(pts[0]);
    expect(out[out.length - 1]).toEqual(pts[pts.length - 1]);
  });

  it('preserves a sharp spike that plain decimation would drop', () => {
    const pts = flatWithSpike(1000, 503);
    const out = lttb(pts, 40);
    expect(out.some((p) => p.v === 1000)).toBe(true);
  });

  it('keeps timestamps strictly increasing', () => {
    const rnd = mulberry32(71);
    const pts = Array.from({ length: 5000 }, (_, i) => ({ t: i * 10, v: rnd() * 100 }));
    const out = lttb(pts, 300);
    for (let i = 1; i < out.length; i++) {
      expect(out[i]!.t).toBeGreaterThan(out[i - 1]!.t);
    }
  });

  it('every output point is one of the input points', () => {
    const pts = flatWithSpike(500, 250);
    const set = new Set(pts);
    for (const p of lttb(pts, 30)) expect(set.has(p)).toBe(true);
  });

  it('rejects a threshold below 3 or non-integer', () => {
    const pts = flatWithSpike(100, 50);
    expect(() => lttb(pts, 2)).toThrow(InvalidMetricError);
    expect(() => lttb(pts, 10.5)).toThrow(InvalidMetricError);
  });
});

// ---------------------------------------------------------------------------
// MetricsEngine facade
// ---------------------------------------------------------------------------

describe('MetricsEngine', () => {
  it('creates histograms lazily on first observation', () => {
    const m = new MetricsEngine({ clock: fakeClock().now });
    expect(m.summary('lat')).toBeUndefined();
    m.observe('lat', 12);
    expect(m.summary('lat')!.count).toBe(1);
    expect(m.histogramNames()).toEqual(['lat']);
  });

  it('routes counters through the registry', () => {
    const m = new MetricsEngine();
    m.increment('req', 2, { route: '/x' });
    expect(m.counter('req', { route: '/x' })).toBe(2);
  });

  it('snapshot includes every histogram and counter', () => {
    const m = new MetricsEngine({ clock: fakeClock().now });
    m.observe('b.lat', 1);
    m.observe('a.lat', 2);
    m.increment('c', 3);
    const snap = m.snapshot();
    expect(Object.keys(snap.histograms)).toEqual(['a.lat', 'b.lat']);
    expect(snap.histograms['a.lat']!.count).toBe(1);
    expect(snap.counters).toEqual([{ name: 'c', labels: {}, value: 3 }]);
  });

  it('enforces the histogram series limit', () => {
    const m = new MetricsEngine({ maxSeries: 2, clock: fakeClock().now });
    m.observe('one', 1);
    m.observe('two', 1);
    expect(() => m.observe('three', 1)).toThrow(CardinalityError);
    m.observe('one', 2); // existing series still accepts data
    expect(m.summary('one')!.count).toBe(2);
  });

  it('validates the histogram configuration at construction', () => {
    expect(() => new MetricsEngine({ windowMs: 1000, slices: 3 })).toThrow(InvalidMetricError);
  });

  it('rejects invalid histogram names', () => {
    const m = new MetricsEngine();
    expect(() => m.observe('bad name', 1)).toThrow(InvalidMetricError);
  });

  it('honours a custom window and boundary set', () => {
    const clock = fakeClock(0);
    const m = new MetricsEngine({ windowMs: 1000, slices: 10, boundaries: [50], clock: clock.now });
    m.observe('lat', 10);
    m.observe('lat', 90);
    const s = m.summary('lat')!;
    expect(s.buckets).toEqual([
      { le: 50, count: 1 },
      { le: Number.POSITIVE_INFINITY, count: 2 },
    ]);
    clock.set(1000);
    expect(m.summary('lat')!.count).toBe(0);
  });
});
