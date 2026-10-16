/**
 * @module metrics_engine
 * @description High-performance analytics primitives for NexusPulse latency
 * and throughput telemetry.
 *
 * ## Components
 *
 * - **`TDigest`** — a merging t-digest (Dunning & Ertl) with the k1 (arcsine)
 *   scale function. It keeps a bounded set of weighted centroids that are dense
 *   near the distribution tails, so p99 and p99.9 stay accurate with a few
 *   hundred centroids regardless of how many samples were observed. Buffered
 *   inserts are merged in one sort-and-sweep pass, which is amortised O(1) per
 *   sample. Exact `min` and `max` are tracked so the extreme quantiles are
 *   clamped to real observations.
 *
 * - **`SlidingWindowHistogram`** — a ring of time slices. Each slice owns a
 *   t-digest, fixed-boundary bucket counts, and running count/sum/min/max. A
 *   query merges only the slices inside the window, so old samples age out
 *   without any per-sample bookkeeping. Bucket counts are cumulative
 *   (Prometheus `le` convention).
 *
 * - **`CounterRegistry`** — monotonic labelled counters. Label sets are
 *   canonicalised so `{a, b}` and `{b, a}` address the same series.
 *   Cardinality is capped to prevent unbounded label explosion.
 *
 * - **Downsampling** — `downsample` aggregates a sorted time series into fixed
 *   buckets (avg, min, max, sum, last, count). `lttb` reduces a series to a
 *   target point count while preserving its visual shape (Largest-Triangle-
 *   Three-Buckets).
 *
 * - **`MetricsEngine`** — a facade that lazily creates named histograms and
 *   counters, enforces series limits, and produces a point-in-time snapshot.
 *
 * ## Determinism
 *
 * Every time-dependent component accepts an injected `clock` so windows can be
 * tested without sleeping. The algorithms contain no randomness.
 *
 * @packageDocumentation
 */

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default t-digest compression. Roughly bounds the centroid count to ~2×this. */
export const DEFAULT_COMPRESSION = 100;

/** Default latency bucket boundaries, in milliseconds. */
export const DEFAULT_LATENCY_BOUNDARIES_MS: readonly number[] = Object.freeze([
  1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10_000,
]);

/** Default window length for a sliding histogram, in milliseconds. */
export const DEFAULT_WINDOW_MS = 60_000;

/** Default number of time slices per sliding window. */
export const DEFAULT_SLICES = 12;

/** Default cap on the number of distinct histogram or counter series. */
export const DEFAULT_MAX_SERIES = 10_000;

const METRIC_NAME_RE = /^[a-zA-Z_:][a-zA-Z0-9_:.-]{0,127}$/;
const LABEL_KEY_RE = /^[a-zA-Z_][a-zA-Z0-9_]{0,63}$/;
const LABEL_VALUE_MAX = 128;
const LABEL_COUNT_MAX = 16;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Root error for every metrics failure. Carries a stable `code`. */
export class MetricsError extends Error {
  public readonly code: string;
  public readonly details: Readonly<Record<string, unknown>>;

  constructor(message: string, code: string, details: Record<string, unknown> = {}) {
    super(message);
    this.name = 'MetricsError';
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

/** A value, name, label, weight, or configuration is out of range or malformed. */
export class InvalidMetricError extends MetricsError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'INVALID_METRIC', details);
    this.name = 'InvalidMetricError';
  }
}

/** A series-cardinality limit would be exceeded. */
export class CardinalityError extends MetricsError {
  constructor(kind: string, limit: number) {
    super(`${kind} series limit of ${limit} reached`, 'CARDINALITY_LIMIT', { kind, limit });
    this.name = 'CardinalityError';
  }
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

function requireFinite(value: unknown, field: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new InvalidMetricError(`${field} must be a finite number`, { field, value });
  }
  return value;
}

function requireNonNegative(value: unknown, field: string): number {
  const n = requireFinite(value, field);
  if (n < 0) {
    throw new InvalidMetricError(`${field} must be ≥ 0`, { field, value });
  }
  return n;
}

function requireName(name: unknown): string {
  if (typeof name !== 'string' || !METRIC_NAME_RE.test(name)) {
    throw new InvalidMetricError(`metric name must match ${METRIC_NAME_RE.source}`, { name });
  }
  return name;
}

function requireInt(value: unknown, field: string, min: number, max: number): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > max) {
    throw new InvalidMetricError(`${field} must be an integer in [${min}, ${max}]`, {
      field,
      value,
    });
  }
  return value;
}

/** Validates and canonicalises a label set into a stable, sorted array. */
function canonicalLabels(labels: Readonly<Record<string, string>> | undefined): Array<[string, string]> {
  if (labels === undefined) return [];
  const entries = Object.entries(labels);
  if (entries.length > LABEL_COUNT_MAX) {
    throw new InvalidMetricError(`at most ${LABEL_COUNT_MAX} labels are allowed`, {
      count: entries.length,
    });
  }
  for (const [key, value] of entries) {
    if (!LABEL_KEY_RE.test(key)) {
      throw new InvalidMetricError(`label key must match ${LABEL_KEY_RE.source}`, { key });
    }
    if (typeof value !== 'string' || value.length > LABEL_VALUE_MAX) {
      throw new InvalidMetricError(`label '${key}' must be a string of at most ${LABEL_VALUE_MAX} characters`, {
        key,
      });
    }
  }
  return entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
}

// ---------------------------------------------------------------------------
// TDigest
// ---------------------------------------------------------------------------

interface Centroid {
  mean: number;
  weight: number;
}

/** Quantile snapshot of a distribution. `null` means no samples. */
export interface PercentileSet {
  readonly p50: number | null;
  readonly p90: number | null;
  readonly p95: number | null;
  readonly p99: number | null;
  readonly p999: number | null;
}

/**
 * Merging t-digest for streaming quantile estimation.
 *
 * Scale function k1: `k(q) = (δ / 2π) · asin(2q − 1)`. Two adjacent centroids
 * may merge only if `k(q_right) − k(q_left) ≤ 1`. Because the derivative of
 * `k` is largest at the tails, tail centroids stay small and therefore precise.
 *
 * ```ts
 * const d = new TDigest();
 * for (const latency of latencies) d.add(latency);
 * d.quantile(0.99); // p99 estimate
 * ```
 */
export class TDigest {
  readonly #compression: number;
  readonly #bufferCap: number;
  #centroids: Centroid[] = [];
  #buffer: Centroid[] = [];
  #total = 0;
  #min = Number.POSITIVE_INFINITY;
  #max = Number.NEGATIVE_INFINITY;

  constructor(compression: number = DEFAULT_COMPRESSION) {
    this.#compression = requireInt(compression, 'compression', 20, 1000);
    this.#bufferCap = this.#compression * 4;
  }

  /** Total weight (sample count when all weights are 1). */
  get count(): number {
    return this.#total;
  }

  /** Smallest value observed, or `null` if empty. */
  get min(): number | null {
    return this.#total === 0 ? null : this.#min;
  }

  /** Largest value observed, or `null` if empty. */
  get max(): number | null {
    return this.#total === 0 ? null : this.#max;
  }

  /** Weighted mean of all observations, or `null` if empty. */
  get mean(): number | null {
    if (this.#total === 0) return null;
    this.#flush();
    let sum = 0;
    for (const c of this.#centroids) sum += c.mean * c.weight;
    return sum / this.#total;
  }

  /** Number of centroids currently retained (after compaction). */
  get centroidCount(): number {
    this.#flush();
    return this.#centroids.length;
  }

  /**
   * Adds one observation.
   *
   * @throws {InvalidMetricError} if `value` is not finite or `weight` is not positive.
   */
  add(value: number, weight = 1): void {
    requireFinite(value, 'value');
    if (!Number.isFinite(weight) || weight <= 0) {
      throw new InvalidMetricError('weight must be a finite number > 0', { weight });
    }
    this.#buffer.push({ mean: value, weight });
    this.#total += weight;
    if (value < this.#min) this.#min = value;
    if (value > this.#max) this.#max = value;
    if (this.#buffer.length >= this.#bufferCap) this.#flush();
  }

  /**
   * Estimates the value at quantile `q` in [0, 1] by interpolating between
   * centroid centres. Returns `null` if the digest is empty.
   *
   * @throws {InvalidMetricError} if `q` is outside [0, 1].
   */
  quantile(q: number): number | null {
    if (typeof q !== 'number' || !(q >= 0 && q <= 1)) {
      throw new InvalidMetricError('quantile must be in [0, 1]', { q });
    }
    if (this.#total === 0) return null;
    this.#flush();
    if (q === 0) return this.#min;
    if (q === 1) return this.#max;

    const cs = this.#centroids;
    const total = this.#total;
    const target = q * total;

    let cum = 0;
    let prevCenter = 0;
    let prevMean = this.#min;
    for (const c of cs) {
      const center = cum + c.weight / 2;
      if (target <= center) {
        const span = center - prevCenter;
        const t = span > 0 ? (target - prevCenter) / span : 0;
        return clamp(prevMean + t * (c.mean - prevMean), this.#min, this.#max);
      }
      prevCenter = center;
      prevMean = c.mean;
      cum += c.weight;
    }
    // Target lies beyond the last centroid centre: interpolate towards max.
    const span = total - prevCenter;
    const t = span > 0 ? (target - prevCenter) / span : 1;
    return clamp(prevMean + t * (this.#max - prevMean), this.#min, this.#max);
  }

  /**
   * Folds `other` into this digest. `other` is not modified.
   */
  merge(other: TDigest): void {
    other.#flush();
    for (const c of other.#centroids) {
      this.#buffer.push({ mean: c.mean, weight: c.weight });
    }
    this.#total += other.#total;
    if (other.#total > 0) {
      if (other.#min < this.#min) this.#min = other.#min;
      if (other.#max > this.#max) this.#max = other.#max;
    }
    if (this.#buffer.length >= this.#bufferCap) this.#flush();
  }

  /** The standard percentile set used throughout NexusPulse. */
  percentiles(): PercentileSet {
    return {
      p50: this.quantile(0.5),
      p90: this.quantile(0.9),
      p95: this.quantile(0.95),
      p99: this.quantile(0.99),
      p999: this.quantile(0.999),
    };
  }

  /** Scale function k1 (arcsine). Range is [−δ/4, δ/4]. */
  #k(q: number): number {
    return (this.#compression / (2 * Math.PI)) * Math.asin(2 * q - 1);
  }

  /**
   * Merges the buffer into the centroid list in one sorted sweep, combining
   * neighbours while the k-scale condition holds.
   */
  #flush(): void {
    if (this.#buffer.length === 0) return;
    const all = this.#centroids.concat(this.#buffer);
    all.sort((a, b) => a.mean - b.mean);
    this.#buffer = [];

    const total = this.#total;
    const merged: Centroid[] = [];
    let current: Centroid = { mean: all[0]!.mean, weight: all[0]!.weight };
    let cum = 0;

    for (let i = 1; i < all.length; i++) {
      const next = all[i]!;
      const proposed = current.weight + next.weight;
      const q0 = cum / total;
      const q1 = (cum + proposed) / total;
      if (this.#k(q1) - this.#k(q0) <= 1) {
        current.mean += ((next.mean - current.mean) * next.weight) / proposed;
        current.weight = proposed;
      } else {
        merged.push(current);
        cum += current.weight;
        current = { mean: next.mean, weight: next.weight };
      }
    }
    merged.push(current);
    this.#centroids = merged;
  }
}

function clamp(x: number, lo: number, hi: number): number {
  return x < lo ? lo : x > hi ? hi : x;
}

/** Computes the standard percentile set from a digest, with nulls when empty. */
function percentilesOf(digest: TDigest): PercentileSet {
  return digest.percentiles();
}

// ---------------------------------------------------------------------------
// SlidingWindowHistogram
// ---------------------------------------------------------------------------

/** Options for `SlidingWindowHistogram`. */
export interface SlidingWindowOptions {
  /** Window length in milliseconds. Must be divisible by `slices`. */
  readonly windowMs?: number;
  /** Number of time slices in the window. */
  readonly slices?: number;
  /** Strictly increasing, non-negative bucket upper bounds. */
  readonly boundaries?: readonly number[];
  /** t-digest compression for each slice. */
  readonly compression?: number;
  /** Time source in milliseconds. Defaults to `Date.now`. */
  readonly clock?: () => number;
}

/** One bucket of a cumulative histogram. `le` of `Infinity` is the overflow bucket. */
export interface HistogramBucket {
  readonly le: number;
  readonly count: number;
}

/** Point-in-time summary of a sliding-window latency distribution. */
export interface LatencySummary extends PercentileSet {
  readonly windowMs: number;
  readonly count: number;
  readonly sum: number;
  readonly mean: number | null;
  readonly min: number | null;
  readonly max: number | null;
  /** Cumulative counts; the final bucket (`le: Infinity`) equals `count`. */
  readonly buckets: readonly HistogramBucket[];
}

interface Slot {
  readonly id: number;
  readonly digest: TDigest;
  readonly bucketCounts: number[];
  count: number;
  sum: number;
  min: number;
  max: number;
}

/**
 * Latency histogram over a sliding time window.
 *
 * ```ts
 * const h = new SlidingWindowHistogram({ windowMs: 60_000 });
 * h.record(12.5);
 * h.snapshot().p99; // p99 over the last minute
 * ```
 */
export class SlidingWindowHistogram {
  readonly #windowMs: number;
  readonly #slices: number;
  readonly #width: number;
  readonly #boundaries: readonly number[];
  readonly #compression: number;
  readonly #clock: () => number;
  readonly #ring: Array<Slot | null>;

  constructor(options: SlidingWindowOptions = {}) {
    const windowMs = requireInt(options.windowMs ?? DEFAULT_WINDOW_MS, 'windowMs', 1, 86_400_000);
    const slices = requireInt(options.slices ?? DEFAULT_SLICES, 'slices', 1, 256);
    if (windowMs % slices !== 0) {
      throw new InvalidMetricError('windowMs must be divisible by slices', { windowMs, slices });
    }
    const boundaries = options.boundaries ?? DEFAULT_LATENCY_BOUNDARIES_MS;
    validateBoundaries(boundaries);

    this.#windowMs = windowMs;
    this.#slices = slices;
    this.#width = windowMs / slices;
    this.#boundaries = Object.freeze([...boundaries]);
    this.#compression = requireInt(options.compression ?? DEFAULT_COMPRESSION, 'compression', 20, 1000);
    this.#clock = options.clock ?? Date.now;
    this.#ring = new Array<Slot | null>(slices).fill(null);
  }

  /**
   * Records one non-negative observation at the current clock time.
   *
   * @throws {InvalidMetricError} if the value is negative or not finite, or the clock has gone backwards past a live slice.
   */
  record(value: number): void {
    requireNonNegative(value, 'value');
    const id = this.#sliceId(this.#clock());
    const idx = mod(id, this.#slices);
    let slot = this.#ring[idx] ?? null;
    if (slot !== null && slot.id > id) {
      throw new InvalidMetricError('clock moved backwards past a live slice', {
        sliceId: id,
        liveSliceId: slot.id,
      });
    }
    if (slot === null || slot.id !== id) {
      slot = {
        id,
        digest: new TDigest(this.#compression),
        bucketCounts: new Array<number>(this.#boundaries.length + 1).fill(0),
        count: 0,
        sum: 0,
        min: Number.POSITIVE_INFINITY,
        max: Number.NEGATIVE_INFINITY,
      };
      this.#ring[idx] = slot;
    }
    slot.digest.add(value);
    slot.count += 1;
    slot.sum += value;
    if (value < slot.min) slot.min = value;
    if (value > slot.max) slot.max = value;
    slot.bucketCounts[this.#bucketIndex(value)]! += 1;
  }

  /** Merges every live slice and summarises the window. */
  snapshot(): LatencySummary {
    const nowId = this.#sliceId(this.#clock());
    const merged = new TDigest(this.#compression);
    const counts = new Array<number>(this.#boundaries.length + 1).fill(0);
    let count = 0;
    let sum = 0;
    let min = Number.POSITIVE_INFINITY;
    let max = Number.NEGATIVE_INFINITY;

    for (const slot of this.#ring) {
      if (slot === null) continue;
      if (slot.id <= nowId - this.#slices || slot.id > nowId) continue;
      merged.merge(slot.digest);
      count += slot.count;
      sum += slot.sum;
      if (slot.min < min) min = slot.min;
      if (slot.max > max) max = slot.max;
      for (let i = 0; i < counts.length; i++) counts[i]! += slot.bucketCounts[i]!;
    }

    const buckets: HistogramBucket[] = [];
    let cumulative = 0;
    for (let i = 0; i < this.#boundaries.length; i++) {
      cumulative += counts[i]!;
      buckets.push({ le: this.#boundaries[i]!, count: cumulative });
    }
    cumulative += counts[this.#boundaries.length]!;
    buckets.push({ le: Number.POSITIVE_INFINITY, count: cumulative });

    const pct = percentilesOf(merged);
    return {
      windowMs: this.#windowMs,
      count,
      sum,
      mean: count === 0 ? null : sum / count,
      min: count === 0 ? null : min,
      max: count === 0 ? null : max,
      ...pct,
      buckets,
    };
  }

  #sliceId(timeMs: number): number {
    return Math.floor(timeMs / this.#width);
  }

  #bucketIndex(value: number): number {
    // First boundary >= value; the overflow bucket is at index boundaries.length.
    let lo = 0;
    let hi = this.#boundaries.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (value <= this.#boundaries[mid]!) hi = mid;
      else lo = mid + 1;
    }
    return lo;
  }
}

function validateBoundaries(boundaries: readonly number[]): void {
  if (boundaries.length === 0) {
    throw new InvalidMetricError('at least one bucket boundary is required');
  }
  let prev = Number.NEGATIVE_INFINITY;
  for (const b of boundaries) {
    requireNonNegative(b, 'boundary');
    if (b <= prev) {
      throw new InvalidMetricError('bucket boundaries must be strictly increasing', { boundary: b });
    }
    prev = b;
  }
}

function mod(a: number, n: number): number {
  return ((a % n) + n) % n;
}

// ---------------------------------------------------------------------------
// CounterRegistry
// ---------------------------------------------------------------------------

/** One labelled counter series. */
export interface CounterSample {
  readonly name: string;
  readonly labels: Readonly<Record<string, string>>;
  readonly value: number;
}

interface CounterSeries {
  readonly name: string;
  readonly labels: Array<[string, string]>;
  value: number;
}

/**
 * Monotonic labelled counters. Values only increase; `delta` must be ≥ 0.
 */
export class CounterRegistry {
  readonly #series = new Map<string, CounterSeries>();
  readonly #maxSeries: number;

  constructor(maxSeries: number = DEFAULT_MAX_SERIES) {
    this.#maxSeries = requireInt(maxSeries, 'maxSeries', 1, 1_000_000);
  }

  /** Number of distinct series currently held. */
  get seriesCount(): number {
    return this.#series.size;
  }

  /**
   * Adds `delta` to the series identified by `name` and `labels`.
   *
   * @throws {InvalidMetricError} for a bad name, label, or negative/non-finite delta.
   * @throws {CardinalityError}   if this would create a series beyond the limit.
   */
  increment(name: string, delta = 1, labels?: Readonly<Record<string, string>>): number {
    requireName(name);
    requireNonNegative(delta, 'delta');
    const canon = canonicalLabels(labels);
    const key = seriesKey(name, canon);
    let series = this.#series.get(key);
    if (series === undefined) {
      if (this.#series.size >= this.#maxSeries) {
        throw new CardinalityError('counter', this.#maxSeries);
      }
      series = { name, labels: canon, value: 0 };
      this.#series.set(key, series);
    }
    series.value += delta;
    return series.value;
  }

  /** Current value of a series, or `0` if it has never been incremented. */
  value(name: string, labels?: Readonly<Record<string, string>>): number {
    requireName(name);
    return this.#series.get(seriesKey(name, canonicalLabels(labels)))?.value ?? 0;
  }

  /** Every series, sorted by name then canonical labels. */
  snapshot(): CounterSample[] {
    return [...this.#series.values()]
      .sort((a, b) => {
        if (a.name !== b.name) return a.name < b.name ? -1 : 1;
        return seriesKey('', a.labels) < seriesKey('', b.labels)
          ? -1
          : seriesKey('', a.labels) > seriesKey('', b.labels)
            ? 1
            : 0;
      })
      .map((s) => ({ name: s.name, labels: Object.fromEntries(s.labels), value: s.value }));
  }
}

function seriesKey(name: string, labels: Array<[string, string]>): string {
  return JSON.stringify([name, labels]);
}

// ---------------------------------------------------------------------------
// Downsampling
// ---------------------------------------------------------------------------

/** A single time-series sample. */
export interface Point {
  readonly t: number;
  readonly v: number;
}

/** Aggregation applied within each downsampling bucket. */
export type Aggregation = 'avg' | 'min' | 'max' | 'sum' | 'last' | 'count';

/**
 * Aggregates a time-ordered series into fixed-width buckets. Each output point
 * is stamped with its bucket's start time. Empty buckets are omitted.
 *
 * @throws {InvalidMetricError} if `bucketMs` is not a positive integer, a point
 *         is not finite, or timestamps are not non-decreasing.
 */
export function downsample(
  points: readonly Point[],
  options: { readonly bucketMs: number; readonly aggregation: Aggregation },
): Point[] {
  const bucketMs = requireInt(options.bucketMs, 'bucketMs', 1, Number.MAX_SAFE_INTEGER);
  const out: Point[] = [];
  let currentStart = Number.NaN;
  let count = 0;
  let sum = 0;
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  let last = 0;
  let prevT = Number.NEGATIVE_INFINITY;

  const flush = (): void => {
    if (count === 0) return;
    let v: number;
    switch (options.aggregation) {
      case 'avg':
        v = sum / count;
        break;
      case 'min':
        v = min;
        break;
      case 'max':
        v = max;
        break;
      case 'sum':
        v = sum;
        break;
      case 'last':
        v = last;
        break;
      case 'count':
        v = count;
        break;
    }
    out.push({ t: currentStart, v });
  };

  for (const p of points) {
    requireFinite(p.t, 't');
    requireFinite(p.v, 'v');
    if (p.t < prevT) {
      throw new InvalidMetricError('points must be sorted by non-decreasing t', { t: p.t, prevT });
    }
    prevT = p.t;
    const start = Math.floor(p.t / bucketMs) * bucketMs;
    if (start !== currentStart) {
      flush();
      currentStart = start;
      count = 0;
      sum = 0;
      min = Number.POSITIVE_INFINITY;
      max = Number.NEGATIVE_INFINITY;
    }
    count += 1;
    sum += p.v;
    if (p.v < min) min = p.v;
    if (p.v > max) max = p.v;
    last = p.v;
  }
  flush();
  return out;
}

/**
 * Largest-Triangle-Three-Buckets downsampling. Reduces `points` to `threshold`
 * points while keeping the first and last points and, in every bucket, the
 * point forming the largest triangle with its neighbours. Peaks and troughs
 * survive, which plain decimation would lose.
 *
 * @throws {InvalidMetricError} if `threshold` is below 3 or not an integer.
 */
export function lttb(points: readonly Point[], threshold: number): Point[] {
  requireInt(threshold, 'threshold', 3, Number.MAX_SAFE_INTEGER);
  const n = points.length;
  if (n <= threshold) return points.slice();

  const out: Point[] = [points[0]!];
  const bucketSize = (n - 2) / (threshold - 2);
  let a = 0;

  for (let i = 0; i < threshold - 2; i++) {
    // Average of the next bucket, used as the third triangle vertex.
    const avgStart = Math.floor((i + 1) * bucketSize) + 1;
    const avgEnd = Math.min(Math.floor((i + 2) * bucketSize) + 1, n);
    const avgLen = avgEnd - avgStart;
    let avgX = 0;
    let avgY = 0;
    if (avgLen > 0) {
      for (let j = avgStart; j < avgEnd; j++) {
        avgX += points[j]!.t;
        avgY += points[j]!.v;
      }
      avgX /= avgLen;
      avgY /= avgLen;
    } else {
      avgX = points[n - 1]!.t;
      avgY = points[n - 1]!.v;
    }

    const rangeStart = Math.floor(i * bucketSize) + 1;
    const rangeEnd = Math.floor((i + 1) * bucketSize) + 1;
    const pa = points[a]!;
    let maxArea = -1;
    let nextA = rangeStart;
    for (let j = rangeStart; j < rangeEnd; j++) {
      const pj = points[j]!;
      const area = Math.abs((pa.t - avgX) * (pj.v - pa.v) - (pa.t - pj.t) * (avgY - pa.v)) * 0.5;
      if (area > maxArea) {
        maxArea = area;
        nextA = j;
      }
    }
    out.push(points[nextA]!);
    a = nextA;
  }

  out.push(points[n - 1]!);
  return out;
}

// ---------------------------------------------------------------------------
// MetricsEngine facade
// ---------------------------------------------------------------------------

/** Options for `MetricsEngine`. */
export interface MetricsEngineOptions extends SlidingWindowOptions {
  /** Maximum number of distinct histograms and, separately, counter series. */
  readonly maxSeries?: number;
}

/** Point-in-time view of every metric held by an engine. */
export interface MetricsSnapshot {
  readonly histograms: Readonly<Record<string, LatencySummary>>;
  readonly counters: readonly CounterSample[];
}

/**
 * Facade over named sliding histograms and counters.
 *
 * ```ts
 * const m = new MetricsEngine({ windowMs: 60_000 });
 * m.observe('http.latency_ms', 42);
 * m.increment('http.requests', 1, { route: '/ingest' });
 * m.snapshot();
 * ```
 */
export class MetricsEngine {
  readonly #histogramOptions: SlidingWindowOptions;
  readonly #histograms = new Map<string, SlidingWindowHistogram>();
  readonly #counters: CounterRegistry;
  readonly #maxSeries: number;

  constructor(options: MetricsEngineOptions = {}) {
    const { maxSeries, ...histogramOptions } = options;
    this.#maxSeries = requireInt(maxSeries ?? DEFAULT_MAX_SERIES, 'maxSeries', 1, 1_000_000);
    this.#histogramOptions = histogramOptions;
    this.#counters = new CounterRegistry(this.#maxSeries);
    // Validate the histogram configuration eagerly so a bad config fails at construction.
    new SlidingWindowHistogram(histogramOptions);
  }

  /** Names of every histogram created so far, sorted. */
  histogramNames(): string[] {
    return [...this.#histograms.keys()].sort();
  }

  /**
   * Records a latency observation into the named histogram, creating it on first use.
   *
   * @throws {CardinalityError} if creating the histogram would exceed `maxSeries`.
   */
  observe(name: string, value: number): void {
    requireName(name);
    let h = this.#histograms.get(name);
    if (h === undefined) {
      if (this.#histograms.size >= this.#maxSeries) {
        throw new CardinalityError('histogram', this.#maxSeries);
      }
      h = new SlidingWindowHistogram(this.#histogramOptions);
      this.#histograms.set(name, h);
    }
    h.record(value);
  }

  /** Summary of the named histogram, or `undefined` if it does not exist. */
  summary(name: string): LatencySummary | undefined {
    return this.#histograms.get(name)?.snapshot();
  }

  /** Increments a labelled counter. See `CounterRegistry.increment`. */
  increment(name: string, delta = 1, labels?: Readonly<Record<string, string>>): number {
    return this.#counters.increment(name, delta, labels);
  }

  /** Current value of a labelled counter. */
  counter(name: string, labels?: Readonly<Record<string, string>>): number {
    return this.#counters.value(name, labels);
  }

  /** Every histogram summary and every counter series, at this instant. */
  snapshot(): MetricsSnapshot {
    const histograms: Record<string, LatencySummary> = {};
    for (const name of this.histogramNames()) {
      histograms[name] = this.#histograms.get(name)!.snapshot();
    }
    return { histograms, counters: this.#counters.snapshot() };
  }
}
