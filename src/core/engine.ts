/**
 * NexusPulse — Core Stream Processing Engine (Module 1)
 * =====================================================
 *
 * A dependency-free, in-process engine that ingests telemetry events, keeps
 * bounded per-series statistical state, and runs pluggable anomaly detectors.
 *
 * Pipeline:
 *
 *   submit(raw) ──► validate ──► bounded queue (backpressure policy)
 *                                     │
 *                          flush() / timer loop
 *                                     ▼
 *        late-event filter ──► dedup ──► detectors (isolated) ──► baseline update
 *                                                    │
 *                                          cooldown / escalation filter
 *                                                    ▼
 *                                           'anomaly' listeners
 *
 * Design guarantees
 * -----------------
 *  - Memory is bounded everywhere: queue capacity, per-series window, per-series
 *    dedup cache, and total series count (LRU eviction).
 *  - A failing detector or listener never stops the pipeline; it is surfaced via
 *    the 'error' event and counted in metrics.
 *  - Outliers do not poison the baseline: anomalous samples are excluded from the
 *    rolling statistics until `baselineAdaptationThreshold` consecutive anomalies
 *    indicate a genuine level shift, after which the baseline adapts.
 *  - Time-dependent behaviour (cooldowns, lateness, future-skew validation) is
 *    driven by event timestamps and an injectable clock, so it is deterministic
 *    under test.
 *
 * Note: error classes live here for now so the module is self-contained; they are
 * intended to be re-exported from `/src/utils/errors` in a later module.
 */

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Base class for every error raised intentionally by NexusPulse. */
export class NexusError extends Error {
  public readonly code: string;
  public readonly details: Readonly<Record<string, unknown>>;

  constructor(code: string, message: string, details: Record<string, unknown> = {}) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = new.target.name;
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

/** Raised when a telemetry event fails schema validation. */
export class ValidationError extends NexusError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('VALIDATION_ERROR', message, details);
  }
}

/** Raised when engine or detector configuration is invalid. */
export class ConfigError extends NexusError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('CONFIG_ERROR', message, details);
  }
}

/** Raised when an operation is not allowed in the engine's current lifecycle state. */
export class EngineStateError extends NexusError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('ENGINE_STATE_ERROR', message, details);
  }
}

/** Raised by `submit()` when the queue is full and the policy is `reject`. */
export class BackpressureError extends NexusError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('BACKPRESSURE', message, details);
  }
}

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

export type Severity = 'info' | 'warning' | 'critical';

const SEVERITY_RANK: Readonly<Record<Severity, number>> = Object.freeze({
  info: 0,
  warning: 1,
  critical: 2,
});

/** A single telemetry data point. `timestamp` is epoch milliseconds. */
export interface TelemetryEvent {
  readonly id: string;
  readonly source: string;
  readonly metric: string;
  readonly value: number;
  readonly timestamp: number;
  readonly tags?: Readonly<Record<string, string>>;
}

/** An anomaly published to `'anomaly'` listeners. */
export interface Anomaly {
  readonly eventId: string;
  readonly seriesKey: string;
  readonly source: string;
  readonly metric: string;
  readonly detector: string;
  readonly severity: Severity;
  readonly value: number;
  /** The value the detector expected (baseline mean or violated bound), if meaningful. */
  readonly expected: number | null;
  /** Detector-specific magnitude (sigma distance for statistical detectors). */
  readonly score: number;
  readonly timestamp: number;
  readonly message: string;
}

/** Why an event did not make it through the pipeline. */
export type DropReason =
  | 'invalid'
  | 'queue-full'
  | 'late'
  | 'duplicate'
  | 'shutdown';

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/** 2000-01-01T00:00:00Z. Smaller timestamps are almost certainly seconds, not milliseconds. */
export const MIN_TIMESTAMP_MS = 946_684_800_000;
export const DEFAULT_MAX_FUTURE_SKEW_MS = 60_000;

const MAX_ID_LENGTH = 128;
const MAX_NAME_LENGTH = 256;
const MAX_TAG_COUNT = 32;
const MAX_TAG_KEY_LENGTH = 64;
const MAX_TAG_VALUE_LENGTH = 256;
/** Source/metric names: no whitespace, no '|' (reserved as the series-key separator). */
const NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:\/-]*$/;

export interface ValidateOptions {
  /** Current time in epoch ms; defaults to `Date.now()`. */
  readonly nowMs?: number;
  /** How far in the future an event timestamp may be. */
  readonly maxFutureSkewMs?: number;
}

/**
 * Validates an untrusted value and returns a frozen, normalised TelemetryEvent.
 * All problems are collected and reported together in `error.details.issues`.
 *
 * @throws ValidationError when the value is not a valid event.
 */
export function validateEvent(raw: unknown, options: ValidateOptions = {}): TelemetryEvent {
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new ValidationError('Invalid telemetry event: event must be a non-null object', {
      issues: ['event must be a non-null object'],
    });
  }

  const record = raw as Record<string, unknown>;
  const issues: string[] = [];
  const nowMs = options.nowMs ?? Date.now();
  const maxFutureSkewMs = options.maxFutureSkewMs ?? DEFAULT_MAX_FUTURE_SKEW_MS;

  const { id, source, metric, value, timestamp } = record;

  if (typeof id !== 'string' || id.length === 0 || id.length > MAX_ID_LENGTH) {
    issues.push(`id must be a string of 1-${MAX_ID_LENGTH} characters`);
  }

  for (const [field, name] of [
    ['source', source],
    ['metric', metric],
  ] as const) {
    if (
      typeof name !== 'string' ||
      name.length === 0 ||
      name.length > MAX_NAME_LENGTH ||
      !NAME_PATTERN.test(name)
    ) {
      issues.push(
        `${field} must be 1-${MAX_NAME_LENGTH} characters matching ${NAME_PATTERN.source}`,
      );
    }
  }

  if (typeof value !== 'number' || !Number.isFinite(value)) {
    issues.push('value must be a finite number');
  }

  if (typeof timestamp !== 'number' || !Number.isInteger(timestamp)) {
    issues.push('timestamp must be an integer number of epoch milliseconds');
  } else if (timestamp < MIN_TIMESTAMP_MS) {
    issues.push(
      `timestamp ${timestamp} is before 2000-01-01; epoch milliseconds are required (got seconds?)`,
    );
  } else if (timestamp > nowMs + maxFutureSkewMs) {
    issues.push(`timestamp is more than ${maxFutureSkewMs}ms in the future`);
  }

  let tags: Record<string, string> | undefined;
  if (record.tags !== undefined) {
    if (typeof record.tags !== 'object' || record.tags === null || Array.isArray(record.tags)) {
      issues.push('tags must be an object whose values are strings');
    } else {
      const entries = Object.entries(record.tags as Record<string, unknown>);
      if (entries.length > MAX_TAG_COUNT) {
        issues.push(`tags may contain at most ${MAX_TAG_COUNT} entries`);
      } else {
        const out: Record<string, string> = {};
        for (const [key, val] of entries) {
          if (key === '__proto__' || key.length === 0 || key.length > MAX_TAG_KEY_LENGTH) {
            issues.push(`tag key "${key.slice(0, 16)}" is invalid`);
          } else if (typeof val !== 'string' || val.length > MAX_TAG_VALUE_LENGTH) {
            issues.push(`tag "${key}" must be a string of at most ${MAX_TAG_VALUE_LENGTH} characters`);
          } else {
            out[key] = val;
          }
        }
        tags = Object.freeze(out);
      }
    }
  }

  if (issues.length > 0) {
    throw new ValidationError(`Invalid telemetry event: ${issues.join('; ')}`, { issues });
  }

  const event: TelemetryEvent = {
    id: id as string,
    source: source as string,
    metric: metric as string,
    value: value as number,
    timestamp: timestamp as number,
    ...(tags !== undefined ? { tags } : {}),
  };
  return Object.freeze(event);
}

/** Canonical key for the time series identified by (source, metric). */
export function seriesKeyOf(source: string, metric: string): string {
  return `${source}|${metric}`;
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/** Fixed-capacity FIFO ring buffer with O(1) push/shift. */
export class RingBuffer<T> implements Iterable<T> {
  private readonly slots: Array<T | undefined>;
  private head = 0;
  private count = 0;

  constructor(public readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new ConfigError('RingBuffer capacity must be a positive integer', { capacity });
    }
    this.slots = new Array<T | undefined>(capacity).fill(undefined);
  }

  get size(): number {
    return this.count;
  }

  get isFull(): boolean {
    return this.count === this.capacity;
  }

  /** Appends an item. If the buffer is full the oldest item is overwritten and returned. */
  push(item: T): T | undefined {
    if (this.count === this.capacity) {
      const evicted = this.slots[this.head];
      this.slots[this.head] = item;
      this.head = (this.head + 1) % this.capacity;
      return evicted;
    }
    this.slots[(this.head + this.count) % this.capacity] = item;
    this.count++;
    return undefined;
  }

  /** Removes and returns the oldest item, or undefined when empty. */
  shift(): T | undefined {
    if (this.count === 0) return undefined;
    const item = this.slots[this.head];
    this.slots[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this.count--;
    return item;
  }

  /** Returns the oldest item without removing it. */
  peek(): T | undefined {
    return this.count === 0 ? undefined : this.slots[this.head];
  }

  clear(): void {
    this.slots.fill(undefined);
    this.head = 0;
    this.count = 0;
  }

  /** Items from oldest to newest. */
  toArray(): T[] {
    const out: T[] = [];
    for (let i = 0; i < this.count; i++) {
      out.push(this.slots[(this.head + i) % this.capacity] as T);
    }
    return out;
  }

  *[Symbol.iterator](): IterableIterator<T> {
    for (let i = 0; i < this.count; i++) {
      yield this.slots[(this.head + i) % this.capacity] as T;
    }
  }
}

export interface WindowSnapshot {
  readonly count: number;
  readonly mean: number;
  /** Sample variance (n-1 denominator); 0 when fewer than two samples. */
  readonly variance: number;
  readonly stdDev: number;
}

/**
 * Sliding-window mean/variance using Welford's algorithm with removal of the
 * evicted sample. Accumulated floating-point drift is bounded by rebuilding the
 * statistics from the window every `recomputeEvery` operations.
 */
export class RollingStats {
  private readonly buffer: RingBuffer<number>;
  private n = 0;
  private mean = 0;
  private m2 = 0;
  private opsSinceRecompute = 0;

  constructor(
    capacity: number,
    private readonly recomputeEvery = 10_000,
  ) {
    if (!Number.isInteger(recomputeEvery) || recomputeEvery < 1) {
      throw new ConfigError('recomputeEvery must be a positive integer', { recomputeEvery });
    }
    this.buffer = new RingBuffer<number>(capacity);
  }

  get capacity(): number {
    return this.buffer.capacity;
  }

  get count(): number {
    return this.n;
  }

  /** Adds a sample, evicting the oldest one when the window is full. */
  add(x: number): void {
    if (!Number.isFinite(x)) {
      throw new ValidationError('RollingStats only accepts finite numbers', { value: String(x) });
    }
    if (this.buffer.isFull) {
      const oldest = this.buffer.shift() as number;
      this.removeSample(oldest);
    }
    this.buffer.push(x);
    this.addSample(x);

    if (++this.opsSinceRecompute >= this.recomputeEvery) this.recompute();
  }

  snapshot(): WindowSnapshot {
    const variance = this.n < 2 ? 0 : this.m2 / (this.n - 1);
    return { count: this.n, mean: this.mean, variance, stdDev: Math.sqrt(variance) };
  }

  private addSample(x: number): void {
    this.n++;
    const delta = x - this.mean;
    this.mean += delta / this.n;
    this.m2 += delta * (x - this.mean);
  }

  private removeSample(x: number): void {
    if (this.n <= 1) {
      this.n = 0;
      this.mean = 0;
      this.m2 = 0;
      return;
    }
    const newCount = this.n - 1;
    const newMean = (this.n * this.mean - x) / newCount;
    this.m2 -= (x - this.mean) * (x - newMean);
    if (this.m2 < 0) this.m2 = 0;
    this.mean = newMean;
    this.n = newCount;
  }

  private recompute(): void {
    this.n = 0;
    this.mean = 0;
    this.m2 = 0;
    for (const x of this.buffer) this.addSample(x);
    this.opsSinceRecompute = 0;
  }
}

export interface EwmaSnapshot {
  readonly count: number;
  readonly mean: number;
  readonly variance: number;
  readonly stdDev: number;
}

/** Exponentially weighted moving mean and variance (West's incremental formulation). */
export class Ewma {
  private n = 0;
  private mean = 0;
  private variance = 0;

  constructor(public readonly alpha: number) {
    if (!(alpha > 0 && alpha <= 1)) {
      throw new ConfigError('EWMA alpha must be in the interval (0, 1]', { alpha });
    }
  }

  update(x: number): void {
    if (!Number.isFinite(x)) {
      throw new ValidationError('Ewma only accepts finite numbers', { value: String(x) });
    }
    if (this.n === 0) {
      this.mean = x;
      this.variance = 0;
    } else {
      const diff = x - this.mean;
      const increment = this.alpha * diff;
      this.mean += increment;
      this.variance = (1 - this.alpha) * (this.variance + diff * increment);
    }
    this.n++;
  }

  snapshot(): EwmaSnapshot {
    return {
      count: this.n,
      mean: this.mean,
      variance: this.variance,
      stdDev: Math.sqrt(this.variance),
    };
  }
}

/** Bounded set of recently seen ids with FIFO eviction. */
class DedupCache {
  private readonly ids = new Set<string>();
  private readonly order: RingBuffer<string>;

  constructor(capacity: number) {
    this.order = new RingBuffer<string>(capacity);
  }

  has(id: string): boolean {
    return this.ids.has(id);
  }

  add(id: string): void {
    if (this.ids.has(id)) return;
    const evicted = this.order.push(id);
    if (evicted !== undefined) this.ids.delete(evicted);
    this.ids.add(id);
  }
}

// ---------------------------------------------------------------------------
// Detectors
// ---------------------------------------------------------------------------

/** Read-only view handed to detectors. Statistics reflect the state *before* this event. */
export interface DetectionContext {
  readonly event: TelemetryEvent;
  readonly seriesKey: string;
  readonly window: WindowSnapshot;
  readonly ewma: EwmaSnapshot;
  /** Number of events previously accepted for this series. */
  readonly sampleCount: number;
}

export interface DetectorFinding {
  readonly severity: Severity;
  readonly score: number;
  readonly expected: number | null;
  readonly message: string;
}

/** A detector must be a pure function of its context (no hidden per-series state). */
export interface AnomalyDetector {
  readonly name: string;
  evaluate(context: DetectionContext): DetectorFinding | null;
}

const MAX_SCORE = 1e9;

const fmt = (n: number): string => (Number.isFinite(n) ? String(Number(n.toPrecision(6))) : String(n));

export interface SigmaDetectorOptions {
  /** Sigma distance that raises a `warning`. Default 3. */
  readonly threshold?: number;
  /** Sigma distance that raises a `critical`. Default 5. */
  readonly criticalThreshold?: number;
  /** Minimum baseline samples before the detector may fire. Default 30. */
  readonly minSamples?: number;
  /** Absolute lower bound for the standard deviation used as divisor. Default 1e-9. */
  readonly absoluteStdDevFloor?: number;
  /** Lower bound relative to |mean| for the divisor, guarding flat series. Default 1e-6. */
  readonly relativeStdDevFloor?: number;
}

type ResolvedSigmaOptions = Required<SigmaDetectorOptions>;

function resolveSigmaOptions(detector: string, o: SigmaDetectorOptions): ResolvedSigmaOptions {
  const resolved: ResolvedSigmaOptions = {
    threshold: o.threshold ?? 3,
    criticalThreshold: o.criticalThreshold ?? 5,
    minSamples: o.minSamples ?? 30,
    absoluteStdDevFloor: o.absoluteStdDevFloor ?? 1e-9,
    relativeStdDevFloor: o.relativeStdDevFloor ?? 1e-6,
  };
  if (!(Number.isFinite(resolved.threshold) && resolved.threshold > 0)) {
    throw new ConfigError(`${detector}: threshold must be a positive number`, { threshold: o.threshold });
  }
  if (!(Number.isFinite(resolved.criticalThreshold) && resolved.criticalThreshold >= resolved.threshold)) {
    throw new ConfigError(`${detector}: criticalThreshold must be >= threshold`, {
      threshold: resolved.threshold,
      criticalThreshold: o.criticalThreshold,
    });
  }
  if (!Number.isInteger(resolved.minSamples) || resolved.minSamples < 2) {
    throw new ConfigError(`${detector}: minSamples must be an integer >= 2`, {
      minSamples: o.minSamples,
    });
  }
  if (!(resolved.absoluteStdDevFloor >= 0) || !(resolved.relativeStdDevFloor >= 0)) {
    throw new ConfigError(`${detector}: std-dev floors must be >= 0`);
  }
  return resolved;
}

function sigmaFinding(
  label: string,
  value: number,
  mean: number,
  stdDev: number,
  o: ResolvedSigmaOptions,
): DetectorFinding | null {
  const deviation = Math.abs(value - mean);
  const floor = Math.max(o.absoluteStdDevFloor, Math.abs(mean) * o.relativeStdDevFloor);
  const divisor = Math.max(stdDev, floor, Number.MIN_VALUE);
  const score = Math.min(deviation / divisor, MAX_SCORE);
  if (score < o.threshold) return null;
  return {
    severity: score >= o.criticalThreshold ? 'critical' : 'warning',
    score,
    expected: mean,
    message: `value ${fmt(value)} deviates ${fmt(score)} sigma from ${label} mean ${fmt(mean)} (sigma=${fmt(stdDev)})`,
  };
}

/** Flags values far from the rolling-window mean, measured in standard deviations. */
export class ZScoreDetector implements AnomalyDetector {
  public readonly name = 'zscore';
  private readonly options: ResolvedSigmaOptions;

  constructor(options: SigmaDetectorOptions = {}) {
    this.options = resolveSigmaOptions('zscore', options);
  }

  evaluate(ctx: DetectionContext): DetectorFinding | null {
    if (ctx.window.count < this.options.minSamples) return null;
    return sigmaFinding('rolling', ctx.event.value, ctx.window.mean, ctx.window.stdDev, this.options);
  }
}

/** Like ZScoreDetector but against an exponentially weighted baseline; adapts faster to drift. */
export class EwmaDetector implements AnomalyDetector {
  public readonly name = 'ewma';
  private readonly options: ResolvedSigmaOptions;

  constructor(options: SigmaDetectorOptions = {}) {
    this.options = resolveSigmaOptions('ewma', options);
  }

  evaluate(ctx: DetectionContext): DetectorFinding | null {
    if (ctx.ewma.count < this.options.minSamples) return null;
    return sigmaFinding('EWMA', ctx.event.value, ctx.ewma.mean, ctx.ewma.stdDev, this.options);
  }
}

export interface ThresholdDetectorOptions {
  /** Detector name; override to run several threshold detectors side by side. Default "threshold". */
  readonly name?: string;
  readonly min?: number;
  readonly max?: number;
  /** Severity of a violation. Default "critical". */
  readonly severity?: Severity;
}

/** Static bounds check. Needs no baseline, so it fires from the very first sample. */
export class ThresholdDetector implements AnomalyDetector {
  public readonly name: string;
  private readonly min: number | undefined;
  private readonly max: number | undefined;
  private readonly severity: Severity;

  constructor(options: ThresholdDetectorOptions) {
    const { min, max } = options;
    if (min === undefined && max === undefined) {
      throw new ConfigError('ThresholdDetector requires at least one of min or max');
    }
    if ((min !== undefined && !Number.isFinite(min)) || (max !== undefined && !Number.isFinite(max))) {
      throw new ConfigError('ThresholdDetector bounds must be finite numbers', { min, max });
    }
    if (min !== undefined && max !== undefined && min > max) {
      throw new ConfigError('ThresholdDetector min must be <= max', { min, max });
    }
    if (options.severity !== undefined && !(options.severity in SEVERITY_RANK)) {
      throw new ConfigError('ThresholdDetector severity is invalid', { severity: options.severity });
    }
    if (options.name !== undefined && options.name.length === 0) {
      throw new ConfigError('ThresholdDetector name must not be empty');
    }
    this.name = options.name ?? 'threshold';
    this.min = min;
    this.max = max;
    this.severity = options.severity ?? 'critical';
  }

  evaluate(ctx: DetectionContext): DetectorFinding | null {
    const v = ctx.event.value;
    if (this.max !== undefined && v > this.max) {
      return {
        severity: this.severity,
        score: Math.min((v - this.max) / Math.max(Math.abs(this.max), 1), MAX_SCORE),
        expected: this.max,
        message: `value ${fmt(v)} exceeds maximum ${fmt(this.max)}`,
      };
    }
    if (this.min !== undefined && v < this.min) {
      return {
        severity: this.severity,
        score: Math.min((this.min - v) / Math.max(Math.abs(this.min), 1), MAX_SCORE),
        expected: this.min,
        message: `value ${fmt(v)} is below minimum ${fmt(this.min)}`,
      };
    }
    return null;
  }
}

// ---------------------------------------------------------------------------
// Engine configuration
// ---------------------------------------------------------------------------

/**
 * What to do when the queue is full:
 *  - `drop-oldest`: evict the oldest queued event to admit the new one (freshest data wins).
 *  - `drop-newest`: refuse the new event and report `queue-full` in the result.
 *  - `reject`: throw `BackpressureError` so the caller (e.g. an HTTP layer) can signal 429.
 */
export type BackpressurePolicy = 'drop-oldest' | 'drop-newest' | 'reject';

export interface EngineConfig {
  /** Rolling window length per series. Default 256. */
  readonly windowSize: number;
  /** Maximum tracked series; least-recently-used series are evicted. Default 10 000. */
  readonly maxSeries: number;
  /** Bounded ingest queue length. Default 50 000. */
  readonly queueCapacity: number;
  readonly backpressure: BackpressurePolicy;
  /** Events older than (series watermark - this) are dropped as late. Default 5 000 ms. */
  readonly allowedLatenessMs: number;
  /** Reject events further than this into the future. Default 60 000 ms. */
  readonly maxFutureSkewMs: number;
  /** Recent event ids remembered per series for idempotency. Default 1 024. */
  readonly dedupCapacity: number;
  /** Minimum event-time gap between repeated alerts of equal severity from one detector. Default 30 000 ms. */
  readonly anomalyCooldownMs: number;
  /** Background flush period used by `start()`. Default 50 ms. */
  readonly flushIntervalMs: number;
  /** Max events processed per background flush. Default 5 000. */
  readonly maxBatchPerFlush: number;
  /** Smoothing factor of the per-series EWMA baseline. Default 0.1. */
  readonly ewmaAlpha: number;
  /**
   * Consecutive anomalous samples tolerated before the baseline starts absorbing
   * them (a sustained level shift is treated as the new normal). Default 5.
   */
  readonly baselineAdaptationThreshold: number;
  /** Time source in epoch ms. Default `Date.now`. */
  readonly clock: () => number;
}

export const DEFAULT_ENGINE_CONFIG: Readonly<EngineConfig> = Object.freeze({
  windowSize: 256,
  maxSeries: 10_000,
  queueCapacity: 50_000,
  backpressure: 'drop-oldest',
  allowedLatenessMs: 5_000,
  maxFutureSkewMs: DEFAULT_MAX_FUTURE_SKEW_MS,
  dedupCapacity: 1_024,
  anomalyCooldownMs: 30_000,
  flushIntervalMs: 50,
  maxBatchPerFlush: 5_000,
  ewmaAlpha: 0.1,
  baselineAdaptationThreshold: 5,
  clock: Date.now,
});

/** Merges overrides over defaults (ignoring `undefined`) and validates every field. */
export function resolveConfig(overrides: Partial<EngineConfig> = {}): EngineConfig {
  const merged: Record<string, unknown> = { ...DEFAULT_ENGINE_CONFIG };
  for (const [key, val] of Object.entries(overrides)) {
    if (val !== undefined) merged[key] = val;
  }
  const cfg = merged as unknown as EngineConfig;

  const int = (name: keyof EngineConfig, min: number): void => {
    const v = cfg[name];
    if (typeof v !== 'number' || !Number.isInteger(v) || v < min) {
      throw new ConfigError(`${String(name)} must be an integer >= ${min}`, { [name]: v });
    }
  };
  const nonNegative = (name: keyof EngineConfig): void => {
    const v = cfg[name];
    if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) {
      throw new ConfigError(`${String(name)} must be a finite number >= 0`, { [name]: v });
    }
  };

  int('windowSize', 2);
  int('maxSeries', 1);
  int('queueCapacity', 1);
  int('dedupCapacity', 1);
  int('flushIntervalMs', 1);
  int('maxBatchPerFlush', 1);
  int('baselineAdaptationThreshold', 0);
  nonNegative('allowedLatenessMs');
  nonNegative('maxFutureSkewMs');
  nonNegative('anomalyCooldownMs');

  if (!(typeof cfg.ewmaAlpha === 'number' && cfg.ewmaAlpha > 0 && cfg.ewmaAlpha <= 1)) {
    throw new ConfigError('ewmaAlpha must be in the interval (0, 1]', { ewmaAlpha: cfg.ewmaAlpha });
  }
  if (!['drop-oldest', 'drop-newest', 'reject'].includes(cfg.backpressure)) {
    throw new ConfigError('backpressure must be drop-oldest, drop-newest or reject', {
      backpressure: cfg.backpressure,
    });
  }
  if (typeof cfg.clock !== 'function') {
    throw new ConfigError('clock must be a function returning epoch milliseconds');
  }
  return Object.freeze(cfg);
}

// ---------------------------------------------------------------------------
// Engine events & results
// ---------------------------------------------------------------------------

export interface DroppedEvent {
  readonly reason: DropReason;
  readonly eventId?: string;
  readonly seriesKey?: string;
  readonly detail?: string;
}

export interface EngineErrorEvent {
  readonly phase: 'detector' | 'listener' | 'internal';
  readonly error: Error;
  readonly detector?: string;
  readonly eventId?: string;
  /** For `listener` errors: the engine event whose listener threw. */
  readonly listenerEvent?: string;
}

export interface EngineEvents {
  anomaly: Anomaly;
  dropped: DroppedEvent;
  error: EngineErrorEvent;
}

export type SubmitResult =
  | { readonly accepted: true }
  | { readonly accepted: false; readonly reason: DropReason; readonly error?: ValidationError };

export interface BatchResult {
  readonly total: number;
  readonly accepted: number;
  readonly rejected: number;
  readonly rejectedByReason: Readonly<Partial<Record<DropReason, number>>>;
}

export type EngineState = 'idle' | 'running' | 'stopped';

export interface EngineMetrics {
  readonly state: EngineState;
  readonly submitted: number;
  readonly accepted: number;
  readonly rejectedInvalid: number;
  readonly droppedQueueFull: number;
  readonly droppedLate: number;
  readonly droppedDuplicate: number;
  readonly processed: number;
  readonly anomaliesDetected: number;
  readonly anomaliesEmitted: number;
  readonly anomaliesSuppressed: number;
  readonly detectorErrors: number;
  readonly listenerErrors: number;
  readonly flushes: number;
  readonly seriesActive: number;
  readonly seriesEvicted: number;
  readonly queueDepth: number;
}

export interface SeriesSnapshot {
  readonly key: string;
  readonly source: string;
  readonly metric: string;
  readonly processed: number;
  readonly watermark: number;
  readonly consecutiveAnomalies: number;
  readonly window: WindowSnapshot;
  readonly ewma: EwmaSnapshot;
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

interface AlertRecord {
  at: number;
  rank: number;
}

interface SeriesState {
  readonly key: string;
  readonly source: string;
  readonly metric: string;
  readonly window: RollingStats;
  readonly ewma: Ewma;
  readonly dedup: DedupCache;
  readonly lastAlerts: Map<string, AlertRecord>;
  watermark: number;
  consecutiveAnomalies: number;
  processed: number;
}

type Listener<T> = (payload: T) => void;

const toError = (err: unknown): Error => (err instanceof Error ? err : new Error(String(err)));

/**
 * The stream processing engine. Lifecycle: `idle` (manual `flush()`), `running`
 * (after `start()`, background flushing) and `stopped` (after `stop()`; terminal).
 */
export class StreamEngine {
  private readonly config: EngineConfig;
  private readonly detectors: readonly AnomalyDetector[];
  private readonly queue: RingBuffer<TelemetryEvent>;
  private readonly series = new Map<string, SeriesState>();
  private readonly listeners = new Map<keyof EngineEvents, Set<Listener<unknown>>>();
  private state: EngineState = 'idle';
  private timer: ReturnType<typeof setTimeout> | undefined;
  private flushing = false;

  private readonly counters = {
    submitted: 0,
    accepted: 0,
    rejectedInvalid: 0,
    droppedQueueFull: 0,
    droppedLate: 0,
    droppedDuplicate: 0,
    processed: 0,
    anomaliesDetected: 0,
    anomaliesEmitted: 0,
    anomaliesSuppressed: 0,
    detectorErrors: 0,
    listenerErrors: 0,
    flushes: 0,
    seriesEvicted: 0,
  };

  /**
   * @param detectors At least one detector; names must be unique.
   * @param config Partial overrides of {@link DEFAULT_ENGINE_CONFIG}.
   * @throws ConfigError on invalid configuration or detector set.
   */
  constructor(detectors: readonly AnomalyDetector[], config: Partial<EngineConfig> = {}) {
    this.config = resolveConfig(config);

    if (!Array.isArray(detectors) || detectors.length === 0) {
      throw new ConfigError('StreamEngine requires at least one detector');
    }
    const names = new Set<string>();
    for (const d of detectors) {
      if (!d || typeof d.name !== 'string' || d.name.length === 0 || typeof d.evaluate !== 'function') {
        throw new ConfigError('Every detector needs a non-empty name and an evaluate() function');
      }
      if (names.has(d.name)) {
        throw new ConfigError(`Duplicate detector name "${d.name}"`, { name: d.name });
      }
      names.add(d.name);
    }
    this.detectors = Object.freeze([...detectors]);
    this.queue = new RingBuffer<TelemetryEvent>(this.config.queueCapacity);
  }

  // -- subscriptions --------------------------------------------------------

  /** Subscribes to an engine event. Returns an unsubscribe function. */
  on<K extends keyof EngineEvents>(event: K, listener: Listener<EngineEvents[K]>): () => void {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    const fn = listener as Listener<unknown>;
    set.add(fn);
    return () => {
      set.delete(fn);
    };
  }

  // -- ingestion ------------------------------------------------------------

  /**
   * Validates and enqueues one event. Never throws for bad input; invalid events
   * are reported via the result and the `dropped` event.
   *
   * @throws BackpressureError only when the queue is full and policy is `reject`.
   */
  submit(raw: unknown): SubmitResult {
    this.counters.submitted++;

    if (this.state === 'stopped') {
      this.emit('dropped', { reason: 'shutdown', eventId: extractId(raw) });
      return { accepted: false, reason: 'shutdown' };
    }

    let event: TelemetryEvent;
    try {
      event = validateEvent(raw, {
        nowMs: this.config.clock(),
        maxFutureSkewMs: this.config.maxFutureSkewMs,
      });
    } catch (err) {
      if (err instanceof ValidationError) {
        this.counters.rejectedInvalid++;
        this.emit('dropped', { reason: 'invalid', eventId: extractId(raw), detail: err.message });
        return { accepted: false, reason: 'invalid', error: err };
      }
      throw err;
    }

    if (this.queue.isFull) {
      switch (this.config.backpressure) {
        case 'reject':
          this.counters.droppedQueueFull++;
          this.emit('dropped', { reason: 'queue-full', eventId: event.id, detail: 'policy=reject' });
          throw new BackpressureError('Ingest queue is full', {
            capacity: this.config.queueCapacity,
            eventId: event.id,
          });
        case 'drop-newest':
          this.counters.droppedQueueFull++;
          this.emit('dropped', { reason: 'queue-full', eventId: event.id, detail: 'policy=drop-newest' });
          return { accepted: false, reason: 'queue-full' };
        case 'drop-oldest': {
          const evicted = this.queue.push(event);
          this.counters.droppedQueueFull++;
          this.counters.accepted++;
          this.emit('dropped', {
            reason: 'queue-full',
            ...(evicted ? { eventId: evicted.id } : {}),
            detail: 'policy=drop-oldest (evicted from queue)',
          });
          return { accepted: true };
        }
      }
    }

    this.queue.push(event);
    this.counters.accepted++;
    return { accepted: true };
  }

  /** Submits many events. Backpressure rejections are tallied instead of thrown. */
  submitBatch(raws: Iterable<unknown>): BatchResult {
    let total = 0;
    let accepted = 0;
    const byReason: Partial<Record<DropReason, number>> = {};
    const reject = (reason: DropReason): void => {
      byReason[reason] = (byReason[reason] ?? 0) + 1;
    };

    for (const raw of raws) {
      total++;
      try {
        const result = this.submit(raw);
        if (result.accepted) accepted++;
        else reject(result.reason);
      } catch (err) {
        if (err instanceof BackpressureError) reject('queue-full');
        else throw err;
      }
    }
    return { total, accepted, rejected: total - accepted, rejectedByReason: byReason };
  }

  // -- processing -----------------------------------------------------------

  /**
   * Processes up to `maxEvents` queued events synchronously.
   * Re-entrant calls (e.g. from a listener) return 0 immediately.
   *
   * @returns the number of events taken off the queue.
   */
  flush(maxEvents: number = Number.POSITIVE_INFINITY): number {
    if (!(maxEvents >= 0)) {
      throw new ConfigError('maxEvents must be a number >= 0', { maxEvents });
    }
    if (this.flushing) return 0;

    this.flushing = true;
    let taken = 0;
    try {
      while (taken < maxEvents) {
        const event = this.queue.shift();
        if (event === undefined) break;
        taken++;
        try {
          this.processEvent(event);
        } catch (err) {
          this.emit('error', { phase: 'internal', error: toError(err), eventId: event.id });
        }
      }
      this.counters.flushes++;
    } finally {
      this.flushing = false;
    }
    return taken;
  }

  /** Starts the background flush loop. Idempotent while running. */
  start(): void {
    if (this.state === 'stopped') {
      throw new EngineStateError('A stopped engine cannot be restarted; create a new instance');
    }
    if (this.state === 'running') return;
    this.state = 'running';
    this.scheduleNext();
  }

  /**
   * Stops the background loop, drains every queued event, and moves to the
   * terminal `stopped` state. Idempotent.
   *
   * @returns the number of events drained.
   */
  stop(): number {
    if (this.state === 'stopped') return 0;
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    // Drain before flipping state so listeners that submit follow-ups are still served.
    let drained = 0;
    while (this.queue.size > 0) {
      const n = this.flush(this.config.maxBatchPerFlush);
      drained += n;
      if (n === 0) break; // re-entrant stop() from a listener; the outer flush finishes the job
    }
    this.state = 'stopped';
    return drained;
  }

  // -- introspection --------------------------------------------------------

  metrics(): EngineMetrics {
    return {
      ...this.counters,
      state: this.state,
      seriesActive: this.series.size,
      queueDepth: this.queue.size,
    };
  }

  /** Statistical snapshot of a series, or undefined if unknown. Does not affect LRU order. */
  getSeriesSnapshot(source: string, metric: string): SeriesSnapshot | undefined {
    const s = this.series.get(seriesKeyOf(source, metric));
    if (!s) return undefined;
    return {
      key: s.key,
      source: s.source,
      metric: s.metric,
      processed: s.processed,
      watermark: s.watermark,
      consecutiveAnomalies: s.consecutiveAnomalies,
      window: s.window.snapshot(),
      ewma: s.ewma.snapshot(),
    };
  }

  // -- internals ------------------------------------------------------------

  private scheduleNext(): void {
    const timer = setTimeout(() => {
      this.timer = undefined;
      try {
        this.flush(this.config.maxBatchPerFlush);
      } finally {
        if (this.state === 'running') this.scheduleNext();
      }
    }, this.config.flushIntervalMs);
    (timer as { unref?: () => void }).unref?.();
    this.timer = timer;
  }

  private acquireSeries(event: TelemetryEvent): SeriesState {
    const key = seriesKeyOf(event.source, event.metric);
    const existing = this.series.get(key);
    if (existing) {
      // Refresh recency: Map iteration order is insertion order.
      this.series.delete(key);
      this.series.set(key, existing);
      return existing;
    }

    const created: SeriesState = {
      key,
      source: event.source,
      metric: event.metric,
      window: new RollingStats(this.config.windowSize),
      ewma: new Ewma(this.config.ewmaAlpha),
      dedup: new DedupCache(this.config.dedupCapacity),
      lastAlerts: new Map(),
      watermark: Number.NEGATIVE_INFINITY,
      consecutiveAnomalies: 0,
      processed: 0,
    };
    this.series.set(key, created);

    if (this.series.size > this.config.maxSeries) {
      const oldest = this.series.keys().next().value as string;
      this.series.delete(oldest);
      this.counters.seriesEvicted++;
    }
    return created;
  }

  private processEvent(event: TelemetryEvent): void {
    const series = this.acquireSeries(event);

    if (event.timestamp < series.watermark - this.config.allowedLatenessMs) {
      this.counters.droppedLate++;
      this.emit('dropped', {
        reason: 'late',
        eventId: event.id,
        seriesKey: series.key,
        detail: `timestamp ${event.timestamp} is older than watermark ${series.watermark} minus ${this.config.allowedLatenessMs}ms`,
      });
      return;
    }

    if (series.dedup.has(event.id)) {
      this.counters.droppedDuplicate++;
      this.emit('dropped', { reason: 'duplicate', eventId: event.id, seriesKey: series.key });
      return;
    }
    series.dedup.add(event.id);
    if (event.timestamp > series.watermark) series.watermark = event.timestamp;

    const context: DetectionContext = {
      event,
      seriesKey: series.key,
      window: series.window.snapshot(),
      ewma: series.ewma.snapshot(),
      sampleCount: series.processed,
    };
    const findings = this.runDetectors(context);

    // Update baseline *after* evaluation so a sample never masks itself. Anomalous
    // samples are withheld until a sustained shift is recognised (see config docs).
    if (findings.length > 0) series.consecutiveAnomalies++;
    else series.consecutiveAnomalies = 0;
    const absorb =
      findings.length === 0 || series.consecutiveAnomalies > this.config.baselineAdaptationThreshold;
    if (absorb) {
      series.window.add(event.value);
      series.ewma.update(event.value);
    }
    series.processed++;
    this.counters.processed++;

    for (const { detector, finding } of findings) {
      this.counters.anomaliesDetected++;
      if (!this.passesCooldown(series, detector.name, finding.severity, event.timestamp)) {
        this.counters.anomaliesSuppressed++;
        continue;
      }
      this.counters.anomaliesEmitted++;
      this.emit(
        'anomaly',
        Object.freeze({
          eventId: event.id,
          seriesKey: series.key,
          source: event.source,
          metric: event.metric,
          detector: detector.name,
          severity: finding.severity,
          value: event.value,
          expected: finding.expected,
          score: finding.score,
          timestamp: event.timestamp,
          message: finding.message,
        }),
      );
    }
  }

  private runDetectors(
    context: DetectionContext,
  ): Array<{ detector: AnomalyDetector; finding: DetectorFinding }> {
    const results: Array<{ detector: AnomalyDetector; finding: DetectorFinding }> = [];
    for (const detector of this.detectors) {
      try {
        const finding = detector.evaluate(context);
        if (finding) {
          if (!(finding.severity in SEVERITY_RANK) || !Number.isFinite(finding.score)) {
            throw new ValidationError(`Detector "${detector.name}" returned a malformed finding`, {
              severity: String(finding.severity),
              score: String(finding.score),
            });
          }
          results.push({ detector, finding });
        }
      } catch (err) {
        this.counters.detectorErrors++;
        this.emit('error', {
          phase: 'detector',
          error: toError(err),
          detector: detector.name,
          eventId: context.event.id,
        });
      }
    }
    return results;
  }

  /**
   * Suppresses repeats of the same-or-lower severity from one detector within the
   * cooldown window; an escalation to a higher severity always passes.
   */
  private passesCooldown(
    series: SeriesState,
    detectorName: string,
    severity: Severity,
    timestamp: number,
  ): boolean {
    const rank = SEVERITY_RANK[severity];
    const last = series.lastAlerts.get(detectorName);
    const cooldown = this.config.anomalyCooldownMs;
    if (cooldown > 0 && last !== undefined && timestamp - last.at < cooldown && rank <= last.rank) {
      return false;
    }
    series.lastAlerts.set(detectorName, { at: timestamp, rank });
    return true;
  }

  private emit<K extends keyof EngineEvents>(event: K, payload: EngineEvents[K]): void {
    const set = this.listeners.get(event);
    if (!set || set.size === 0) return;
    for (const listener of [...set]) {
      try {
        listener(payload);
      } catch (err) {
        this.counters.listenerErrors++;
        // Errors thrown by 'error' listeners are swallowed to avoid infinite recursion.
        if (event !== 'error') {
          this.emit('error', { phase: 'listener', error: toError(err), listenerEvent: event });
        }
      }
    }
  }
}

function extractId(raw: unknown): string | undefined {
  if (typeof raw === 'object' && raw !== null && !Array.isArray(raw)) {
    const id = (raw as Record<string, unknown>).id;
    if (typeof id === 'string') return id.slice(0, MAX_ID_LENGTH);
  }
  return undefined;
}

/** Convenience factory: z-score + EWMA detectors with sensible defaults. */
export function createDefaultEngine(config: Partial<EngineConfig> = {}): StreamEngine {
  return new StreamEngine([new ZScoreDetector(), new EwmaDetector()], config);
}
