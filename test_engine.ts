import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  BackpressureError,
  ConfigError,
  DEFAULT_ENGINE_CONFIG,
  EngineStateError,
  EwmaDetector,
  Ewma,
  MIN_TIMESTAMP_MS,
  NexusError,
  RingBuffer,
  RollingStats,
  StreamEngine,
  ThresholdDetector,
  ValidationError,
  ZScoreDetector,
  createDefaultEngine,
  resolveConfig,
  seriesKeyOf,
  validateEvent,
} from '../src/core/engine';
import type {
  Anomaly,
  AnomalyDetector,
  DetectionContext,
  DroppedEvent,
  EngineConfig,
  EngineErrorEvent,
  TelemetryEvent,
} from '../src/core/engine';

// ---------------------------------------------------------------------------
// Mock generators
// ---------------------------------------------------------------------------

const T0 = 1_760_000_000_000;
const NOW = T0 + 3_600_000;
const fixedClock = (): number => NOW;

/** Deterministic PRNG (mulberry32) so statistical tests never flake. */
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

/** Gaussian samples via Box-Muller. */
function gaussianSeries(n: number, mean: number, sd: number, seed: number): number[] {
  const rand = mulberry32(seed);
  const out: number[] = [];
  while (out.length < n) {
    const u = Math.max(rand(), 1e-12);
    const v = rand();
    const mag = Math.sqrt(-2 * Math.log(u));
    out.push(mean + sd * mag * Math.cos(2 * Math.PI * v));
    if (out.length < n) out.push(mean + sd * mag * Math.sin(2 * Math.PI * v));
  }
  return out;
}

let idSeq = 0;
function makeEvent(overrides: Partial<TelemetryEvent> = {}): TelemetryEvent {
  idSeq++;
  return {
    id: `evt-${idSeq}`,
    source: 'host-1',
    metric: 'cpu.load',
    value: 1,
    timestamp: T0,
    ...overrides,
  };
}

interface FeedOptions {
  readonly source?: string;
  readonly metric?: string;
  readonly startTs?: number;
  readonly stepMs?: number;
  readonly idPrefix?: string;
}

/** Submits values as events one second apart, then flushes. Returns the next free timestamp. */
function feed(engine: StreamEngine, values: readonly number[], opts: FeedOptions = {}): number {
  const { source = 'host-1', metric = 'cpu.load', startTs = T0, stepMs = 1_000 } = opts;
  const prefix = opts.idPrefix ?? `${source}-${metric}-${startTs}`;
  let ts = startTs;
  values.forEach((value, i) => {
    engine.submit({ id: `${prefix}-${i}`, source, metric, value, timestamp: ts });
    ts += stepMs;
  });
  engine.flush();
  return ts;
}

function collect(engine: StreamEngine) {
  const anomalies: Anomaly[] = [];
  const dropped: DroppedEvent[] = [];
  const errors: EngineErrorEvent[] = [];
  engine.on('anomaly', (a) => anomalies.push(a));
  engine.on('dropped', (d) => dropped.push(d));
  engine.on('error', (e) => errors.push(e));
  return { anomalies, dropped, errors };
}

/** Low-noise baseline (std ~1) that can never reach 3 sigma; keeps wiring tests deterministic. */
function alternatingSeries(n: number, low = 99, high = 101): number[] {
  return Array.from({ length: n }, (_, i) => (i % 2 === 0 ? low : high));
}

function naiveStats(values: readonly number[]): { mean: number; variance: number } {
  const n = values.length;
  const mean = values.reduce((s, x) => s + x, 0) / n;
  const variance = n < 2 ? 0 : values.reduce((s, x) => s + (x - mean) ** 2, 0) / (n - 1);
  return { mean, variance };
}

const baseConfig = (extra: Partial<EngineConfig> = {}): Partial<EngineConfig> => ({
  clock: fixedClock,
  ...extra,
});

/** Detector that flags value > 10 and records every event id it sees. */
function recordingDetector(seen: string[] = []): AnomalyDetector & { seen: string[] } {
  return {
    name: 'recorder',
    seen,
    evaluate(ctx: DetectionContext) {
      seen.push(ctx.event.id);
      return null;
    },
  };
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

describe('error classes', () => {
  it('carry code, name, frozen details and a working prototype chain', () => {
    const err = new ValidationError('bad', { field: 'x' });
    expect(err).toBeInstanceOf(ValidationError);
    expect(err).toBeInstanceOf(NexusError);
    expect(err).toBeInstanceOf(Error);
    expect(err.code).toBe('VALIDATION_ERROR');
    expect(err.name).toBe('ValidationError');
    expect(err.details).toEqual({ field: 'x' });
    expect(Object.isFrozen(err.details)).toBe(true);
  });

  it('assigns a distinct code to each subclass', () => {
    expect(new ConfigError('x').code).toBe('CONFIG_ERROR');
    expect(new EngineStateError('x').code).toBe('ENGINE_STATE_ERROR');
    expect(new BackpressureError('x').code).toBe('BACKPRESSURE');
  });
});

// ---------------------------------------------------------------------------
// validateEvent
// ---------------------------------------------------------------------------

describe('validateEvent', () => {
  const valid = {
    id: 'abc',
    source: 'host-1',
    metric: 'cpu.load',
    value: 0.5,
    timestamp: T0,
    tags: { region: 'eu-west' },
  };

  it('returns a frozen, normalised copy of a valid event', () => {
    const ev = validateEvent(valid, { nowMs: NOW });
    expect(ev).toEqual(valid);
    expect(ev).not.toBe(valid);
    expect(Object.isFrozen(ev)).toBe(true);
    expect(Object.isFrozen(ev.tags)).toBe(true);
  });

  it('omits tags when none are supplied', () => {
    const { tags: _tags, ...noTags } = valid;
    const ev = validateEvent(noTags, { nowMs: NOW });
    expect('tags' in ev).toBe(false);
  });

  it('ignores unknown extra fields', () => {
    const ev = validateEvent({ ...valid, extra: 'ignored' }, { nowMs: NOW });
    expect('extra' in ev).toBe(false);
  });

  it.each([null, undefined, 'str', 42, true, [], [valid]])('rejects non-object input %j', (input) => {
    expect(() => validateEvent(input, { nowMs: NOW })).toThrow(ValidationError);
  });

  it('reports every problem at once', () => {
    try {
      validateEvent({}, { nowMs: NOW });
      expect.unreachable();
    } catch (err) {
      expect(err).toBeInstanceOf(ValidationError);
      const issues = (err as ValidationError).details.issues as string[];
      expect(issues).toHaveLength(5);
      expect(issues.join('|')).toMatch(/id/);
      expect(issues.join('|')).toMatch(/source/);
      expect(issues.join('|')).toMatch(/metric/);
      expect(issues.join('|')).toMatch(/value/);
      expect(issues.join('|')).toMatch(/timestamp/);
    }
  });

  it.each([NaN, Infinity, -Infinity, '5', null, undefined, {}])('rejects value %j', (value) => {
    expect(() => validateEvent({ ...valid, value }, { nowMs: NOW })).toThrow(/value/);
  });

  it('accepts zero and negative values', () => {
    expect(validateEvent({ ...valid, value: 0 }, { nowMs: NOW }).value).toBe(0);
    expect(validateEvent({ ...valid, value: -273.15 }, { nowMs: NOW }).value).toBe(-273.15);
  });

  it.each(['', 'has space', 'pipe|char', '-leading-dash', 'a'.repeat(257), 5])(
    'rejects invalid source/metric %j',
    (name) => {
      expect(() => validateEvent({ ...valid, source: name }, { nowMs: NOW })).toThrow(/source/);
      expect(() => validateEvent({ ...valid, metric: name }, { nowMs: NOW })).toThrow(/metric/);
    },
  );

  it.each(['a', 'host-1', 'svc.api/v2:latency_ms'.replace('_', '-')])('accepts name %s', (name) => {
    expect(() => validateEvent({ ...valid, source: name, metric: name }, { nowMs: NOW })).not.toThrow();
  });

  it('rejects empty or over-long ids', () => {
    expect(() => validateEvent({ ...valid, id: '' }, { nowMs: NOW })).toThrow(/id/);
    expect(() => validateEvent({ ...valid, id: 'x'.repeat(129) }, { nowMs: NOW })).toThrow(/id/);
    expect(validateEvent({ ...valid, id: 'x'.repeat(128) }, { nowMs: NOW }).id).toHaveLength(128);
  });

  it('rejects fractional, seconds-based and non-numeric timestamps', () => {
    expect(() => validateEvent({ ...valid, timestamp: T0 + 0.5 }, { nowMs: NOW })).toThrow(/timestamp/);
    expect(() => validateEvent({ ...valid, timestamp: 1_760_000_000 }, { nowMs: NOW })).toThrow(/seconds/);
    expect(() => validateEvent({ ...valid, timestamp: MIN_TIMESTAMP_MS - 1 }, { nowMs: NOW })).toThrow(
      /timestamp/,
    );
    expect(() => validateEvent({ ...valid, timestamp: '1760000000000' }, { nowMs: NOW })).toThrow(/timestamp/);
    expect(() => validateEvent({ ...valid, timestamp: NaN }, { nowMs: NOW })).toThrow(/timestamp/);
  });

  it('enforces the future-skew boundary exactly', () => {
    const skew = 1_000;
    expect(() =>
      validateEvent({ ...valid, timestamp: NOW + skew }, { nowMs: NOW, maxFutureSkewMs: skew }),
    ).not.toThrow();
    expect(() =>
      validateEvent({ ...valid, timestamp: NOW + skew + 1 }, { nowMs: NOW, maxFutureSkewMs: skew }),
    ).toThrow(/future/);
  });

  it('falls back to Date.now() when nowMs is omitted', () => {
    const now = Date.now();
    expect(() => validateEvent({ ...valid, timestamp: now })).not.toThrow();
    expect(() => validateEvent({ ...valid, timestamp: now + 10 * 60_000 })).toThrow(/future/);
  });

  describe('tags', () => {
    it('rejects non-object tags', () => {
      for (const tags of ['x', 5, null, ['a']]) {
        expect(() => validateEvent({ ...valid, tags }, { nowMs: NOW })).toThrow(/tags/);
      }
    });

    it('rejects non-string and over-long tag values', () => {
      expect(() => validateEvent({ ...valid, tags: { a: 1 } }, { nowMs: NOW })).toThrow(/tag "a"/);
      expect(() => validateEvent({ ...valid, tags: { a: 'x'.repeat(257) } }, { nowMs: NOW })).toThrow(
        /tag "a"/,
      );
    });

    it('rejects too many tags and over-long keys', () => {
      const many = Object.fromEntries(Array.from({ length: 33 }, (_, i) => [`k${i}`, 'v']));
      expect(() => validateEvent({ ...valid, tags: many }, { nowMs: NOW })).toThrow(/at most 32/);
      expect(() => validateEvent({ ...valid, tags: { ['k'.repeat(65)]: 'v' } }, { nowMs: NOW })).toThrow(
        /tag key/,
      );
    });

    it('rejects a __proto__ key arriving from JSON', () => {
      const parsed = JSON.parse('{"id":"a","source":"s","metric":"m","value":1,"timestamp":' + T0 + ',"tags":{"__proto__":"x"}}');
      expect(() => validateEvent(parsed, { nowMs: NOW })).toThrow(/tag key/);
    });
  });
});

describe('seriesKeyOf', () => {
  it('joins source and metric unambiguously', () => {
    expect(seriesKeyOf('a', 'b')).toBe('a|b');
    expect(seriesKeyOf('a', 'b')).not.toBe(seriesKeyOf('b', 'a'));
  });
});

// ---------------------------------------------------------------------------
// RingBuffer
// ---------------------------------------------------------------------------

describe('RingBuffer', () => {
  it.each([0, -1, 1.5, NaN])('rejects capacity %s', (capacity) => {
    expect(() => new RingBuffer<number>(capacity)).toThrow(ConfigError);
  });

  it('behaves as a FIFO below capacity', () => {
    const rb = new RingBuffer<number>(3);
    expect(rb.size).toBe(0);
    expect(rb.shift()).toBeUndefined();
    expect(rb.peek()).toBeUndefined();
    expect(rb.push(1)).toBeUndefined();
    expect(rb.push(2)).toBeUndefined();
    expect(rb.peek()).toBe(1);
    expect(rb.shift()).toBe(1);
    expect(rb.toArray()).toEqual([2]);
  });

  it('overwrites the oldest item when full and returns it', () => {
    const rb = new RingBuffer<number>(3);
    [1, 2, 3].forEach((n) => rb.push(n));
    expect(rb.isFull).toBe(true);
    expect(rb.push(4)).toBe(1);
    expect(rb.push(5)).toBe(2);
    expect(rb.toArray()).toEqual([3, 4, 5]);
    expect([...rb]).toEqual([3, 4, 5]);
  });

  it('stays correct across many wrap-arounds', () => {
    const rb = new RingBuffer<number>(4);
    const model: number[] = [];
    for (let i = 0; i < 100; i++) {
      rb.push(i);
      model.push(i);
      if (model.length > 4) model.shift();
      if (i % 7 === 0) {
        expect(rb.shift()).toBe(model.shift());
      }
      expect(rb.toArray()).toEqual(model);
    }
  });

  it('clear() resets the buffer', () => {
    const rb = new RingBuffer<string>(2);
    rb.push('a');
    rb.push('b');
    rb.clear();
    expect(rb.size).toBe(0);
    expect(rb.toArray()).toEqual([]);
    rb.push('c');
    expect(rb.toArray()).toEqual(['c']);
  });

  it('supports capacity 1', () => {
    const rb = new RingBuffer<number>(1);
    rb.push(1);
    expect(rb.push(2)).toBe(1);
    expect(rb.toArray()).toEqual([2]);
  });
});

// ---------------------------------------------------------------------------
// RollingStats & Ewma
// ---------------------------------------------------------------------------

describe('RollingStats', () => {
  it('reports zeros when empty and zero variance for one sample', () => {
    const rs = new RollingStats(5);
    expect(rs.snapshot()).toEqual({ count: 0, mean: 0, variance: 0, stdDev: 0 });
    rs.add(7);
    expect(rs.snapshot()).toEqual({ count: 1, mean: 7, variance: 0, stdDev: 0 });
  });

  it('matches a naive two-pass computation through window eviction', () => {
    const rs = new RollingStats(10);
    const values = gaussianSeries(300, 50, 7, 1);
    const window: number[] = [];
    for (const v of values) {
      rs.add(v);
      window.push(v);
      if (window.length > 10) window.shift();
      const expected = naiveStats(window);
      const snap = rs.snapshot();
      expect(snap.count).toBe(window.length);
      expect(snap.mean).toBeCloseTo(expected.mean, 8);
      expect(snap.variance).toBeCloseTo(expected.variance, 6);
    }
  });

  it('stays accurate with forced periodic recomputation', () => {
    const rs = new RollingStats(8, 5);
    const window: number[] = [];
    for (const v of gaussianSeries(200, -20, 3, 2)) {
      rs.add(v);
      window.push(v);
      if (window.length > 8) window.shift();
    }
    const expected = naiveStats(window);
    expect(rs.snapshot().mean).toBeCloseTo(expected.mean, 10);
    expect(rs.snapshot().variance).toBeCloseTo(expected.variance, 8);
  });

  it('is numerically stable with a large constant offset', () => {
    const rs = new RollingStats(20);
    const window: number[] = [];
    for (const v of gaussianSeries(500, 1e9, 1, 3)) {
      rs.add(v);
      window.push(v);
      if (window.length > 20) window.shift();
    }
    const expected = naiveStats(window);
    expect(rs.snapshot().mean).toBeCloseTo(expected.mean, 4);
    expect(Math.abs(rs.snapshot().variance - expected.variance)).toBeLessThan(0.01);
  });

  it('never reports a negative variance for constant data', () => {
    const rs = new RollingStats(5);
    for (let i = 0; i < 100; i++) rs.add(0.1);
    expect(rs.snapshot().variance).toBeGreaterThanOrEqual(0);
    expect(rs.snapshot().stdDev).toBeLessThan(1e-9);
  });

  it('handles a window of capacity 1', () => {
    const rs = new RollingStats(1);
    rs.add(3);
    rs.add(9);
    expect(rs.snapshot()).toEqual({ count: 1, mean: 9, variance: 0, stdDev: 0 });
  });

  it('rejects non-finite samples and invalid options', () => {
    const rs = new RollingStats(3);
    expect(() => rs.add(NaN)).toThrow(ValidationError);
    expect(() => rs.add(Infinity)).toThrow(ValidationError);
    expect(() => new RollingStats(3, 0)).toThrow(ConfigError);
    expect(() => new RollingStats(0)).toThrow(ConfigError);
    expect(rs.count).toBe(0);
    expect(rs.capacity).toBe(3);
  });
});

describe('Ewma', () => {
  it('seeds with the first value', () => {
    const e = new Ewma(0.5);
    e.update(10);
    expect(e.snapshot()).toEqual({ count: 1, mean: 10, variance: 0, stdDev: 0 });
  });

  it('applies the incremental mean/variance recurrence', () => {
    const e = new Ewma(0.5);
    e.update(10);
    e.update(20);
    const snap = e.snapshot();
    expect(snap.mean).toBeCloseTo(15, 12);
    expect(snap.variance).toBeCloseTo(25, 12);
    expect(snap.stdDev).toBeCloseTo(5, 12);
  });

  it('converges to a constant input with vanishing variance', () => {
    const e = new Ewma(0.2);
    for (let i = 0; i < 300; i++) e.update(4);
    expect(e.snapshot().mean).toBeCloseTo(4, 12);
    expect(e.snapshot().variance).toBeCloseTo(0, 12);
  });

  it.each([0, -0.1, 1.1, NaN])('rejects alpha %s', (alpha) => {
    expect(() => new Ewma(alpha)).toThrow(ConfigError);
  });

  it('accepts alpha = 1 and rejects non-finite input', () => {
    const e = new Ewma(1);
    e.update(1);
    e.update(5);
    expect(e.snapshot().mean).toBe(5);
    expect(() => e.update(NaN)).toThrow(ValidationError);
  });
});

// ---------------------------------------------------------------------------
// Detectors
// ---------------------------------------------------------------------------

function ctxFor(
  value: number,
  stats: { count: number; mean: number; stdDev: number },
  ewma?: { count: number; mean: number; stdDev: number },
): DetectionContext {
  const e = ewma ?? stats;
  return {
    event: makeEvent({ value }),
    seriesKey: 'host-1|cpu.load',
    window: { ...stats, variance: stats.stdDev ** 2 },
    ewma: { ...e, variance: e.stdDev ** 2 },
    sampleCount: stats.count,
  };
}

describe('ZScoreDetector', () => {
  const stats = { count: 100, mean: 100, stdDev: 2 };

  it('stays silent during warm-up', () => {
    const d = new ZScoreDetector({ minSamples: 30 });
    expect(d.evaluate(ctxFor(1000, { ...stats, count: 29 }))).toBeNull();
  });

  it('does not fire within the threshold', () => {
    const d = new ZScoreDetector();
    expect(d.evaluate(ctxFor(105.9, stats))).toBeNull(); // 2.95 sigma
    expect(d.evaluate(ctxFor(94.1, stats))).toBeNull();
  });

  it('raises a warning between the two thresholds, symmetrically', () => {
    const d = new ZScoreDetector();
    const high = d.evaluate(ctxFor(108, stats)); // 4 sigma
    const low = d.evaluate(ctxFor(92, stats));
    expect(high?.severity).toBe('warning');
    expect(low?.severity).toBe('warning');
    expect(high?.score).toBeCloseTo(4, 10);
    expect(high?.expected).toBe(100);
    expect(high?.message).toMatch(/4 sigma/);
  });

  it('raises a critical at or above the critical threshold', () => {
    const d = new ZScoreDetector();
    expect(d.evaluate(ctxFor(110, stats))?.severity).toBe('critical'); // exactly 5 sigma
    expect(d.evaluate(ctxFor(500, stats))?.severity).toBe('critical');
  });

  it('flags any real deviation on a perfectly flat series without dividing by zero', () => {
    const d = new ZScoreDetector();
    const flat = { count: 100, mean: 100, stdDev: 0 };
    expect(d.evaluate(ctxFor(100, flat))).toBeNull();
    const finding = d.evaluate(ctxFor(101, flat));
    expect(finding?.severity).toBe('critical');
    expect(Number.isFinite(finding?.score)).toBe(true);
  });

  it('caps the score for zero-floor configurations', () => {
    const d = new ZScoreDetector({ absoluteStdDevFloor: 0, relativeStdDevFloor: 0 });
    const finding = d.evaluate(ctxFor(5, { count: 100, mean: 0, stdDev: 0 }));
    expect(finding?.score).toBe(1e9);
  });

  it.each([
    [{ threshold: 0 }],
    [{ threshold: NaN }],
    [{ threshold: 4, criticalThreshold: 3 }],
    [{ minSamples: 1 }],
    [{ minSamples: 2.5 }],
    [{ absoluteStdDevFloor: -1 }],
    [{ relativeStdDevFloor: -1 }],
  ])('rejects invalid options %j', (options) => {
    expect(() => new ZScoreDetector(options)).toThrow(ConfigError);
  });
});

describe('EwmaDetector', () => {
  it('uses the EWMA baseline, not the window', () => {
    const d = new EwmaDetector();
    const window = { count: 100, mean: 1000, stdDev: 1 };
    const ewma = { count: 100, mean: 100, stdDev: 2 };
    expect(d.evaluate(ctxFor(1000, window, ewma))?.severity).toBe('critical');
    expect(d.evaluate(ctxFor(100, window, ewma))).toBeNull();
  });

  it('respects minSamples on the EWMA count', () => {
    const d = new EwmaDetector({ minSamples: 10 });
    const w = { count: 100, mean: 0, stdDev: 1 };
    expect(d.evaluate(ctxFor(50, w, { count: 9, mean: 0, stdDev: 1 }))).toBeNull();
    expect(d.evaluate(ctxFor(50, w, { count: 10, mean: 0, stdDev: 1 }))).not.toBeNull();
  });
});

describe('ThresholdDetector', () => {
  const stats = { count: 0, mean: 0, stdDev: 0 };

  it('fires from the first sample, without a baseline', () => {
    const d = new ThresholdDetector({ max: 90 });
    const finding = d.evaluate(ctxFor(95, stats));
    expect(finding?.severity).toBe('critical');
    expect(finding?.expected).toBe(90);
    expect(finding?.message).toMatch(/exceeds maximum 90/);
  });

  it('checks the minimum bound and treats bounds as inclusive', () => {
    const d = new ThresholdDetector({ min: 10, max: 90, severity: 'warning', name: 'range' });
    expect(d.name).toBe('range');
    expect(d.evaluate(ctxFor(10, stats))).toBeNull();
    expect(d.evaluate(ctxFor(90, stats))).toBeNull();
    const low = d.evaluate(ctxFor(5, stats));
    expect(low?.severity).toBe('warning');
    expect(low?.message).toMatch(/below minimum 10/);
    expect(low?.score).toBeCloseTo(0.5, 10);
  });

  it('defaults its name to "threshold"', () => {
    expect(new ThresholdDetector({ max: 1 }).name).toBe('threshold');
  });

  it.each([
    [{}],
    [{ min: 5, max: 1 }],
    [{ min: NaN }],
    [{ max: Infinity }],
    [{ max: 1, severity: 'fatal' as never }],
    [{ max: 1, name: '' }],
  ])('rejects invalid options %j', (options) => {
    expect(() => new ThresholdDetector(options)).toThrow(ConfigError);
  });
});

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

describe('resolveConfig', () => {
  it('returns frozen defaults when called with no overrides', () => {
    const cfg = resolveConfig();
    expect(cfg.windowSize).toBe(DEFAULT_ENGINE_CONFIG.windowSize);
    expect(Object.isFrozen(cfg)).toBe(true);
  });

  it('applies overrides and ignores undefined values', () => {
    const cfg = resolveConfig({ windowSize: 10, maxSeries: undefined });
    expect(cfg.windowSize).toBe(10);
    expect(cfg.maxSeries).toBe(DEFAULT_ENGINE_CONFIG.maxSeries);
  });

  it.each<[string, Partial<EngineConfig>]>([
    ['windowSize < 2', { windowSize: 1 }],
    ['fractional windowSize', { windowSize: 4.5 }],
    ['maxSeries 0', { maxSeries: 0 }],
    ['queueCapacity 0', { queueCapacity: 0 }],
    ['dedupCapacity 0', { dedupCapacity: 0 }],
    ['flushIntervalMs 0', { flushIntervalMs: 0 }],
    ['maxBatchPerFlush 0', { maxBatchPerFlush: 0 }],
    ['negative adaptation threshold', { baselineAdaptationThreshold: -1 }],
    ['negative lateness', { allowedLatenessMs: -1 }],
    ['infinite future skew', { maxFutureSkewMs: Infinity }],
    ['NaN cooldown', { anomalyCooldownMs: NaN }],
    ['alpha 0', { ewmaAlpha: 0 }],
    ['alpha > 1', { ewmaAlpha: 1.5 }],
    ['unknown backpressure', { backpressure: 'explode' as never }],
    ['non-function clock', { clock: 5 as never }],
  ])('rejects %s', (_label, overrides) => {
    expect(() => resolveConfig(overrides)).toThrow(ConfigError);
  });

  it('accepts boundary values', () => {
    expect(() =>
      resolveConfig({
        windowSize: 2,
        maxSeries: 1,
        allowedLatenessMs: 0,
        anomalyCooldownMs: 0,
        ewmaAlpha: 1,
        baselineAdaptationThreshold: 0,
      }),
    ).not.toThrow();
  });
});

describe('StreamEngine construction', () => {
  it('requires at least one detector', () => {
    expect(() => new StreamEngine([])).toThrow(/at least one detector/);
  });

  it('rejects duplicate and malformed detectors', () => {
    expect(() => new StreamEngine([new ZScoreDetector(), new ZScoreDetector()])).toThrow(/Duplicate/);
    expect(() => new StreamEngine([{ name: '', evaluate: () => null }])).toThrow(ConfigError);
    expect(() => new StreamEngine([{ name: 'x' } as unknown as AnomalyDetector])).toThrow(ConfigError);
  });

  it('propagates configuration errors', () => {
    expect(() => new StreamEngine([new ZScoreDetector()], { windowSize: 0 })).toThrow(ConfigError);
  });

  it('createDefaultEngine wires the zscore and ewma detectors', () => {
    const engine = createDefaultEngine(baseConfig());
    const { anomalies } = collect(engine);
    feed(engine, [...alternatingSeries(100), 400]);
    expect(anomalies.map((a) => a.detector).sort()).toEqual(['ewma', 'zscore']);
  });
});

// ---------------------------------------------------------------------------
// Engine: ingestion
// ---------------------------------------------------------------------------

describe('StreamEngine ingestion', () => {
  it('accepts valid events and processes them on flush', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig());
    expect(engine.submit(makeEvent()).accepted).toBe(true);
    expect(engine.metrics().queueDepth).toBe(1);
    expect(engine.flush()).toBe(1);
    const m = engine.metrics();
    expect(m).toMatchObject({ submitted: 1, accepted: 1, processed: 1, queueDepth: 0, seriesActive: 1 });
  });

  it('rejects invalid input without throwing and emits a dropped event', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig());
    const { dropped } = collect(engine);
    const result = engine.submit({ id: 'broken', value: 'nope' });
    expect(result.accepted).toBe(false);
    if (!result.accepted) {
      expect(result.reason).toBe('invalid');
      expect(result.error).toBeInstanceOf(ValidationError);
    }
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({ reason: 'invalid', eventId: 'broken' });
    expect(engine.metrics().rejectedInvalid).toBe(1);
    expect(engine.metrics().queueDepth).toBe(0);
  });

  it('does not leak an id from a non-string or array payload', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig());
    const { dropped } = collect(engine);
    engine.submit({ id: 123 });
    engine.submit([1, 2]);
    engine.submit(null);
    expect(dropped.map((d) => d.eventId)).toEqual([undefined, undefined, undefined]);
  });

  it('applies the future-skew rule using the injected clock', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig({ maxFutureSkewMs: 1_000 }));
    expect(engine.submit(makeEvent({ timestamp: NOW + 1_000 })).accepted).toBe(true);
    expect(engine.submit(makeEvent({ timestamp: NOW + 1_001 })).accepted).toBe(false);
  });

  it('submitBatch tallies accepted and rejected events by reason', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig());
    const result = engine.submitBatch([makeEvent(), makeEvent(), { bad: true }, 'nope']);
    expect(result).toEqual({
      total: 4,
      accepted: 2,
      rejected: 2,
      rejectedByReason: { invalid: 2 },
    });
  });

  it('submitBatch accepts any iterable and handles an empty one', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig());
    expect(engine.submitBatch([])).toEqual({ total: 0, accepted: 0, rejected: 0, rejectedByReason: {} });
    function* gen() {
      yield makeEvent();
      yield makeEvent();
    }
    expect(engine.submitBatch(gen()).accepted).toBe(2);
  });

  it('flush(maxEvents) processes only that many events', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig());
    for (let i = 0; i < 5; i++) engine.submit(makeEvent({ timestamp: T0 + i }));
    expect(engine.flush(2)).toBe(2);
    expect(engine.metrics().queueDepth).toBe(3);
    expect(engine.flush(0)).toBe(0);
    expect(engine.flush()).toBe(3);
    expect(engine.flush()).toBe(0);
  });

  it('flush rejects invalid limits', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig());
    expect(() => engine.flush(-1)).toThrow(ConfigError);
    expect(() => engine.flush(NaN)).toThrow(ConfigError);
  });
});

// ---------------------------------------------------------------------------
// Engine: backpressure
// ---------------------------------------------------------------------------

describe('StreamEngine backpressure', () => {
  const ids = (n: number): TelemetryEvent[] =>
    Array.from({ length: n }, (_, i) => makeEvent({ id: `bp-${i + 1}`, timestamp: T0 + i }));

  it('drop-newest refuses new events once full', () => {
    const rec = recordingDetector();
    const engine = new StreamEngine([rec], baseConfig({ queueCapacity: 2, backpressure: 'drop-newest' }));
    const { dropped } = collect(engine);
    const [a, b, c] = ids(3) as [TelemetryEvent, TelemetryEvent, TelemetryEvent];
    expect(engine.submit(a).accepted).toBe(true);
    expect(engine.submit(b).accepted).toBe(true);
    const third = engine.submit(c);
    expect(third).toEqual({ accepted: false, reason: 'queue-full' });
    engine.flush();
    expect(rec.seen).toEqual(['bp-1', 'bp-2']);
    expect(dropped).toEqual([expect.objectContaining({ reason: 'queue-full', eventId: 'bp-3' })]);
    expect(engine.metrics().droppedQueueFull).toBe(1);
  });

  it('drop-oldest evicts the head of the queue to admit the new event', () => {
    const rec = recordingDetector();
    const engine = new StreamEngine([rec], baseConfig({ queueCapacity: 2, backpressure: 'drop-oldest' }));
    const { dropped } = collect(engine);
    for (const ev of ids(3)) expect(engine.submit(ev).accepted).toBe(true);
    engine.flush();
    expect(rec.seen).toEqual(['bp-2', 'bp-3']);
    expect(dropped).toEqual([expect.objectContaining({ reason: 'queue-full', eventId: 'bp-1' })]);
    expect(engine.metrics()).toMatchObject({ droppedQueueFull: 1, accepted: 3, processed: 2 });
  });

  it('reject throws BackpressureError and keeps the queue intact', () => {
    const rec = recordingDetector();
    const engine = new StreamEngine([rec], baseConfig({ queueCapacity: 1, backpressure: 'reject' }));
    const [a, b] = ids(2) as [TelemetryEvent, TelemetryEvent];
    engine.submit(a);
    expect(() => engine.submit(b)).toThrow(BackpressureError);
    engine.flush();
    expect(rec.seen).toEqual(['bp-1']);
    expect(engine.metrics().droppedQueueFull).toBe(1);
  });

  it('submitBatch converts BackpressureError into a tally', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ queueCapacity: 1, backpressure: 'reject' }));
    const result = engine.submitBatch(ids(3));
    expect(result.accepted).toBe(1);
    expect(result.rejectedByReason).toEqual({ 'queue-full': 2 });
  });

  it('frees capacity after a flush', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ queueCapacity: 1, backpressure: 'drop-newest' }));
    const [a, b] = ids(2) as [TelemetryEvent, TelemetryEvent];
    engine.submit(a);
    engine.flush();
    expect(engine.submit(b).accepted).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Engine: lateness and idempotency
// ---------------------------------------------------------------------------

describe('StreamEngine event-time handling', () => {
  it('drops events older than watermark minus allowed lateness', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ allowedLatenessMs: 5_000 }));
    const { dropped } = collect(engine);
    engine.submit(makeEvent({ id: 'on-time', timestamp: T0 + 100_000 }));
    engine.submit(makeEvent({ id: 'too-late', timestamp: T0 + 94_999 }));
    engine.submit(makeEvent({ id: 'edge', timestamp: T0 + 95_000 }));
    engine.submit(makeEvent({ id: 'slightly-late', timestamp: T0 + 96_000 }));
    engine.flush();
    expect(dropped.map((d) => [d.reason, d.eventId])).toEqual([['late', 'too-late']]);
    expect(engine.metrics()).toMatchObject({ droppedLate: 1, processed: 3 });
  });

  it('tracks lateness per series, not globally', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ allowedLatenessMs: 1_000 }));
    engine.submit(makeEvent({ metric: 'a', timestamp: T0 + 100_000 }));
    engine.submit(makeEvent({ metric: 'b', timestamp: T0 }));
    engine.flush();
    expect(engine.metrics()).toMatchObject({ droppedLate: 0, processed: 2 });
  });

  it('never moves the watermark backwards', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ allowedLatenessMs: 10_000 }));
    engine.submit(makeEvent({ timestamp: T0 + 20_000 }));
    engine.submit(makeEvent({ timestamp: T0 + 15_000 }));
    engine.flush();
    expect(engine.getSeriesSnapshot('host-1', 'cpu.load')?.watermark).toBe(T0 + 20_000);
  });

  it('drops duplicate ids within a series, but not across series', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig());
    const { dropped } = collect(engine);
    engine.submit(makeEvent({ id: 'same', metric: 'a' }));
    engine.submit(makeEvent({ id: 'same', metric: 'a' }));
    engine.submit(makeEvent({ id: 'same', metric: 'b' }));
    engine.flush();
    expect(dropped).toEqual([expect.objectContaining({ reason: 'duplicate', eventId: 'same' })]);
    expect(engine.metrics()).toMatchObject({ droppedDuplicate: 1, processed: 2 });
  });

  it('forgets ids beyond the dedup capacity (bounded memory)', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ dedupCapacity: 2 }));
    for (const id of ['a', 'b', 'c']) engine.submit(makeEvent({ id }));
    engine.submit(makeEvent({ id: 'a' })); // evicted from the cache, so reprocessed
    engine.submit(makeEvent({ id: 'c' })); // still cached
    engine.flush();
    expect(engine.metrics()).toMatchObject({ processed: 4, droppedDuplicate: 1 });
  });

  it('does not let a duplicate update the baseline', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig());
    engine.submit(makeEvent({ id: 'x', value: 10 }));
    engine.submit(makeEvent({ id: 'x', value: 1_000 }));
    engine.flush();
    expect(engine.getSeriesSnapshot('host-1', 'cpu.load')?.window.mean).toBe(10);
  });
});

// ---------------------------------------------------------------------------
// Engine: anomaly detection behaviour
// ---------------------------------------------------------------------------

describe('StreamEngine anomaly detection', () => {
  it('stays quiet on stationary noise', () => {
    const engine = new StreamEngine([new ZScoreDetector({ threshold: 4.5, criticalThreshold: 6 })], baseConfig());
    const { anomalies } = collect(engine);
    feed(engine, gaussianSeries(2_000, 100, 2, 11));
    expect(anomalies).toHaveLength(0);
  });

  it('flags a spike with a complete, frozen anomaly record', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig());
    const { anomalies } = collect(engine);
    const values = gaussianSeries(100, 100, 2, 5);
    const nextTs = feed(engine, values);
    feed(engine, [200], { startTs: nextTs, idPrefix: 'spike' });

    expect(anomalies).toHaveLength(1);
    const a = anomalies[0] as Anomaly;
    expect(a).toMatchObject({
      eventId: 'spike-0',
      seriesKey: 'host-1|cpu.load',
      source: 'host-1',
      metric: 'cpu.load',
      detector: 'zscore',
      severity: 'critical',
      value: 200,
      timestamp: nextTs,
    });
    expect(a.expected).toBeCloseTo(100, 0);
    expect(a.score).toBeGreaterThan(5);
    expect(a.message).toMatch(/sigma/);
    expect(Object.isFrozen(a)).toBe(true);
  });

  it('does not fire during warm-up, however extreme the value', () => {
    const engine = new StreamEngine([new ZScoreDetector({ minSamples: 30 })], baseConfig());
    const { anomalies } = collect(engine);
    feed(engine, [...gaussianSeries(29, 100, 2, 3), 10_000]);
    expect(anomalies).toHaveLength(0);
  });

  it('keeps outliers out of the baseline', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig({ anomalyCooldownMs: 0 }));
    const { anomalies } = collect(engine);
    let ts = feed(engine, gaussianSeries(100, 100, 2, 5));
    ts = feed(engine, [500, 500], { startTs: ts, idPrefix: 'out' });
    const snap = engine.getSeriesSnapshot('host-1', 'cpu.load');
    expect(Math.abs((snap?.window.mean ?? 0) - 100)).toBeLessThan(1);
    expect(snap?.window.count).toBe(100);
    expect(snap?.consecutiveAnomalies).toBe(2);
    expect(anomalies).toHaveLength(2);
    // The next normal value is judged against the clean baseline and resets the streak.
    feed(engine, [100], { startTs: ts, idPrefix: 'norm' });
    expect(engine.getSeriesSnapshot('host-1', 'cpu.load')?.consecutiveAnomalies).toBe(0);
    expect(anomalies).toHaveLength(2);
  });

  it('adapts to a sustained level shift instead of alerting forever', () => {
    const engine = new StreamEngine(
      [new ZScoreDetector()],
      baseConfig({ windowSize: 50, baselineAdaptationThreshold: 3, anomalyCooldownMs: 0 }),
    );
    const { anomalies } = collect(engine);
    const ts = feed(engine, gaussianSeries(60, 100, 2, 9));
    feed(engine, new Array<number>(100).fill(200), { startTs: ts, idPrefix: 'shift' });

    expect(anomalies.length).toBeGreaterThanOrEqual(3);
    const lastShiftId = anomalies.reduce((max, a) => Math.max(max, Number(a.eventId.split('-')[1])), -1);
    expect(lastShiftId).toBeLessThan(20); // alerts stop long before the 100 shifted events end
    expect(engine.getSeriesSnapshot('host-1', 'cpu.load')?.window.mean).toBeGreaterThan(190);
  });

  it('with a zero adaptation threshold the baseline absorbs every sample', () => {
    const engine = new StreamEngine([new ZScoreDetector()], baseConfig({ baselineAdaptationThreshold: 0 }));
    feed(engine, [...gaussianSeries(100, 100, 2, 5), 500]);
    expect(engine.getSeriesSnapshot('host-1', 'cpu.load')?.window.count).toBe(101);
  });

  it('evaluates against pre-update statistics so a sample cannot mask itself', () => {
    const seen: number[] = [];
    const probe: AnomalyDetector = {
      name: 'probe',
      evaluate(ctx) {
        seen.push(ctx.window.count);
        return null;
      },
    };
    const engine = new StreamEngine([probe], baseConfig());
    feed(engine, [1, 2, 3]);
    expect(seen).toEqual([0, 1, 2]);
  });

  it('tracks each (source, metric) series independently', () => {
    const engine = new StreamEngine([new ZScoreDetector({ threshold: 5, criticalThreshold: 8 })], baseConfig());
    const { anomalies } = collect(engine);
    feed(engine, gaussianSeries(100, 100, 2, 1), { source: 'a' });
    feed(engine, gaussianSeries(100, 5_000, 2, 2), { source: 'b' });
    feed(engine, [5_000], { source: 'a', startTs: T0 + 200_000, idPrefix: 'a-late-spike' });
    expect(anomalies).toHaveLength(1);
    expect(anomalies[0]?.source).toBe('a');
    expect(engine.metrics().seriesActive).toBe(2);
  });

  it('runs every detector and reports each finding separately', () => {
    const engine = new StreamEngine(
      [new ThresholdDetector({ max: 150, severity: 'warning' }), new ZScoreDetector()],
      baseConfig(),
    );
    const { anomalies } = collect(engine);
    feed(engine, [...alternatingSeries(100), 300]);
    expect(anomalies.map((a) => a.detector).sort()).toEqual(['threshold', 'zscore']);
  });

  it('threshold detector alerts on the very first event', () => {
    const engine = new StreamEngine([new ThresholdDetector({ min: 0, max: 100 })], baseConfig());
    const { anomalies } = collect(engine);
    feed(engine, [-5]);
    expect(anomalies).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Engine: cooldown & escalation
// ---------------------------------------------------------------------------

describe('StreamEngine alert cooldown', () => {
  /** value >= 20 is critical, value > 10 is a warning. */
  const tiered: AnomalyDetector = {
    name: 'tiered',
    evaluate(ctx) {
      const v = ctx.event.value;
      if (v >= 20) return { severity: 'critical', score: v, expected: 10, message: 'critical' };
      if (v > 10) return { severity: 'warning', score: v, expected: 10, message: 'warning' };
      return null;
    },
  };

  it('suppresses same-severity repeats, lets escalations through, then re-arms', () => {
    const engine = new StreamEngine([tiered], baseConfig({ anomalyCooldownMs: 10_000 }));
    const { anomalies } = collect(engine);
    const at = (offsetSec: number, value: number, id: string): void => {
      engine.submit({ id, source: 's', metric: 'm', value, timestamp: T0 + offsetSec * 1_000 });
    };
    at(0, 11, 'w0'); // emitted
    at(1, 11, 'w1'); // suppressed (same severity, in cooldown)
    at(2, 25, 'c2'); // emitted (escalation)
    at(3, 11, 'w3'); // suppressed (lower than last alert)
    at(11, 11, 'w11'); // suppressed (9s after the critical)
    at(13, 11, 'w13'); // emitted (11s after the critical)
    engine.flush();

    expect(anomalies.map((a) => a.eventId)).toEqual(['w0', 'c2', 'w13']);
    expect(engine.metrics()).toMatchObject({
      anomaliesDetected: 6,
      anomaliesEmitted: 3,
      anomaliesSuppressed: 3,
    });
  });

  it('uses independent cooldowns per detector and per series', () => {
    const second: AnomalyDetector = { ...tiered, name: 'tiered-2', evaluate: tiered.evaluate };
    const engine = new StreamEngine([tiered, second], baseConfig({ anomalyCooldownMs: 60_000 }));
    const { anomalies } = collect(engine);
    engine.submit({ id: '1', source: 's', metric: 'm1', value: 11, timestamp: T0 });
    engine.submit({ id: '2', source: 's', metric: 'm2', value: 11, timestamp: T0 + 1 });
    engine.flush();
    expect(anomalies).toHaveLength(4);
  });

  it('a cooldown of 0 disables suppression entirely', () => {
    const engine = new StreamEngine([tiered], baseConfig({ anomalyCooldownMs: 0 }));
    const { anomalies } = collect(engine);
    feed(engine, [11, 11, 11, 11], { source: 's', metric: 'm' });
    expect(anomalies).toHaveLength(4);
  });
});

// ---------------------------------------------------------------------------
// Engine: fault isolation
// ---------------------------------------------------------------------------

describe('StreamEngine fault isolation', () => {
  const explosive: AnomalyDetector = {
    name: 'explosive',
    evaluate(ctx) {
      if (ctx.event.id === 'bad') throw new Error('detector exploded');
      return null;
    },
  };

  it('a throwing detector does not block other detectors or later events', () => {
    const engine = new StreamEngine([explosive, new ThresholdDetector({ max: 5 })], baseConfig());
    const { anomalies, errors } = collect(engine);
    engine.submit(makeEvent({ id: 'bad', value: 10 }));
    engine.submit(makeEvent({ id: 'good', value: 10, timestamp: T0 + 1 }));
    engine.flush();

    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ phase: 'detector', detector: 'explosive', eventId: 'bad' });
    expect(errors[0]?.error.message).toBe('detector exploded');
    expect(anomalies.map((a) => a.eventId)).toEqual(['bad']); // second is within cooldown
    expect(engine.metrics()).toMatchObject({ processed: 2, detectorErrors: 1 });
  });

  it('wraps non-Error throwables', () => {
    const weird: AnomalyDetector = {
      name: 'weird',
      evaluate() {
        throw 'a string';
      },
    };
    const engine = new StreamEngine([weird], baseConfig());
    const { errors } = collect(engine);
    feed(engine, [1]);
    expect(errors[0]?.error).toBeInstanceOf(Error);
    expect(errors[0]?.error.message).toBe('a string');
  });

  it('treats a malformed finding as a detector error', () => {
    const malformed: AnomalyDetector = {
      name: 'malformed',
      evaluate: () => ({ severity: 'apocalyptic' as never, score: 1, expected: null, message: 'x' }),
    };
    const nan: AnomalyDetector = {
      name: 'nan',
      evaluate: () => ({ severity: 'info', score: NaN, expected: null, message: 'x' }),
    };
    const engine = new StreamEngine([malformed, nan], baseConfig());
    const { anomalies, errors } = collect(engine);
    feed(engine, [1]);
    expect(anomalies).toHaveLength(0);
    expect(errors).toHaveLength(2);
    expect(engine.metrics().detectorErrors).toBe(2);
  });

  it('a throwing anomaly listener does not stop other listeners and is reported', () => {
    const engine = new StreamEngine([new ThresholdDetector({ max: 5 })], baseConfig());
    const second: Anomaly[] = [];
    const errors: EngineErrorEvent[] = [];
    engine.on('error', (e) => errors.push(e));
    engine.on('anomaly', () => {
      throw new Error('listener failed');
    });
    engine.on('anomaly', (a) => second.push(a));
    feed(engine, [10]);
    expect(second).toHaveLength(1);
    expect(errors).toEqual([
      expect.objectContaining({ phase: 'listener', listenerEvent: 'anomaly' }),
    ]);
    expect(engine.metrics().listenerErrors).toBe(1);
  });

  it('swallows errors thrown by error listeners instead of recursing', () => {
    const engine = new StreamEngine([new ThresholdDetector({ max: 5 })], baseConfig());
    engine.on('anomaly', () => {
      throw new Error('boom');
    });
    engine.on('error', () => {
      throw new Error('error handler boom');
    });
    expect(() => feed(engine, [10])).not.toThrow();
    expect(engine.metrics().listenerErrors).toBe(2);
  });

  it('unsubscribe functions stop delivery', () => {
    const engine = new StreamEngine([new ThresholdDetector({ max: 5, name: 't' })], baseConfig({ anomalyCooldownMs: 0 }));
    const seen: string[] = [];
    const off = engine.on('anomaly', (a) => seen.push(a.eventId));
    feed(engine, [10], { idPrefix: 'one' });
    off();
    feed(engine, [10], { idPrefix: 'two', startTs: T0 + 10_000 });
    expect(seen).toEqual(['one-0']);
  });

  it('is safe against re-entrant flush() from a listener', () => {
    const engine = new StreamEngine([new ThresholdDetector({ max: 5 })], baseConfig());
    const reentrant: number[] = [];
    engine.on('anomaly', () => reentrant.push(engine.flush()));
    feed(engine, [10]);
    expect(reentrant).toEqual([0]);
  });

  it('events submitted from a listener are processed by the same flush', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig());
    let injected = false;
    engine.on('dropped', () => {
      if (!injected) {
        injected = true;
        engine.submit(makeEvent({ id: 'follow-up', timestamp: T0 + 5 }));
      }
    });
    engine.submit(makeEvent({ id: 'dup' }));
    engine.submit(makeEvent({ id: 'dup' }));
    engine.flush();
    expect(engine.metrics()).toMatchObject({ processed: 2, queueDepth: 0 });
  });
});

// ---------------------------------------------------------------------------
// Engine: series management (LRU)
// ---------------------------------------------------------------------------

describe('StreamEngine series eviction', () => {
  const evt = (metric: string, ts: number): TelemetryEvent => makeEvent({ metric, timestamp: ts });

  it('evicts the least recently used series beyond maxSeries', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ maxSeries: 2 }));
    for (const [i, m] of ['a', 'b', 'c'].entries()) engine.submit(evt(m, T0 + i));
    engine.flush();
    expect(engine.getSeriesSnapshot('host-1', 'a')).toBeUndefined();
    expect(engine.getSeriesSnapshot('host-1', 'b')).toBeDefined();
    expect(engine.getSeriesSnapshot('host-1', 'c')).toBeDefined();
    expect(engine.metrics()).toMatchObject({ seriesActive: 2, seriesEvicted: 1 });
  });

  it('refreshes recency on access', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ maxSeries: 2 }));
    ['a', 'b', 'a', 'c'].forEach((m, i) => engine.submit(evt(m, T0 + i)));
    engine.flush();
    expect(engine.getSeriesSnapshot('host-1', 'a')).toBeDefined();
    expect(engine.getSeriesSnapshot('host-1', 'b')).toBeUndefined();
  });

  it('getSeriesSnapshot does not change LRU order', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ maxSeries: 2 }));
    ['a', 'b'].forEach((m, i) => engine.submit(evt(m, T0 + i)));
    engine.flush();
    engine.getSeriesSnapshot('host-1', 'a'); // peek only
    engine.submit(evt('c', T0 + 10));
    engine.flush();
    expect(engine.getSeriesSnapshot('host-1', 'a')).toBeUndefined();
  });

  it('a re-created series starts with a fresh baseline', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ maxSeries: 1 }));
    engine.submit(makeEvent({ metric: 'a', value: 5 }));
    engine.submit(makeEvent({ metric: 'b', value: 6, timestamp: T0 + 1 }));
    engine.submit(makeEvent({ metric: 'a', value: 7, timestamp: T0 + 2 }));
    engine.flush();
    const snap = engine.getSeriesSnapshot('host-1', 'a');
    expect(snap?.window.count).toBe(1);
    expect(snap?.window.mean).toBe(7);
  });

  it('returns undefined for unknown series', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig());
    expect(engine.getSeriesSnapshot('nope', 'nope')).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Engine: lifecycle
// ---------------------------------------------------------------------------

describe('StreamEngine lifecycle', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('starts idle and reports state in metrics', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig());
    expect(engine.metrics().state).toBe('idle');
    engine.start();
    expect(engine.metrics().state).toBe('running');
    engine.stop();
    expect(engine.metrics().state).toBe('stopped');
  });

  it('flushes in the background at the configured interval', () => {
    vi.useFakeTimers();
    const engine = new StreamEngine([recordingDetector()], baseConfig({ flushIntervalMs: 100 }));
    engine.start();
    engine.start(); // idempotent: must not double-schedule
    engine.submit(makeEvent());
    expect(engine.metrics().processed).toBe(0);
    vi.advanceTimersByTime(99);
    expect(engine.metrics().processed).toBe(0);
    vi.advanceTimersByTime(1);
    expect(engine.metrics().processed).toBe(1);

    engine.submit(makeEvent({ timestamp: T0 + 1 }));
    vi.advanceTimersByTime(100);
    expect(engine.metrics().processed).toBe(2);
    expect(vi.getTimerCount()).toBe(1);
    engine.stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('bounds the work done per background flush', () => {
    vi.useFakeTimers();
    const engine = new StreamEngine([recordingDetector()], baseConfig({ flushIntervalMs: 10, maxBatchPerFlush: 2 }));
    engine.start();
    for (let i = 0; i < 5; i++) engine.submit(makeEvent({ timestamp: T0 + i }));
    vi.advanceTimersByTime(10);
    expect(engine.metrics().processed).toBe(2);
    vi.advanceTimersByTime(20);
    expect(engine.metrics().processed).toBe(5);
    engine.stop();
  });

  it('stop() drains the queue, reports the count, and is idempotent', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig({ maxBatchPerFlush: 2 }));
    for (let i = 0; i < 5; i++) engine.submit(makeEvent({ timestamp: T0 + i }));
    expect(engine.stop()).toBe(5);
    expect(engine.metrics()).toMatchObject({ processed: 5, queueDepth: 0 });
    expect(engine.stop()).toBe(0);
  });

  it('rejects submissions after stop with reason "shutdown"', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig());
    const { dropped } = collect(engine);
    engine.stop();
    expect(engine.submit(makeEvent({ id: 'late-arrival' }))).toEqual({ accepted: false, reason: 'shutdown' });
    expect(dropped).toEqual([{ reason: 'shutdown', eventId: 'late-arrival' }]);
  });

  it('cannot be restarted once stopped', () => {
    const engine = new StreamEngine([recordingDetector()], baseConfig());
    engine.stop();
    expect(() => engine.start()).toThrow(EngineStateError);
  });

  it('keeps running if a flush cycle throws unexpectedly', () => {
    vi.useFakeTimers();
    const engine = new StreamEngine([recordingDetector()], baseConfig({ flushIntervalMs: 10 }));
    engine.start();
    // A throwing 'dropped' listener is isolated, but make sure the loop is also resilient.
    engine.on('dropped', () => {
      throw new Error('listener bug');
    });
    engine.submit(makeEvent({ id: 'dup' }));
    engine.submit(makeEvent({ id: 'dup' }));
    vi.advanceTimersByTime(10);
    engine.submit(makeEvent({ id: 'after', timestamp: T0 + 1 }));
    vi.advanceTimersByTime(10);
    expect(engine.metrics().processed).toBe(2);
    engine.stop();
  });
});

// ---------------------------------------------------------------------------
// End-to-end scenario
// ---------------------------------------------------------------------------

describe('end-to-end scenario', () => {
  it('detects an incident across a noisy multi-host fleet with exactly the expected alerts', () => {
    // Conservative sigma thresholds: 900 Gaussian samples would trip a 3-sigma rule by chance.
    const sigma = { threshold: 6, criticalThreshold: 8 };
    const engine = new StreamEngine(
      [new ZScoreDetector(sigma), new EwmaDetector(sigma)],
      baseConfig({ windowSize: 200 }),
    );
    const { anomalies, errors } = collect(engine);
    const hosts = ['web-1', 'web-2', 'web-3'];

    hosts.forEach((host, h) => {
      const normal = gaussianSeries(300, 40 + h * 10, 1.5, 100 + h);
      feed(engine, normal, { source: host, metric: 'cpu.load' });
    });
    expect(anomalies).toHaveLength(0);

    // Incident: only web-2 spikes.
    feed(engine, [95, 96, 97], { source: 'web-2', metric: 'cpu.load', startTs: T0 + 400_000, idPrefix: 'incident' });

    expect(errors).toHaveLength(0);
    expect(new Set(anomalies.map((a) => a.source))).toEqual(new Set(['web-2']));
    expect(anomalies.every((a) => a.severity === 'critical')).toBe(true);
    expect(engine.metrics()).toMatchObject({ seriesActive: 3, processed: 903, droppedLate: 0 });
  });
});
