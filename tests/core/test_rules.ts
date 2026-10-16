/**
 * @file tests/core/test_rules.ts
 * @description Vitest suite for /src/core/rules.ts.
 */

import { describe, it, expect } from 'vitest';
import {
  RuleSet,
  RuleError,
  compileRule,
  violationOf,
  DEFAULT_COOLDOWN_MS,
  MAX_COOLDOWN_MS,
  MAX_RULES,
  MAX_TRACKED_SERIES,
  type RuleDefinition,
} from '../../src/core/rules.js';
import type { TelemetryEvent } from '../../src/core/engine.js';

const BASE = 1_700_000_000_000;

function ev(overrides: Partial<TelemetryEvent> = {}): TelemetryEvent {
  return { id: 'e', source: 'edge-01', metric: 'cpu', value: 50, timestamp: BASE, ...overrides };
}

const hot: RuleDefinition = { metric: 'cpu', max: 90, severity: 'critical' };
const cold: RuleDefinition = { metric: 'cpu', min: 5, severity: 'warning' };

function expectRuleError(fn: () => unknown, pattern?: RegExp): void {
  try {
    fn();
  } catch (err) {
    expect(err).toBeInstanceOf(RuleError);
    expect((err as RuleError).code).toBe('RULE_INVALID');
    if (pattern !== undefined) expect((err as Error).message).toMatch(pattern);
    return;
  }
  throw new Error('expected a RuleError');
}

// ---------------------------------------------------------------------------
// Compilation
// ---------------------------------------------------------------------------

describe('compileRule', () => {
  it('accepts a min-only, max-only and two-sided rule', () => {
    expect(compileRule('low', cold)).toMatchObject({ name: 'low', min: 5, max: undefined });
    expect(compileRule('high', hot)).toMatchObject({ name: 'high', min: undefined, max: 90 });
    expect(compileRule('band', { metric: 'cpu', min: 1, max: 2, severity: 'info' })).toMatchObject({ min: 1, max: 2 });
  });

  it('applies the default cooldown when none is given', () => {
    expect(compileRule('high', hot).cooldownMs).toBe(DEFAULT_COOLDOWN_MS);
  });

  it('accepts cooldown 0 (disabled) and the maximum', () => {
    expect(compileRule('a', { ...hot, cooldownMs: 0 }).cooldownMs).toBe(0);
    expect(compileRule('a', { ...hot, cooldownMs: MAX_COOLDOWN_MS }).cooldownMs).toBe(MAX_COOLDOWN_MS);
  });

  it.each(['', 'Bad', 'a_b', '-lead', 'x'.repeat(64), 'has space'])('rejects the rule name %j', (name) => {
    expectRuleError(() => compileRule(name, hot), /rule names/);
  });

  it('rejects a definition that is not an object', () => {
    expectRuleError(() => compileRule('a', 'cpu'), /definition must be an object/);
    expectRuleError(() => compileRule('a', []), /definition must be an object/);
  });

  it.each<[string, unknown]>([
    ['an empty metric', ''],
    ['a metric with a space', 'cpu load'],
    ['a metric over 256 characters', 'm'.repeat(257)],
    ['a non-string metric', 7],
  ])('rejects %s', (_label, metric) => {
    expectRuleError(() => compileRule('a', { ...hot, metric }), /metric must be/);
  });

  it('requires at least one bound', () => {
    expectRuleError(() => compileRule('a', { metric: 'cpu', severity: 'info' }), /at least one of min or max/);
  });

  it.each<[string, unknown]>([
    ['a NaN min', Number.NaN],
    ['an infinite max', Number.POSITIVE_INFINITY],
    ['a string bound', '5'],
  ])('rejects %s', (_label, value) => {
    expectRuleError(() => compileRule('a', { metric: 'cpu', min: value as number, severity: 'info' }), /finite number/);
  });

  it('rejects min above max', () => {
    expectRuleError(() => compileRule('a', { metric: 'cpu', min: 10, max: 1, severity: 'info' }), /min must not exceed max/);
  });

  it('accepts min equal to max', () => {
    expect(() => compileRule('a', { metric: 'cpu', min: 4, max: 4, severity: 'info' })).not.toThrow();
  });

  it.each<[string, unknown]>([
    ['an unknown severity', 'loud'],
    ['a missing severity', undefined],
    ['a numeric severity', 2],
  ])('rejects %s', (_label, severity) => {
    expectRuleError(() => compileRule('a', { ...hot, severity }), /severity must be/);
  });

  it.each<[string, unknown]>([
    ['a negative cooldown', -1],
    ['a fractional cooldown', 1.5],
    ['a cooldown above the maximum', MAX_COOLDOWN_MS + 1],
    ['a string cooldown', '1000'],
  ])('rejects %s', (_label, cooldownMs) => {
    expectRuleError(() => compileRule('a', { ...hot, cooldownMs }), /cooldownMs must be/);
  });
});

// ---------------------------------------------------------------------------
// Bound checks
// ---------------------------------------------------------------------------

describe('violationOf', () => {
  it('returns null for a different metric, even if the value is out of range', () => {
    expect(violationOf(compileRule('h', hot), ev({ metric: 'mem', value: 999 }))).toBeNull();
  });

  it('reports above-max and below-min with the violated bound', () => {
    expect(violationOf(compileRule('h', hot), ev({ value: 91 }))).toEqual({ kind: 'above-max', bound: 90 });
    expect(violationOf(compileRule('c', cold), ev({ value: 4 }))).toEqual({ kind: 'below-min', bound: 5 });
  });

  it('treats both bounds as inclusive', () => {
    const band = compileRule('b', { metric: 'cpu', min: 10, max: 20, severity: 'info' });
    expect(violationOf(band, ev({ value: 10 }))).toBeNull();
    expect(violationOf(band, ev({ value: 20 }))).toBeNull();
    expect(violationOf(band, ev({ value: 9.999 }))).toEqual({ kind: 'below-min', bound: 10 });
    expect(violationOf(band, ev({ value: 20.001 }))).toEqual({ kind: 'above-max', bound: 20 });
  });

  it('reports below-min for a value under the lower edge of a band', () => {
    const band = compileRule('b', { metric: 'cpu', min: 10, max: 20, severity: 'info' });
    expect(violationOf(band, ev({ value: -5 }))?.kind).toBe('below-min');
  });

  it('never flags NaN, which the engine already rejects upstream', () => {
    expect(violationOf(compileRule('h', hot), ev({ value: Number.NaN }))).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// RuleSet construction
// ---------------------------------------------------------------------------

describe('RuleSet construction', () => {
  it('starts empty by default', () => {
    expect(new RuleSet().size).toBe(0);
    expect(new RuleSet().evaluate(ev({ value: 1000 }))).toEqual([]);
  });

  it('rejects duplicate names and invalid rules', () => {
    expect(() => new RuleSet([['a', hot], ['a', cold]])).toThrow(/duplicate rule name "a"/);
    expectRuleError(() => new RuleSet([['a', { metric: 'cpu', severity: 'info' }]]));
  });

  it('enforces the rule limit', () => {
    const defs = Array.from({ length: MAX_RULES + 1 }, (_, i) => [`r${i}`, hot] as const);
    expect(() => new RuleSet(defs)).toThrow(/at most 1000 rules/);
    expect(new RuleSet(defs.slice(0, MAX_RULES)).size).toBe(MAX_RULES);
  });
});

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

describe('RuleSet evaluation', () => {
  it('produces a finding with the event and rule details', () => {
    const set = new RuleSet([['high-cpu', hot]]);
    const [finding] = set.evaluate(ev({ id: 'e9', value: 97, timestamp: BASE }));
    expect(finding).toEqual({
      rule: 'high-cpu',
      kind: 'above-max',
      severity: 'critical',
      eventId: 'e9',
      source: 'edge-01',
      metric: 'cpu',
      value: 97,
      bound: 90,
      timestamp: BASE,
      message: 'value 97 is above maximum 90',
    });
  });

  it('describes a below-minimum violation', () => {
    const [finding] = new RuleSet([['low-cpu', cold]]).evaluate(ev({ value: 1 }));
    expect(finding?.message).toBe('value 1 is below minimum 5');
  });

  it('returns nothing for an in-range value', () => {
    expect(new RuleSet([['high-cpu', hot]]).evaluate(ev({ value: 50 }))).toEqual([]);
  });

  it('returns findings in rule declaration order', () => {
    const band = { metric: 'cpu', min: 0, max: 40, severity: 'info' as const };
    const set = new RuleSet([
      ['z-first', band],
      ['a-second', { metric: 'cpu', max: 45, severity: 'warning' }],
    ]);
    expect(set.evaluate(ev({ value: 60 })).map((f) => f.rule)).toEqual(['z-first', 'a-second']);
  });

  it('applies several rules to one event independently', () => {
    const set = new RuleSet([
      ['high', hot],
      ['other-metric', { metric: 'mem', max: 1, severity: 'info' }],
    ]);
    expect(set.evaluate(ev({ value: 95 })).map((f) => f.rule)).toEqual(['high']);
  });
});

describe('cooldown', () => {
  const rule: RuleDefinition = { metric: 'cpu', max: 90, severity: 'critical', cooldownMs: 30_000 };

  it('suppresses a repeat within the cooldown and fires again after it', () => {
    const set = new RuleSet([['r', rule]]);
    expect(set.evaluate(ev({ value: 95, timestamp: BASE }))).toHaveLength(1);
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 10_000 }))).toHaveLength(0);
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 30_000 }))).toHaveLength(1);
  });

  it('does not extend the window on suppressed matches', () => {
    const set = new RuleSet([['r', rule]]);
    set.evaluate(ev({ value: 95, timestamp: BASE }));
    set.evaluate(ev({ value: 95, timestamp: BASE + 20_000 })); // suppressed
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 31_000 }))).toHaveLength(1);
  });

  it('keeps sources independent', () => {
    const set = new RuleSet([['r', rule]]);
    expect(set.evaluate(ev({ source: 'a', value: 95 }))).toHaveLength(1);
    expect(set.evaluate(ev({ source: 'b', value: 95 }))).toHaveLength(1);
    expect(set.evaluate(ev({ source: 'a', value: 95, timestamp: BASE + 1 }))).toHaveLength(0);
  });

  it('keeps rules independent for the same source', () => {
    const set = new RuleSet([
      ['r1', rule],
      ['r2', { ...rule, max: 80 }],
    ]);
    expect(set.evaluate(ev({ value: 95 })).map((f) => f.rule)).toEqual(['r1', 'r2']);
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 1 }))).toEqual([]);
  });

  it('cooldown 0 never suppresses', () => {
    const set = new RuleSet([['r', { ...rule, cooldownMs: 0 }]]);
    for (let i = 0; i < 5; i++) {
      expect(set.evaluate(ev({ value: 95, timestamp: BASE + i }))).toHaveLength(1);
    }
    expect(set.stats().suppressed).toBe(0);
  });

  it('suppresses a late event older than the last firing, measured in event time', () => {
    const set = new RuleSet([['r', rule]]);
    set.evaluate(ev({ value: 95, timestamp: BASE + 100_000 }));
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 50_000 }))).toHaveLength(0);
  });

  it('with cooldown disabled, a late event still fires', () => {
    const set = new RuleSet([['r', { ...rule, cooldownMs: 0 }]]);
    set.evaluate(ev({ value: 95, timestamp: BASE + 100_000 }));
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 50_000 }))).toHaveLength(1);
  });

  it('a non-matching event does not start or reset a cooldown', () => {
    const set = new RuleSet([['r', rule]]);
    set.evaluate(ev({ value: 95, timestamp: BASE }));
    set.evaluate(ev({ value: 10, timestamp: BASE + 5_000 }));
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 30_000 }))).toHaveLength(1);
  });

  it('bounds the cooldown table by evicting the stalest sources first', () => {
    const set = new RuleSet([['r', rule]]);
    for (let i = 0; i <= MAX_TRACKED_SERIES; i++) {
      set.evaluate(ev({ source: `s${i}`, value: 95, timestamp: BASE }));
    }
    expect(set.stats().trackedSeries).toBe(MAX_TRACKED_SERIES);
    // s0 was the stalest entry and has been evicted, so it fires again immediately.
    expect(set.evaluate(ev({ source: 's0', value: 95, timestamp: BASE }))).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// Statistics and updates
// ---------------------------------------------------------------------------

describe('stats', () => {
  it('counts evaluations, matches and suppressions', () => {
    const set = new RuleSet([['r', { ...hot, cooldownMs: 1_000 }]]);
    set.evaluate(ev({ value: 95, timestamp: BASE }));
    set.evaluate(ev({ value: 95, timestamp: BASE + 1 }));
    set.evaluate(ev({ value: 10, timestamp: BASE + 2 }));
    expect(set.stats()).toEqual({ rules: 1, evaluated: 3, matched: 2, suppressed: 1, trackedSeries: 1 });
  });

  it('returns a frozen snapshot', () => {
    expect(Object.isFrozen(new RuleSet().stats())).toBe(true);
  });
});

describe('update', () => {
  it('replaces the active rules', () => {
    const set = new RuleSet([['old', hot]]);
    set.update([['new', cold]]);
    expect(set.evaluate(ev({ value: 95 }))).toEqual([]);
    expect(set.evaluate(ev({ value: 1 })).map((f) => f.rule)).toEqual(['new']);
    expect(set.size).toBe(1);
  });

  it('leaves the previous rules in force when the update is invalid', () => {
    const set = new RuleSet([['keep', hot]]);
    expect(() => set.update([['bad', { metric: 'cpu', severity: 'info' }]])).toThrow(RuleError);
    expect(set.evaluate(ev({ value: 95 })).map((f) => f.rule)).toEqual(['keep']);
  });

  it('keeps cooldown state for rules that survive an update', () => {
    const set = new RuleSet([['keep', { ...hot, cooldownMs: 60_000 }]]);
    set.evaluate(ev({ value: 95, timestamp: BASE }));
    set.update([
      ['keep', { ...hot, cooldownMs: 60_000 }],
      ['extra', cold],
    ]);
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 1_000 }))).toEqual([]);
  });

  it('drops cooldown state for rules that are removed, so a re-added rule fires at once', () => {
    const set = new RuleSet([['gone', { ...hot, cooldownMs: 60_000 }]]);
    set.evaluate(ev({ value: 95, timestamp: BASE }));
    set.update([]);
    expect(set.stats().trackedSeries).toBe(0);
    set.update([['gone', { ...hot, cooldownMs: 60_000 }]]);
    expect(set.evaluate(ev({ value: 95, timestamp: BASE + 1_000 }))).toHaveLength(1);
  });
});
