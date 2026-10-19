/**
 * Tests for the Prometheus exposition renderer and NexusPulse metric families.
 */

import { describe, it, expect } from 'vitest';
import {
  escapeHelp,
  escapeLabelValue,
  formatValue,
  MetricsError,
  nexusPulseFamilies,
  renderNexusPulseMetrics,
  renderPrometheus,
  type MetricFamily,
} from '../../src/observability/prometheus.js';
import { createDefaultEngine, type EngineMetrics } from '../../src/core/engine.js';
import type { IngestStats } from '../../src/ingest/tcp_ingest.js';

function counter(name: string, value: number, help = 'A counter.'): MetricFamily {
  return { name, type: 'counter', help, samples: [{ value }] };
}

function gauge(name: string, value: number, help = 'A gauge.'): MetricFamily {
  return { name, type: 'gauge', help, samples: [{ value }] };
}

function errorOf(fn: () => unknown): MetricsError {
  try {
    fn();
  } catch (err) {
    if (err instanceof MetricsError) return err;
    throw err;
  }
  throw new Error('expected a MetricsError');
}

// ---------------------------------------------------------------------------
// Escaping and formatting
// ---------------------------------------------------------------------------

describe('escapeHelp', () => {
  it('escapes backslashes and newlines', () => {
    expect(escapeHelp('a\\b\nc')).toBe('a\\\\b\\nc');
  });

  it('leaves double quotes alone in help text', () => {
    expect(escapeHelp('say "hi"')).toBe('say "hi"');
  });
});

describe('escapeLabelValue', () => {
  it('escapes backslash, double quote and newline', () => {
    expect(escapeLabelValue('a\\b"c\nd')).toBe('a\\\\b\\"c\\nd');
  });

  it('returns plain text unchanged', () => {
    expect(escapeLabelValue('edge-01')).toBe('edge-01');
  });
});

describe('formatValue', () => {
  it('formats finite numbers with String()', () => {
    expect(formatValue(0)).toBe('0');
    expect(formatValue(42)).toBe('42');
    expect(formatValue(-1.5)).toBe('-1.5');
  });

  it('spells non-finite values the way Prometheus expects', () => {
    expect(formatValue(NaN)).toBe('NaN');
    expect(formatValue(Infinity)).toBe('+Inf');
    expect(formatValue(-Infinity)).toBe('-Inf');
  });
});

// ---------------------------------------------------------------------------
// Renderer
// ---------------------------------------------------------------------------

describe('renderPrometheus output', () => {
  it('writes HELP and TYPE before the samples', () => {
    const text = renderPrometheus([counter('nexuspulse_events_total', 7, 'Events seen.')]);
    expect(text).toBe(
      ['# HELP nexuspulse_events_total Events seen.', '# TYPE nexuspulse_events_total counter', 'nexuspulse_events_total 7', ''].join(
        '\n',
      ),
    );
  });

  it('ends with a newline', () => {
    expect(renderPrometheus([gauge('nexuspulse_depth', 1)]).endsWith('\n')).toBe(true);
  });

  it('returns an empty string for no families', () => {
    expect(renderPrometheus([])).toBe('');
  });

  it('sorts families by name regardless of input order', () => {
    const text = renderPrometheus([gauge('nexuspulse_b', 1), gauge('nexuspulse_a', 2)]);
    expect(text.indexOf('nexuspulse_a 2')).toBeLessThan(text.indexOf('nexuspulse_b 1'));
  });

  it('writes one line per labelled sample with sorted label keys', () => {
    const text = renderPrometheus([
      {
        name: 'nexuspulse_series_total',
        type: 'counter',
        help: 'Per source.',
        samples: [
          { labels: { source: 'edge-02', metric: 'mem' }, value: 2 },
          { labels: { source: 'edge-01', metric: 'cpu' }, value: 5 },
        ],
      },
    ]);
    expect(text).toContain('nexuspulse_series_total{metric="mem",source="edge-02"} 2');
    expect(text).toContain('nexuspulse_series_total{metric="cpu",source="edge-01"} 5');
  });

  it('escapes special characters in label values and help text', () => {
    const text = renderPrometheus([
      {
        name: 'nexuspulse_note',
        type: 'gauge',
        help: 'line one\nline two',
        samples: [{ labels: { path: 'C:\\temp "x"' }, value: 1 }],
      },
    ]);
    expect(text).toContain('# HELP nexuspulse_note line one\\nline two');
    expect(text).toContain('nexuspulse_note{path="C:\\\\temp \\"x\\""} 1');
  });

  it('renders non-finite gauge values', () => {
    const text = renderPrometheus([gauge('nexuspulse_ratio', NaN), gauge('nexuspulse_peak', Infinity)]);
    expect(text).toContain('nexuspulse_ratio NaN');
    expect(text).toContain('nexuspulse_peak +Inf');
  });

  it('accepts a sample with an explicit empty label set', () => {
    const text = renderPrometheus([{ name: 'nexuspulse_x', type: 'gauge', help: 'x.', samples: [{ labels: {}, value: 3 }] }]);
    expect(text).toContain('nexuspulse_x 3\n');
  });
});

describe('renderPrometheus validation', () => {
  it('rejects names that are not valid metric names', () => {
    const err = errorOf(() => renderPrometheus([gauge('nexuspulse-bad', 1)]));
    expect(err.code).toBe('METRICS_INVALID');
    expect(err.message).toContain('not a valid Prometheus metric name');
  });

  it('rejects names without the nexuspulse_ prefix', () => {
    expect(errorOf(() => renderPrometheus([gauge('other_depth', 1)])).message).toContain('must start with nexuspulse_');
  });

  it('rejects an unknown family type', () => {
    const bad = { name: 'nexuspulse_x', type: 'histogram', help: 'x.', samples: [{ value: 1 }] } as unknown as MetricFamily;
    expect(errorOf(() => renderPrometheus([bad])).message).toContain('type must be counter or gauge');
  });

  it('requires counter names to end with _total', () => {
    expect(errorOf(() => renderPrometheus([counter('nexuspulse_events', 1)])).message).toContain('must end with _total');
  });

  it('forbids gauge names that end with _total', () => {
    expect(errorOf(() => renderPrometheus([gauge('nexuspulse_events_total', 1)])).message).toContain(
      'must not end with _total',
    );
  });

  it('rejects empty or overlong help text', () => {
    expect(errorOf(() => renderPrometheus([gauge('nexuspulse_x', 1, '')])).message).toContain('help must be');
    expect(errorOf(() => renderPrometheus([gauge('nexuspulse_x', 1, 'h'.repeat(513))])).message).toContain(
      'help must be',
    );
  });

  it('rejects a family with no samples', () => {
    const empty: MetricFamily = { name: 'nexuspulse_x', type: 'gauge', help: 'x.', samples: [] };
    expect(errorOf(() => renderPrometheus([empty])).message).toContain('family has no samples');
  });

  it('rejects negative counter values', () => {
    expect(errorOf(() => renderPrometheus([counter('nexuspulse_x_total', -1)])).message).toContain(
      'counter values must be non-negative',
    );
  });

  it('rejects NaN counter values', () => {
    expect(errorOf(() => renderPrometheus([counter('nexuspulse_x_total', NaN)])).message).toContain(
      'counter values must be non-negative',
    );
  });

  it('rejects label names that are invalid or reserved', () => {
    const bad = (key: string): MetricFamily => ({
      name: 'nexuspulse_x',
      type: 'gauge',
      help: 'x.',
      samples: [{ labels: { [key]: 'v' }, value: 1 }],
    });
    expect(errorOf(() => renderPrometheus([bad('bad-label')])).message).toContain('label name');
    expect(errorOf(() => renderPrometheus([bad('__reserved')])).message).toContain('label name');
    expect(errorOf(() => renderPrometheus([bad('1st')])).message).toContain('label name');
  });

  it('rejects label values that are too long', () => {
    const family: MetricFamily = {
      name: 'nexuspulse_x',
      type: 'gauge',
      help: 'x.',
      samples: [{ labels: { k: 'v'.repeat(257) }, value: 1 }],
    };
    expect(errorOf(() => renderPrometheus([family])).message).toContain('must be a string of at most 256');
  });

  it('rejects more than eight labels on one sample', () => {
    const labels: Record<string, string> = {};
    for (let i = 0; i < 9; i++) labels[`k${i}`] = 'v';
    const family: MetricFamily = { name: 'nexuspulse_x', type: 'gauge', help: 'x.', samples: [{ labels, value: 1 }] };
    expect(errorOf(() => renderPrometheus([family])).message).toContain('at most 8 labels');
  });

  it('rejects two samples with the same label set', () => {
    const family: MetricFamily = {
      name: 'nexuspulse_x',
      type: 'gauge',
      help: 'x.',
      samples: [
        { labels: { a: '1', b: '2' }, value: 1 },
        { labels: { b: '2', a: '1' }, value: 2 },
      ],
    };
    expect(errorOf(() => renderPrometheus([family])).message).toContain('same label set');
  });

  it('rejects two families with the same name', () => {
    expect(errorOf(() => renderPrometheus([gauge('nexuspulse_x', 1), gauge('nexuspulse_x', 2)])).message).toContain(
      'used more than once',
    );
  });

  it('rejects a non-numeric sample value', () => {
    const bad = { name: 'nexuspulse_x', type: 'gauge', help: 'x.', samples: [{ value: '1' }] } as unknown as MetricFamily;
    expect(errorOf(() => renderPrometheus([bad])).message).toContain('sample value must be a number');
  });

  it('limits the number of families', () => {
    const many = Array.from({ length: 1001 }, (_, i) => gauge(`nexuspulse_g${i}`, i));
    expect(errorOf(() => renderPrometheus(many)).message).toContain('at most 1000 families');
  });
});

// ---------------------------------------------------------------------------
// NexusPulse families
// ---------------------------------------------------------------------------

const INGEST: IngestStats = {
  connectionsAccepted: 4,
  connectionsRejected: 1,
  connectionsOpen: 2,
  connectionErrors: 0,
  idleClosed: 1,
  framesReceived: 90,
  malformedFrames: 3,
  eventsAccepted: 80,
  eventsRejected: 6,
  eventsDroppedQueueFull: 2,
  authFailures: 0,
  internalErrors: 0,
};

function engineMetrics(overrides: Partial<EngineMetrics> = {}): EngineMetrics {
  return { ...createDefaultEngine().metrics(), ...overrides };
}

describe('nexusPulseFamilies', () => {
  it('emits engine counters, gauges and the state info metric', () => {
    const families = nexusPulseFamilies({ engine: engineMetrics({ accepted: 12, queueDepth: 3 }) });
    const byName = new Map(families.map((f) => [f.name, f]));

    expect(byName.get('nexuspulse_engine_events_accepted_total')?.samples[0]?.value).toBe(12);
    expect(byName.get('nexuspulse_engine_events_accepted_total')?.type).toBe('counter');
    expect(byName.get('nexuspulse_engine_queue_depth')?.type).toBe('gauge');
    expect(byName.get('nexuspulse_engine_queue_depth')?.samples[0]?.value).toBe(3);
    expect(byName.get('nexuspulse_engine_state')?.samples[0]?.labels).toEqual({ state: 'idle' });
  });

  it('omits ingest, rule and app families when their input is absent', () => {
    const names = nexusPulseFamilies({ engine: engineMetrics() }).map((f) => f.name);
    expect(names.some((n) => n.startsWith('nexuspulse_ingest_'))).toBe(false);
    expect(names).not.toContain('nexuspulse_rules_loaded');
    expect(names).not.toContain('nexuspulse_rule_findings_total');
  });

  it('maps ingest statistics, including the open-connections gauge', () => {
    const families = nexusPulseFamilies({ engine: engineMetrics(), ingest: INGEST });
    const byName = new Map(families.map((f) => [f.name, f]));
    expect(byName.get('nexuspulse_ingest_events_accepted_total')?.samples[0]?.value).toBe(80);
    expect(byName.get('nexuspulse_ingest_malformed_frames_total')?.samples[0]?.value).toBe(3);
    expect(byName.get('nexuspulse_ingest_connections_open')?.type).toBe('gauge');
    expect(byName.get('nexuspulse_ingest_connections_open')?.samples[0]?.value).toBe(2);
  });

  it('maps rule and app counters', () => {
    const families = nexusPulseFamilies({
      engine: engineMetrics(),
      rules: { rules: 4 },
      findings: 9,
      ruleReloads: 2,
      ruleReloadFailures: 1,
    });
    const byName = new Map(families.map((f) => [f.name, f]));
    expect(byName.get('nexuspulse_rules_loaded')?.samples[0]?.value).toBe(4);
    expect(byName.get('nexuspulse_rule_findings_total')?.samples[0]?.value).toBe(9);
    expect(byName.get('nexuspulse_rule_reloads_total')?.samples[0]?.value).toBe(2);
    expect(byName.get('nexuspulse_rule_reload_failures_total')?.samples[0]?.value).toBe(1);
  });

  it('accepts an AppStats-shaped value with undefined optional parts', () => {
    const stats = {
      state: 'created' as const,
      engine: engineMetrics(),
      ingest: undefined,
      rules: { rules: 0 } as never,
      findings: 0,
      ruleReloads: 0,
      ruleReloadFailures: 0,
    };
    expect(() => renderNexusPulseMetrics(stats)).not.toThrow();
  });

  it('produces a family set that renders without errors', () => {
    const text = renderNexusPulseMetrics({
      engine: engineMetrics({ accepted: 5, submitted: 6, rejectedInvalid: 1 }),
      ingest: INGEST,
      rules: { rules: 2 },
      findings: 1,
      ruleReloads: 1,
      ruleReloadFailures: 0,
    });
    expect(text).toContain('# TYPE nexuspulse_engine_events_accepted_total counter');
    expect(text).toContain('nexuspulse_engine_events_accepted_total 5');
    expect(text).toContain('nexuspulse_engine_state{state="idle"} 1');
    expect(text).toContain('nexuspulse_ingest_connections_open 2');
    expect(text.endsWith('\n')).toBe(true);
  });

  it('gives every engine counter a _total suffix and every gauge none', () => {
    const families = nexusPulseFamilies({ engine: engineMetrics(), ingest: INGEST, rules: { rules: 0 } });
    for (const family of families) {
      if (family.type === 'counter') expect(family.name.endsWith('_total')).toBe(true);
      else expect(family.name.endsWith('_total')).toBe(false);
    }
  });
});
