/**
 * Prometheus text exposition for NexusPulse.
 *
 * Implements the text format (version 0.0.4): `# HELP` and `# TYPE` lines per
 * family, then one line per sample. The renderer validates names and values and
 * refuses output Prometheus would reject, so a bad metric fails in tests rather
 * than at scrape time.
 *
 * Naming. Every exported name starts with `nexuspulse_`. Counters end in `_total`.
 * Gauges do not.
 */

import type { EngineMetrics } from '../core/engine.js';
import type { IngestStats } from '../ingest/tcp_ingest.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type MetricType = 'counter' | 'gauge';

export interface MetricSample {
  readonly labels?: Readonly<Record<string, string>> | undefined;
  readonly value: number;
}

export interface MetricFamily {
  readonly name: string;
  readonly type: MetricType;
  readonly help: string;
  readonly samples: readonly MetricSample[];
}

/** Raised when a family or sample cannot be rendered as valid exposition text. Code: METRICS_INVALID. */
export class MetricsError extends Error {
  public readonly code = 'METRICS_INVALID';
  public readonly family: string;

  constructor(family: string, message: string) {
    super(`metric ${family}: ${message}`);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'MetricsError';
    this.family = family;
  }
}

// ---------------------------------------------------------------------------
// Validation and escaping
// ---------------------------------------------------------------------------

const METRIC_NAME = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;
const LABEL_NAME = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const NAME_PREFIX = 'nexuspulse_';
const MAX_HELP_LENGTH = 512;
const MAX_LABELS = 8;
const MAX_LABEL_VALUE_LENGTH = 256;
const MAX_FAMILIES = 1000;

/** Escapes a HELP string: backslash and newline. */
export function escapeHelp(text: string): string {
  return text.replace(/\\/g, '\\\\').replace(/\n/g, '\\n');
}

/** Escapes a label value: backslash, double quote and newline. */
export function escapeLabelValue(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/"/g, '\\"');
}

/** Formats a sample value. Prometheus spells the non-finite values NaN, +Inf and -Inf. */
export function formatValue(value: number): string {
  if (Number.isNaN(value)) return 'NaN';
  if (value === Infinity) return '+Inf';
  if (value === -Infinity) return '-Inf';
  return String(value);
}

function renderLabels(labels: Readonly<Record<string, string>> | undefined): string {
  if (labels === undefined) return '';
  const keys = Object.keys(labels).sort();
  if (keys.length === 0) return '';
  const parts = keys.map((key) => `${key}="${escapeLabelValue(labels[key] ?? '')}"`);
  return `{${parts.join(',')}}`;
}

function validateFamily(family: MetricFamily): void {
  const name = family.name;
  if (!METRIC_NAME.test(name)) throw new MetricsError(name, 'name is not a valid Prometheus metric name');
  if (!name.startsWith(NAME_PREFIX)) throw new MetricsError(name, `name must start with ${NAME_PREFIX}`);
  if (family.type !== 'counter' && family.type !== 'gauge') {
    throw new MetricsError(name, 'type must be counter or gauge');
  }
  if (family.type === 'counter' && !name.endsWith('_total')) {
    throw new MetricsError(name, 'counter names must end with _total');
  }
  if (family.type === 'gauge' && name.endsWith('_total')) {
    throw new MetricsError(name, 'gauge names must not end with _total');
  }
  if (family.help.length === 0 || family.help.length > MAX_HELP_LENGTH) {
    throw new MetricsError(name, `help must be 1-${MAX_HELP_LENGTH} characters`);
  }
  if (family.samples.length === 0) throw new MetricsError(name, 'family has no samples');

  const seen = new Set<string>();
  for (const sample of family.samples) {
    if (typeof sample.value !== 'number') throw new MetricsError(name, 'sample value must be a number');
    if (family.type === 'counter' && (sample.value < 0 || Number.isNaN(sample.value))) {
      throw new MetricsError(name, 'counter values must be non-negative numbers');
    }

    const labels = sample.labels ?? {};
    const keys = Object.keys(labels);
    if (keys.length > MAX_LABELS) throw new MetricsError(name, `at most ${MAX_LABELS} labels per sample`);
    for (const key of keys) {
      if (!LABEL_NAME.test(key) || key.startsWith('__')) {
        throw new MetricsError(name, `label name ${JSON.stringify(key)} is not allowed`);
      }
      const value = labels[key];
      if (typeof value !== 'string' || value.length > MAX_LABEL_VALUE_LENGTH) {
        throw new MetricsError(name, `label ${key} must be a string of at most ${MAX_LABEL_VALUE_LENGTH} characters`);
      }
    }

    const identity = JSON.stringify(Object.keys(labels).sort().map((key) => [key, labels[key]]));
    if (seen.has(identity)) throw new MetricsError(name, 'two samples have the same label set');
    seen.add(identity);
  }
}

// ---------------------------------------------------------------------------
// Renderer
// ---------------------------------------------------------------------------

/**
 * Renders families as exposition text. Families are emitted sorted by name, so
 * output is deterministic. The result ends with a newline, or is empty when
 * there are no families.
 *
 * @throws {MetricsError} If any family or sample is invalid, or names repeat.
 */
export function renderPrometheus(families: readonly MetricFamily[]): string {
  if (families.length > MAX_FAMILIES) throw new MetricsError('<all>', `at most ${MAX_FAMILIES} families`);

  const names = new Set<string>();
  for (const family of families) {
    validateFamily(family);
    if (names.has(family.name)) throw new MetricsError(family.name, 'family name is used more than once');
    names.add(family.name);
  }

  const lines: string[] = [];
  const sorted = [...families].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  for (const family of sorted) {
    lines.push(`# HELP ${family.name} ${escapeHelp(family.help)}`);
    lines.push(`# TYPE ${family.name} ${family.type}`);
    for (const sample of family.samples) {
      lines.push(`${family.name}${renderLabels(sample.labels)} ${formatValue(sample.value)}`);
    }
  }
  return lines.length === 0 ? '' : `${lines.join('\n')}\n`;
}

// ---------------------------------------------------------------------------
// NexusPulse families
// ---------------------------------------------------------------------------

type NumericEngineKey = Exclude<keyof EngineMetrics, 'state'>;
type NumericIngestKey = keyof IngestStats;

const ENGINE_COUNTERS: readonly (readonly [NumericEngineKey, string, string])[] = [
  ['submitted', 'nexuspulse_engine_events_submitted_total', 'Events offered to the engine.'],
  ['accepted', 'nexuspulse_engine_events_accepted_total', 'Events accepted into the queue.'],
  ['rejectedInvalid', 'nexuspulse_engine_events_rejected_invalid_total', 'Events rejected by validation.'],
  ['droppedQueueFull', 'nexuspulse_engine_events_dropped_queue_full_total', 'Events dropped because the queue was full.'],
  ['droppedLate', 'nexuspulse_engine_events_dropped_late_total', 'Events dropped as too late to order.'],
  ['droppedDuplicate', 'nexuspulse_engine_events_dropped_duplicate_total', 'Events dropped as duplicates.'],
  ['processed', 'nexuspulse_engine_events_processed_total', 'Events processed by detectors.'],
  ['anomaliesDetected', 'nexuspulse_engine_anomalies_detected_total', 'Anomalies detected before suppression.'],
  ['anomaliesEmitted', 'nexuspulse_engine_anomalies_emitted_total', 'Anomalies emitted to listeners.'],
  ['anomaliesSuppressed', 'nexuspulse_engine_anomalies_suppressed_total', 'Anomalies suppressed by cooldown.'],
  ['detectorErrors', 'nexuspulse_engine_detector_errors_total', 'Detector failures.'],
  ['listenerErrors', 'nexuspulse_engine_listener_errors_total', 'Listener failures.'],
  ['flushes', 'nexuspulse_engine_flushes_total', 'Queue flush cycles.'],
  ['seriesEvicted', 'nexuspulse_engine_series_evicted_total', 'Series evicted from the LRU.'],
];

const ENGINE_GAUGES: readonly (readonly [NumericEngineKey, string, string])[] = [
  ['queueDepth', 'nexuspulse_engine_queue_depth', 'Events waiting in the queue.'],
  ['seriesActive', 'nexuspulse_engine_series_active', 'Series currently tracked.'],
];

const INGEST_COUNTERS: readonly (readonly [NumericIngestKey, string, string])[] = [
  ['connectionsAccepted', 'nexuspulse_ingest_connections_accepted_total', 'TCP connections accepted.'],
  ['connectionsRejected', 'nexuspulse_ingest_connections_rejected_total', 'TCP connections refused at the limit.'],
  ['connectionErrors', 'nexuspulse_ingest_connection_errors_total', 'TCP connection errors.'],
  ['idleClosed', 'nexuspulse_ingest_idle_closed_total', 'TCP connections closed for inactivity.'],
  ['framesReceived', 'nexuspulse_ingest_frames_received_total', 'DATA frames received.'],
  ['malformedFrames', 'nexuspulse_ingest_malformed_frames_total', 'DATA frames that failed to decode.'],
  ['eventsAccepted', 'nexuspulse_ingest_events_accepted_total', 'Events accepted from TCP.'],
  ['eventsRejected', 'nexuspulse_ingest_events_rejected_total', 'Events rejected from TCP.'],
  ['eventsDroppedQueueFull', 'nexuspulse_ingest_events_dropped_queue_full_total', 'TCP events dropped under backpressure.'],
  ['internalErrors', 'nexuspulse_ingest_internal_errors_total', 'Unexpected ingest failures.'],
];

/** Input accepted by {@link nexusPulseFamilies}. `AppStats` from the app layer satisfies it. */
export interface MetricsInput {
  readonly engine: EngineMetrics;
  readonly ingest?: IngestStats | undefined;
  readonly rules?: { readonly rules: number } | undefined;
  readonly findings?: number | undefined;
  readonly ruleReloads?: number | undefined;
  readonly ruleReloadFailures?: number | undefined;
  /** Event-loop delay figures, when a monitor is running. */
  readonly eventLoop?: { readonly samples: number; readonly p50Ms: number; readonly p99Ms: number; readonly maxMs: number } | undefined;
}

/** Converts NexusPulse statistics into metric families. */
export function nexusPulseFamilies(input: MetricsInput): MetricFamily[] {
  const families: MetricFamily[] = [];
  const engine = input.engine;

  families.push({
    name: 'nexuspulse_engine_state',
    type: 'gauge',
    help: 'Engine lifecycle state; the active state has value 1.',
    samples: [{ labels: { state: engine.state }, value: 1 }],
  });
  for (const [key, name, help] of ENGINE_COUNTERS) {
    families.push({ name, type: 'counter', help, samples: [{ value: engine[key] }] });
  }
  for (const [key, name, help] of ENGINE_GAUGES) {
    families.push({ name, type: 'gauge', help, samples: [{ value: engine[key] }] });
  }

  if (input.ingest !== undefined) {
    const ingest = input.ingest;
    for (const [key, name, help] of INGEST_COUNTERS) {
      families.push({ name, type: 'counter', help, samples: [{ value: ingest[key] }] });
    }
    families.push({
      name: 'nexuspulse_ingest_connections_open',
      type: 'gauge',
      help: 'TCP connections currently open.',
      samples: [{ value: ingest.connectionsOpen }],
    });
  }

  if (input.rules !== undefined) {
    families.push({
      name: 'nexuspulse_rules_loaded',
      type: 'gauge',
      help: 'Alert rules currently in force.',
      samples: [{ value: input.rules.rules }],
    });
  }
  if (input.findings !== undefined) {
    families.push({
      name: 'nexuspulse_rule_findings_total',
      type: 'counter',
      help: 'Rule violations reported.',
      samples: [{ value: input.findings }],
    });
  }
  if (input.ruleReloads !== undefined) {
    families.push({
      name: 'nexuspulse_rule_reloads_total',
      type: 'counter',
      help: 'Successful rule reloads.',
      samples: [{ value: input.ruleReloads }],
    });
  }
  if (input.ruleReloadFailures !== undefined) {
    families.push({
      name: 'nexuspulse_rule_reload_failures_total',
      type: 'counter',
      help: 'Rule reloads rejected because a stored rule was invalid.',
      samples: [{ value: input.ruleReloadFailures }],
    });
  }
  if (input.eventLoop !== undefined) {
    const lag = input.eventLoop;
    families.push(
      {
        name: 'nexuspulse_event_loop_lag_p99_ms',
        type: 'gauge',
        help: '99th percentile event-loop delay in milliseconds.',
        samples: [{ value: lag.p99Ms }],
      },
      {
        name: 'nexuspulse_event_loop_lag_max_ms',
        type: 'gauge',
        help: 'Largest event-loop delay in milliseconds since the last reset.',
        samples: [{ value: lag.maxMs }],
      },
      {
        name: 'nexuspulse_event_loop_samples_total',
        type: 'counter',
        help: 'Event-loop delay samples taken.',
        samples: [{ value: lag.samples }],
      },
    );
  }
  return families;
}

/** Convenience: renders NexusPulse statistics straight to exposition text. */
export function renderNexusPulseMetrics(input: MetricsInput): string {
  return renderPrometheus(nexusPulseFamilies(input));
}
