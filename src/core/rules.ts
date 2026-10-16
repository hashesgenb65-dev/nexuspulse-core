/**
 * NexusPulse alert rules: static bounds checks evaluated against telemetry events.
 *
 * A rule names a metric and a valid range. An event whose metric matches and
 * whose value falls outside the range produces a {@link RuleFinding}. Each rule
 * has a cooldown, keyed by rule and source, so a sensor stuck outside its range
 * raises one alert per cooldown instead of one per sample.
 *
 * Cooldown uses event time, not wall-clock time, so replaying old data behaves
 * the same as live data. An event older than the last firing for its rule and
 * source is treated as inside the cooldown and suppressed.
 *
 * Rules are compiled and validated before they replace the active set, so a bad
 * update leaves the previous rules in force.
 */

import type { Severity, TelemetryEvent } from './engine.js';

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Raised for an invalid rule definition. Code: RULE_INVALID. */
export class RuleError extends Error {
  public readonly code = 'RULE_INVALID';
  public readonly details: Readonly<Record<string, unknown>>;

  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'RuleError';
    this.details = Object.freeze({ ...details });
  }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** The stored shape of a rule, without its name. */
export interface RuleDefinition {
  readonly metric: string;
  /** Lowest acceptable value (inclusive). At least one of `min` or `max` is required. */
  readonly min?: number;
  /** Highest acceptable value (inclusive). */
  readonly max?: number;
  readonly severity: Severity;
  /** Minimum event-time gap between repeat findings for one source. Default 30 000 ms; 0 disables. */
  readonly cooldownMs?: number;
}

export type ViolationKind = 'below-min' | 'above-max';

export interface RuleFinding {
  readonly rule: string;
  readonly kind: ViolationKind;
  readonly severity: Severity;
  readonly eventId: string;
  readonly source: string;
  readonly metric: string;
  readonly value: number;
  /** The violated bound: `min` for below-min, `max` for above-max. */
  readonly bound: number;
  readonly timestamp: number;
  readonly message: string;
}

export interface RuleSetStats {
  readonly rules: number;
  /** Events passed to evaluate(). */
  readonly evaluated: number;
  /** Rule matches before cooldown. */
  readonly matched: number;
  /** Matches suppressed by cooldown. */
  readonly suppressed: number;
  /** Cooldown entries currently tracked. */
  readonly trackedSeries: number;
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

export const DEFAULT_COOLDOWN_MS = 30_000;
export const MAX_COOLDOWN_MS = 86_400_000;
export const MAX_RULES = 1_000;
/** Cooldown entries kept in memory. The oldest are evicted first. */
export const MAX_TRACKED_SERIES = 100_000;

const RULE_NAME_PATTERN = /^[a-z0-9][a-z0-9-]{0,62}$/;
const METRIC_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/;
const SEVERITIES: ReadonlySet<string> = new Set<Severity>(['info', 'warning', 'critical']);

// ---------------------------------------------------------------------------
// Compilation
// ---------------------------------------------------------------------------

/** A rule after validation, with its name and default cooldown applied. */
export interface CompiledRule {
  readonly name: string;
  readonly metric: string;
  readonly min: number | undefined;
  readonly max: number | undefined;
  readonly severity: Severity;
  readonly cooldownMs: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function invalid(name: string, message: string): RuleError {
  return new RuleError(`rule "${name}": ${message}`, { rule: name });
}

function optionalFinite(name: string, field: 'min' | 'max', value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw invalid(name, `${field} must be a finite number`);
  }
  return value;
}

/**
 * Validates one rule and returns its compiled form.
 *
 * @throws {RuleError} When the name, metric, bounds, severity or cooldown is invalid.
 */
export function compileRule(name: string, definition: unknown): CompiledRule {
  if (typeof name !== 'string' || !RULE_NAME_PATTERN.test(name)) {
    throw new RuleError('rule names must be 1-63 lowercase letters, digits or hyphens', { rule: String(name) });
  }
  if (!isRecord(definition)) throw invalid(name, 'definition must be an object');

  const metric = definition['metric'];
  if (typeof metric !== 'string' || metric.length === 0 || metric.length > 256 || !METRIC_PATTERN.test(metric)) {
    throw invalid(name, 'metric must be 1-256 characters of letters, digits and ._:/-');
  }

  const min = optionalFinite(name, 'min', definition['min']);
  const max = optionalFinite(name, 'max', definition['max']);
  if (min === undefined && max === undefined) throw invalid(name, 'at least one of min or max is required');
  if (min !== undefined && max !== undefined && min > max) throw invalid(name, 'min must not exceed max');

  const severity = definition['severity'];
  if (typeof severity !== 'string' || !SEVERITIES.has(severity)) {
    throw invalid(name, 'severity must be info, warning or critical');
  }

  const cooldownIn = definition['cooldownMs'];
  let cooldownMs = DEFAULT_COOLDOWN_MS;
  if (cooldownIn !== undefined) {
    if (typeof cooldownIn !== 'number' || !Number.isInteger(cooldownIn) || cooldownIn < 0 || cooldownIn > MAX_COOLDOWN_MS) {
      throw invalid(name, `cooldownMs must be an integer from 0 to ${MAX_COOLDOWN_MS}`);
    }
    cooldownMs = cooldownIn;
  }

  return { name, metric, min, max, severity: severity as Severity, cooldownMs };
}

function compileAll(definitions: Iterable<readonly [string, unknown]>): CompiledRule[] {
  const compiled: CompiledRule[] = [];
  const seen = new Set<string>();
  for (const [name, definition] of definitions) {
    if (compiled.length >= MAX_RULES) {
      throw new RuleError(`a rule set may hold at most ${MAX_RULES} rules`, { limit: MAX_RULES });
    }
    if (seen.has(name)) throw new RuleError(`duplicate rule name "${name}"`, { rule: name });
    seen.add(name);
    compiled.push(compileRule(name, definition));
  }
  return compiled;
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/**
 * Returns the bound an event violates for one rule, or null if the event does
 * not match the rule's metric or lies within its range.
 */
export function violationOf(
  rule: Pick<CompiledRule, 'metric' | 'min' | 'max'>,
  event: Pick<TelemetryEvent, 'metric' | 'value'>,
): { kind: ViolationKind; bound: number } | null {
  if (event.metric !== rule.metric) return null;
  if (rule.min !== undefined && event.value < rule.min) return { kind: 'below-min', bound: rule.min };
  if (rule.max !== undefined && event.value > rule.max) return { kind: 'above-max', bound: rule.max };
  return null;
}

function describe(kind: ViolationKind, value: number, bound: number): string {
  return kind === 'below-min' ? `value ${value} is below minimum ${bound}` : `value ${value} is above maximum ${bound}`;
}

/** An ordered, validated set of rules with per-source cooldowns. */
export class RuleSet {
  private rules: CompiledRule[];
  /** `rule \0 source` -> event timestamp of the last finding. Insertion order gives eviction order. */
  private readonly lastFired = new Map<string, number>();
  private readonly counters = { evaluated: 0, matched: 0, suppressed: 0 };

  /**
   * @throws {RuleError} If any rule is invalid or a name is duplicated.
   */
  constructor(definitions: Iterable<readonly [string, unknown]> = []) {
    this.rules = compileAll(definitions);
  }

  get size(): number {
    return this.rules.length;
  }

  /**
   * Replaces the active rules. The new set is validated first, so on error the
   * previous rules stay in force. Cooldown state is kept for rules that survive
   * the update and dropped for rules that do not.
   *
   * @throws {RuleError} If any new rule is invalid.
   */
  update(definitions: Iterable<readonly [string, unknown]>): void {
    const next = compileAll(definitions);
    const names = new Set(next.map((r) => r.name));
    for (const key of [...this.lastFired.keys()]) {
      const rule = key.slice(0, key.indexOf('\u0000'));
      if (!names.has(rule)) this.lastFired.delete(key);
    }
    this.rules = next;
  }

  /**
   * Checks an event against every rule. Returns the findings that survive
   * cooldown, in rule order.
   */
  evaluate(event: TelemetryEvent): RuleFinding[] {
    this.counters.evaluated++;
    const findings: RuleFinding[] = [];
    for (const rule of this.rules) {
      const violation = violationOf(rule, event);
      if (violation === null) continue;
      this.counters.matched++;

      const key = `${rule.name}\u0000${event.source}`;
      const last = this.lastFired.get(key);
      if (rule.cooldownMs > 0 && last !== undefined && event.timestamp - last < rule.cooldownMs) {
        this.counters.suppressed++;
        continue;
      }

      this.remember(key, event.timestamp);
      findings.push({
        rule: rule.name,
        kind: violation.kind,
        severity: rule.severity,
        eventId: event.id,
        source: event.source,
        metric: event.metric,
        value: event.value,
        bound: violation.bound,
        timestamp: event.timestamp,
        message: describe(violation.kind, event.value, violation.bound),
      });
    }
    return findings;
  }

  stats(): RuleSetStats {
    return Object.freeze({
      rules: this.rules.length,
      evaluated: this.counters.evaluated,
      matched: this.counters.matched,
      suppressed: this.counters.suppressed,
      trackedSeries: this.lastFired.size,
    });
  }

  private remember(key: string, timestamp: number): void {
    // Re-inserting moves the key to the end, so eviction always removes the stalest entry.
    this.lastFired.delete(key);
    this.lastFired.set(key, timestamp);
    while (this.lastFired.size > MAX_TRACKED_SERIES) {
      const oldest = this.lastFired.keys().next();
      if (oldest.done === true) break;
      this.lastFired.delete(oldest.value);
    }
  }
}
