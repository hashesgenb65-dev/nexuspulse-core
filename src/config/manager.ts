/**
 * NexusPulse configuration center and feature flag engine.
 *
 * Resolution order for a feature flag, first match wins:
 *   1. Unknown flag: disabled (reason `unknown-flag`).
 *   2. Tenant override for the context's tenant (reason `tenant-override`).
 *   3. Rules in declaration order. For each rule:
 *      a. a target segment matches the context (reason `segment`), or
 *      b. the user's sticky bucket falls inside the rule's percentage rollout
 *         (reason `rollout`).
 *   4. The flag's default (reason `default`).
 *
 * Rollout buckets come from a hash of the flag key and user id, so a user's
 * cohort is stable across calls and independent between flags.
 *
 * Every mutation bumps the version and notifies change listeners. A listener
 * that throws does not affect the mutation or the other listeners.
 */

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Stable error codes raised by the configuration engine. */
export type ConfigurationErrorCode =
  | 'CONFIG_INVALID_KEY'
  | 'CONFIG_INVALID_VALUE'
  | 'CONFIG_INVALID_FLAG'
  | 'CONFIG_UNKNOWN_FLAG'
  | 'CONFIG_INVALID_CONTEXT'
  | 'CONFIG_INVALID_TENANT';

/** Raised for invalid input. The `code` is stable; the message is for humans. */
export class ConfigurationError extends Error {
  public readonly code: ConfigurationErrorCode;
  public readonly details: Readonly<Record<string, unknown>>;

  constructor(code: ConfigurationErrorCode, message: string, details: Record<string, unknown> = {}) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'ConfigurationError';
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface FeatureFlagRule {
  ruleId: string;
  targetSegments: string[]; // e.g., ['beta_testers', 'enterprise']
  percentageRollout: number; // 0 to 100, fractions allowed
  enabled: boolean;
}

export interface FeatureFlagDefinition {
  flagKey: string;
  description: string;
  defaultEnabled: boolean;
  rules: FeatureFlagRule[];
}

export interface EvaluationContext {
  userId: string;
  tenantId: string;
  segments?: readonly string[];
}

export type FlagReason = 'unknown-flag' | 'tenant-override' | 'segment' | 'rollout' | 'default';

export interface FlagDecision {
  readonly enabled: boolean;
  readonly reason: FlagReason;
  /** The rule that decided the result, for `segment` and `rollout` decisions. */
  readonly ruleId?: string;
}

export type ConfigChange =
  | { readonly kind: 'config'; readonly action: 'set' | 'delete'; readonly key: string; readonly version: number }
  | {
      readonly kind: 'flag';
      readonly action: 'register' | 'unregister';
      readonly flagKey: string;
      readonly version: number;
    }
  | {
      readonly kind: 'override';
      readonly action: 'set' | 'clear';
      readonly flagKey: string;
      readonly tenantId: string;
      readonly version: number;
    };

export type ChangeListener = (change: ConfigChange) => void;

/** Omit applied to each member of a union, so discriminated members keep their own fields. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

// ---------------------------------------------------------------------------
// Limits and patterns
// ---------------------------------------------------------------------------

/** Number of rollout buckets. Percentages resolve to 0.01% steps. */
export const BUCKET_COUNT = 10_000;
export const MAX_RULES_PER_FLAG = 100;
export const MAX_SEGMENTS_PER_RULE = 50;
export const MAX_DESCRIPTION_LENGTH = 500;

const KEY_PATTERN = /^[A-Za-z][A-Za-z0-9_.-]{0,127}$/;
const SEGMENT_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;
const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** 32-bit FNV-1a. Deterministic across processes, unlike a seeded hash. */
function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

function invalidFlag(message: string, details: Record<string, unknown> = {}): ConfigurationError {
  return new ConfigurationError('CONFIG_INVALID_FLAG', message, details);
}

function assertConfigKey(key: unknown): asserts key is string {
  if (typeof key !== 'string' || !KEY_PATTERN.test(key)) {
    throw new ConfigurationError('CONFIG_INVALID_KEY', 'config key must match /^[A-Za-z][A-Za-z0-9_.-]{0,127}$/', {
      key: typeof key === 'string' ? key : String(key),
    });
  }
}

function assertFlagKey(flagKey: unknown): asserts flagKey is string {
  if (typeof flagKey !== 'string' || !KEY_PATTERN.test(flagKey)) {
    throw invalidFlag('flagKey must match /^[A-Za-z][A-Za-z0-9_.-]{0,127}$/', {
      flagKey: typeof flagKey === 'string' ? flagKey : String(flagKey),
    });
  }
}

function assertTenantId(tenantId: unknown): asserts tenantId is string {
  if (typeof tenantId !== 'string' || !ID_PATTERN.test(tenantId)) {
    throw new ConfigurationError('CONFIG_INVALID_TENANT', 'tenantId must be 1-128 letters, digits or _.:- characters', {
      tenantId: typeof tenantId === 'string' ? tenantId : String(tenantId),
    });
  }
}

/** Copies a config value through JSON so later mutation on either side cannot leak. */
function toJsonText(key: string, value: unknown): string {
  if (value === undefined || typeof value === 'function' || typeof value === 'symbol') {
    throw new ConfigurationError('CONFIG_INVALID_VALUE', `value for "${key}" is not JSON-serialisable`, { key });
  }
  let text: string | undefined;
  try {
    text = JSON.stringify(value);
  } catch (err) {
    throw new ConfigurationError('CONFIG_INVALID_VALUE', `value for "${key}" is not JSON-serialisable`, {
      key,
      reason: (err as Error).message,
    });
  }
  if (text === undefined) {
    throw new ConfigurationError('CONFIG_INVALID_VALUE', `value for "${key}" is not JSON-serialisable`, { key });
  }
  return text;
}

function validateRule(raw: unknown, flagKey: string, index: number, seenIds: Set<string>): FeatureFlagRule {
  if (!isRecord(raw)) throw invalidFlag(`rule ${index} must be an object`, { flagKey, index });

  const ruleId = raw['ruleId'];
  if (typeof ruleId !== 'string' || !ID_PATTERN.test(ruleId)) {
    throw invalidFlag(`rule ${index} has an invalid ruleId`, { flagKey, index });
  }
  if (seenIds.has(ruleId)) throw invalidFlag(`duplicate ruleId "${ruleId}"`, { flagKey, ruleId });
  seenIds.add(ruleId);

  const segmentsIn = raw['targetSegments'];
  if (!Array.isArray(segmentsIn) || segmentsIn.length > MAX_SEGMENTS_PER_RULE) {
    throw invalidFlag(`rule "${ruleId}" targetSegments must be an array of at most ${MAX_SEGMENTS_PER_RULE} entries`, {
      flagKey,
      ruleId,
    });
  }
  const segments: string[] = [];
  for (const segment of segmentsIn) {
    if (typeof segment !== 'string' || !SEGMENT_PATTERN.test(segment)) {
      throw invalidFlag(`rule "${ruleId}" has an invalid segment`, { flagKey, ruleId });
    }
    if (!segments.includes(segment)) segments.push(segment);
  }

  const rollout = raw['percentageRollout'];
  if (typeof rollout !== 'number' || !Number.isFinite(rollout) || rollout < 0 || rollout > 100) {
    throw invalidFlag(`rule "${ruleId}" percentageRollout must be a number from 0 to 100`, { flagKey, ruleId });
  }

  const enabled = raw['enabled'];
  if (typeof enabled !== 'boolean') throw invalidFlag(`rule "${ruleId}" enabled must be a boolean`, { flagKey, ruleId });

  if (segments.length === 0 && rollout === 0) {
    throw invalidFlag(`rule "${ruleId}" has neither target segments nor a rollout, so it can never match`, {
      flagKey,
      ruleId,
    });
  }

  return { ruleId, targetSegments: segments, percentageRollout: rollout, enabled };
}

/** Validates a flag definition and returns an independent copy of it. */
function validateFlag(input: unknown): FeatureFlagDefinition {
  if (!isRecord(input)) throw invalidFlag('flag definition must be an object');
  const flagKey = input['flagKey'];
  assertFlagKey(flagKey);

  const description = input['description'];
  if (typeof description !== 'string' || description.length > MAX_DESCRIPTION_LENGTH) {
    throw invalidFlag(`description must be a string of at most ${MAX_DESCRIPTION_LENGTH} characters`, { flagKey });
  }
  const defaultEnabled = input['defaultEnabled'];
  if (typeof defaultEnabled !== 'boolean') throw invalidFlag('defaultEnabled must be a boolean', { flagKey });

  const rulesIn = input['rules'];
  if (!Array.isArray(rulesIn) || rulesIn.length > MAX_RULES_PER_FLAG) {
    throw invalidFlag(`rules must be an array of at most ${MAX_RULES_PER_FLAG} entries`, { flagKey });
  }
  const seenIds = new Set<string>();
  const rules = rulesIn.map((rule, index) => validateRule(rule, flagKey, index, seenIds));

  return { flagKey, description, defaultEnabled, rules };
}

function validateContext(input: unknown): { userId: string; tenantId: string; segments: ReadonlySet<string> } {
  if (!isRecord(input)) {
    throw new ConfigurationError('CONFIG_INVALID_CONTEXT', 'evaluation context must be an object');
  }
  const userId = input['userId'];
  if (typeof userId !== 'string' || !ID_PATTERN.test(userId)) {
    throw new ConfigurationError('CONFIG_INVALID_CONTEXT', 'userId must be 1-128 letters, digits or _.:- characters');
  }
  const tenantId = input['tenantId'];
  if (typeof tenantId !== 'string' || !ID_PATTERN.test(tenantId)) {
    throw new ConfigurationError('CONFIG_INVALID_CONTEXT', 'tenantId must be 1-128 letters, digits or _.:- characters');
  }
  const segmentsIn = input['segments'];
  const segments = new Set<string>();
  if (segmentsIn !== undefined) {
    if (!Array.isArray(segmentsIn) || segmentsIn.length > MAX_SEGMENTS_PER_RULE) {
      throw new ConfigurationError('CONFIG_INVALID_CONTEXT', `segments must be an array of at most ${MAX_SEGMENTS_PER_RULE} strings`);
    }
    for (const segment of segmentsIn) {
      if (typeof segment !== 'string' || segment.length === 0 || segment.length > 64) {
        throw new ConfigurationError('CONFIG_INVALID_CONTEXT', 'each segment must be a string of 1-64 characters');
      }
      segments.add(segment);
    }
  }
  return { userId, tenantId, segments };
}

function decision(enabled: boolean, reason: FlagReason, ruleId?: string): FlagDecision {
  return Object.freeze(ruleId === undefined ? { enabled, reason } : { enabled, reason, ruleId });
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

export class ConfigurationManagerEngine {
  /** Config values as JSON text, so reads always return a fresh copy. */
  private readonly configStore = new Map<string, string>();
  private readonly flags = new Map<string, FeatureFlagDefinition>();
  /** flagKey -> tenantId -> enabled. */
  private readonly tenantOverrides = new Map<string, Map<string, boolean>>();
  private readonly listeners = new Set<ChangeListener>();
  private version: number = 1;
  private listenerErrorCount = 0;

  // -- configuration values --------------------------------------------------

  /**
   * Stores a JSON-serialisable value under a key. The value is copied, so later
   * changes to the argument do not affect the stored value.
   *
   * @throws {ConfigurationError} CONFIG_INVALID_KEY or CONFIG_INVALID_VALUE.
   */
  public setConfig(key: string, value: unknown): void {
    assertConfigKey(key);
    const text = toJsonText(key, value);
    this.configStore.set(key, text);
    this.bump({ kind: 'config', action: 'set', key });
  }

  /** Returns a fresh copy of the stored value, or `defaultValue` if the key is absent. */
  public getConfig<T>(key: string, defaultValue: T): T {
    const text = this.configStore.get(key);
    if (text === undefined) return defaultValue;
    return JSON.parse(text) as T;
  }

  public hasConfig(key: string): boolean {
    return this.configStore.has(key);
  }

  /** Removes a key. Returns false, and does not bump the version, if the key was absent. */
  public deleteConfig(key: string): boolean {
    if (!this.configStore.delete(key)) return false;
    this.bump({ kind: 'config', action: 'delete', key });
    return true;
  }

  /** Sorted config keys, optionally restricted to a prefix. */
  public listConfigKeys(prefix: string = ''): string[] {
    return [...this.configStore.keys()].filter((k) => k.startsWith(prefix)).sort();
  }

  // -- feature flags ---------------------------------------------------------

  /**
   * Registers or replaces a flag. The definition is validated and copied.
   *
   * @throws {ConfigurationError} CONFIG_INVALID_FLAG when the definition is invalid.
   */
  public registerFlag(flag: FeatureFlagDefinition): void {
    const copy = validateFlag(flag);
    this.flags.set(copy.flagKey, copy);
    this.bump({ kind: 'flag', action: 'register', flagKey: copy.flagKey });
  }

  /** Removes a flag and its tenant overrides. Returns false if it did not exist. */
  public unregisterFlag(flagKey: string): boolean {
    if (!this.flags.delete(flagKey)) return false;
    this.tenantOverrides.delete(flagKey);
    this.bump({ kind: 'flag', action: 'unregister', flagKey });
    return true;
  }

  /** A copy of the flag definition, or undefined if it is not registered. */
  public getFlag(flagKey: string): FeatureFlagDefinition | undefined {
    const flag = this.flags.get(flagKey);
    return flag === undefined ? undefined : structuredClone(flag);
  }

  /** Sorted keys of all registered flags. */
  public listFlags(): string[] {
    return [...this.flags.keys()].sort();
  }

  /**
   * Forces a flag on or off for one tenant, ahead of any rule.
   *
   * @throws {ConfigurationError} CONFIG_UNKNOWN_FLAG or CONFIG_INVALID_TENANT.
   */
  public setTenantOverride(flagKey: string, tenantId: string, enabled: boolean): void {
    if (!this.flags.has(flagKey)) {
      throw new ConfigurationError('CONFIG_UNKNOWN_FLAG', `flag "${flagKey}" is not registered`, { flagKey });
    }
    assertTenantId(tenantId);
    if (typeof enabled !== 'boolean') {
      throw new ConfigurationError('CONFIG_INVALID_VALUE', 'override value must be a boolean', { flagKey, tenantId });
    }
    let byTenant = this.tenantOverrides.get(flagKey);
    if (byTenant === undefined) {
      byTenant = new Map<string, boolean>();
      this.tenantOverrides.set(flagKey, byTenant);
    }
    byTenant.set(tenantId, enabled);
    this.bump({ kind: 'override', action: 'set', flagKey, tenantId });
  }

  /** Removes a tenant override. Returns false if none was set. */
  public clearTenantOverride(flagKey: string, tenantId: string): boolean {
    const byTenant = this.tenantOverrides.get(flagKey);
    if (byTenant === undefined || !byTenant.delete(tenantId)) return false;
    if (byTenant.size === 0) this.tenantOverrides.delete(flagKey);
    this.bump({ kind: 'override', action: 'clear', flagKey, tenantId });
    return true;
  }

  /**
   * Evaluates a flag for a context.
   *
   * @throws {ConfigurationError} CONFIG_INVALID_CONTEXT when the context is malformed.
   */
  public evaluateFlag(flagKey: string, context: EvaluationContext): boolean {
    return this.explainFlag(flagKey, context).enabled;
  }

  /**
   * Evaluates a flag and reports which step decided the result.
   *
   * @throws {ConfigurationError} CONFIG_INVALID_CONTEXT when the context is malformed.
   */
  public explainFlag(flagKey: string, context: EvaluationContext): FlagDecision {
    const ctx = validateContext(context);
    const flag = this.flags.get(flagKey);
    if (flag === undefined) return decision(false, 'unknown-flag');

    const override = this.tenantOverrides.get(flagKey)?.get(ctx.tenantId);
    if (override !== undefined) return decision(override, 'tenant-override');

    const bucket = this.bucketOf(flagKey, ctx.userId);
    for (const rule of flag.rules) {
      if (rule.targetSegments.some((segment) => ctx.segments.has(segment))) {
        return decision(rule.enabled, 'segment', rule.ruleId);
      }
      if (rule.percentageRollout > 0 && bucket < (rule.percentageRollout * BUCKET_COUNT) / 100) {
        return decision(rule.enabled, 'rollout', rule.ruleId);
      }
    }
    return decision(flag.defaultEnabled, 'default');
  }

  // -- change notification ---------------------------------------------------

  /**
   * Subscribes to changes. Returns a function that unsubscribes.
   * A listener that throws is counted in {@link listenerErrors} and skipped.
   */
  public onChange(listener: ChangeListener): () => void {
    if (typeof listener !== 'function') throw new TypeError('listener must be a function');
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Number of listener invocations that threw. */
  public get listenerErrors(): number {
    return this.listenerErrorCount;
  }

  public getVersion(): number {
    return this.version;
  }

  // -- internals -------------------------------------------------------------

  private bump(change: DistributiveOmit<ConfigChange, 'version'>): void {
    this.version++;
    const full = { ...change, version: this.version } as ConfigChange;
    for (const listener of [...this.listeners]) {
      try {
        listener(full);
      } catch {
        this.listenerErrorCount++;
      }
    }
  }

  private bucketOf(flagKey: string, userId: string): number {
    return fnv1a(`${flagKey}\u0000${userId}`) % BUCKET_COUNT;
  }
}
