/**
 * NexusPulse structured logger.
 *
 * Emits one JSON object per line through an injectable sink. Logging calls
 * never throw: serialisation failures degrade to a minimal fallback record and
 * every drop or failure is counted and exposed through {@link Logger.stats}.
 *
 * Record shape: `{ time, level, msg, ...bindings, ...fields }`. The keys
 * `time`, `level` and `msg` are reserved and cannot be overridden.
 */

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Base class for errors raised intentionally by the logger module. */
export class LoggerError extends Error {
  public readonly code: string;
  public readonly details: Readonly<Record<string, unknown>>;

  /**
   * @param code Stable machine-readable error code.
   * @param message Human-readable description.
   * @param details Extra context; copied and frozen.
   */
  constructor(code: string, message: string, details: Record<string, unknown> = {}) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = new.target.name;
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

/** Thrown by {@link createLogger} when an option has an invalid value. Code: LOGGER_INVALID_OPTION. */
export class InvalidLoggerOptionError extends LoggerError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('LOGGER_INVALID_OPTION', message, details);
  }
}

/** Thrown by {@link Logger.child} when bindings are not a plain object. Code: LOGGER_INVALID_BINDINGS. */
export class InvalidBindingsError extends LoggerError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('LOGGER_INVALID_BINDINGS', message, details);
  }
}

// ---------------------------------------------------------------------------
// Types and constants
// ---------------------------------------------------------------------------

/** Supported severity levels, ordered debug < info < warn < error. */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/** Function receiving each serialised record (without trailing newline). */
export type LogSink = (line: string) => void;

/** Numeric rank of every level; higher is more severe. */
export const LOG_LEVEL_RANK: Readonly<Record<LogLevel, number>> = Object.freeze({
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
});

/** Keys redacted when no `redactKeys` option is supplied (compared case-insensitively). */
export const DEFAULT_REDACT_KEYS: readonly string[] = Object.freeze([
  'password',
  'passwd',
  'secret',
  'token',
  'authorization',
  'apikey',
  'api_key',
  'cookie',
  'accesstoken',
  'refreshtoken',
  'privatekey',
]);

/** Replacement value for redacted fields. */
export const REDACTED = '[REDACTED]';
/** Replacement for values nested deeper than {@link MAX_LOG_DEPTH}. */
export const MAX_DEPTH_MARKER = '[MaxDepth]';
/** Replacement for circular references. */
export const CIRCULAR_MARKER = '[Circular]';
/** Maximum container nesting depth kept in a record; deeper values are replaced. */
export const MAX_LOG_DEPTH = 8;
/** Default maximum size of a serialised line in bytes. */
export const DEFAULT_MAX_LINE_BYTES = 16 * 1024;
/** Smallest accepted `maxLineBytes`. */
export const MIN_MAX_LINE_BYTES = 256;

const RESERVED_KEYS: ReadonlySet<string> = new Set(['level', 'time', 'msg']);
const LEVELS: readonly LogLevel[] = ['debug', 'info', 'warn', 'error'];

/** Options accepted by {@link createLogger}. */
export interface LoggerOptions {
  /** Minimum level emitted. Default `info`. */
  level?: LogLevel;
  /** Destination for each line. Default writes the line plus newline to stdout. */
  sink?: LogSink;
  /** Case-insensitive key names to redact. Replaces {@link DEFAULT_REDACT_KEYS}. */
  redactKeys?: readonly string[];
  /** Include error stacks when serialising errors. Default false. */
  includeStack?: boolean;
  /** Maximum serialised line size in bytes (min 256). Default 16 KiB. */
  maxLineBytes?: number;
  /** Per-level keep probability in [0,1]. Missing levels keep every record. */
  sampling?: Partial<Record<LogLevel, number>>;
  /** Random source returning [0,1). Default Math.random. */
  random?: () => number;
  /** Wall-clock source in epoch milliseconds. Default Date.now. */
  clock?: () => number;
  /** Fields bound to every record of the root logger. */
  bindings?: Record<string, unknown>;
}

/** Counters describing what the logger did with records. */
export interface LoggerStats {
  /** Records delivered to the sink without the sink throwing. */
  emitted: number;
  /** Records skipped because they were below the level threshold. */
  filtered: number;
  /** Records skipped by sampling. */
  sampledOut: number;
  /** Records whose line exceeded the size limit and was truncated. */
  truncated: number;
  /** Records for which serialisation, timestamping or processing failed. */
  failed: number;
  /** Records lost because the sink threw or rejected. */
  sinkErrors: number;
  /** Binding or field keys ignored because they were reserved. */
  reservedKeyConflicts: number;
  /** Records never delivered: sampledOut + sinkErrors + unrecoverable failures. */
  dropped: number;
}

interface Core {
  sink: LogSink;
  redact: ReadonlySet<string>;
  includeStack: boolean;
  maxLineBytes: number;
  sampling: Readonly<Partial<Record<LogLevel, number>>>;
  random: () => number;
  clock: () => number;
  level: LogLevel;
  counters: LoggerStats;
}

type Dict = Record<string, unknown>;

// ---------------------------------------------------------------------------
// Sanitisation
// ---------------------------------------------------------------------------

interface SanitizeConfig {
  redact: ReadonlySet<string>;
  includeStack: boolean;
}

function describeError(err: unknown): string {
  try {
    return err instanceof Error ? err.message : String(err);
  } catch {
    return 'unknown';
  }
}

function serialiseError(err: Error, cfg: SanitizeConfig): Dict {
  const out: Dict = Object.create(null) as Dict;
  out['name'] = safeRead(() => String(err.name));
  out['message'] = safeRead(() => String(err.message));
  const code = safeRead(() => (err as { code?: unknown }).code);
  if (code !== undefined && code !== null) {
    out['code'] = typeof code === 'string' || typeof code === 'number' ? code : String(code);
  }
  if (cfg.includeStack) {
    const stack = safeRead(() => err.stack);
    if (typeof stack === 'string') out['stack'] = stack;
  }
  return out;
}

function safeRead<T>(fn: () => T): T | string {
  try {
    return fn();
  } catch (e) {
    return `[Unserializable: ${describeError(e)}]`;
  }
}

function sanitize(value: unknown, depth: number, ancestors: Set<object>, cfg: SanitizeConfig): unknown {
  switch (typeof value) {
    case 'string':
    case 'boolean':
    case 'undefined':
      return value;
    case 'number':
      return Number.isFinite(value) ? value : String(value);
    case 'bigint':
      return value.toString();
    case 'symbol':
      return value.toString();
    case 'function':
      return `[Function: ${value.name || 'anonymous'}]`;
    default:
      break;
  }
  if (value === null) return null;
  const obj = value as object;
  if (ancestors.has(obj)) return CIRCULAR_MARKER;
  if (depth > MAX_LOG_DEPTH) return MAX_DEPTH_MARKER;
  if (obj instanceof Date) {
    return Number.isNaN(obj.getTime()) ? '[InvalidDate]' : obj.toISOString();
  }
  if (ArrayBuffer.isView(obj)) return `[Binary ${obj.byteLength} bytes]`;
  if (obj instanceof ArrayBuffer) return `[Binary ${obj.byteLength} bytes]`;
  ancestors.add(obj);
  try {
    if (obj instanceof Error) return serialiseError(obj, cfg);
    if (Array.isArray(obj)) {
      const out: unknown[] = [];
      const len = obj.length;
      for (let i = 0; i < len; i++) {
        try {
          const item: unknown = obj[i];
          const v = sanitize(item, depth + 1, ancestors, cfg);
          out.push(v === undefined ? null : v);
        } catch (e) {
          out.push(`[Unserializable: ${describeError(e)}]`);
        }
      }
      return out;
    }
    if (obj instanceof Set) {
      return sanitize(Array.from(obj), depth, new Set(ancestors).add(obj), cfg);
    }
    const out: Dict = Object.create(null) as Dict;
    const entries: Array<[string, () => unknown]> =
      obj instanceof Map
        ? Array.from(obj.entries(), ([k, v]): [string, () => unknown] => [String(k), () => v])
        : Object.keys(obj).map((k): [string, () => unknown] => [k, () => (obj as Dict)[k]]);
    for (const [key, read] of entries) {
      if (cfg.redact.has(key.toLowerCase())) {
        out[key] = REDACTED;
        continue;
      }
      try {
        out[key] = sanitize(read(), depth + 1, ancestors, cfg);
      } catch (e) {
        out[key] = `[Unserializable: ${describeError(e)}]`;
      }
    }
    return out;
  } catch (e) {
    return `[Unserializable: ${describeError(e)}]`;
  } finally {
    ancestors.delete(obj);
  }
}

/**
 * Convert an arbitrary value into a JSON-safe structure using the same rules
 * the logger applies: redaction, depth limit, cycle detection, BigInt to string
 * and error serialisation. Never throws.
 *
 * @param value Value to convert.
 * @param redactKeys Case-insensitive keys to redact. Default {@link DEFAULT_REDACT_KEYS}.
 * @param includeStack Include error stacks. Default false.
 * @returns A JSON-safe value (plain objects, arrays, primitives).
 */
export function toLoggable(
  value: unknown,
  redactKeys: readonly string[] = DEFAULT_REDACT_KEYS,
  includeStack = false,
): unknown {
  const cfg: SanitizeConfig = { redact: new Set(redactKeys.map((k) => k.toLowerCase())), includeStack };
  return sanitize(value, 1, new Set<object>(), cfg);
}

function isPlainObject(v: unknown): v is Dict {
  if (typeof v !== 'object' || v === null || Array.isArray(v)) return false;
  const proto: unknown = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
}

function byteLength(s: string): number {
  return Buffer.byteLength(s, 'utf8');
}

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

/**
 * Create a root logger.
 *
 * @param options Logger configuration; see {@link LoggerOptions}.
 * @returns A configured {@link Logger}.
 * @throws {InvalidLoggerOptionError} If level, sink, redactKeys, maxLineBytes,
 *   sampling, random, clock or bindings are invalid.
 */
export function createLogger(options: LoggerOptions = {}): Logger {
  return new Logger(options);
}

function isLevel(v: unknown): v is LogLevel {
  return typeof v === 'string' && (LEVELS as readonly string[]).includes(v);
}

/** Structured JSON-lines logger. See the module documentation. */
export class Logger {
  private readonly core: Core;
  private readonly bound: Dict;
  private ownLevel: LogLevel | undefined;

  /**
   * Construct a root logger. Prefer {@link createLogger}.
   *
   * @param options Logger configuration.
   * @throws {InvalidLoggerOptionError} On invalid options.
   * @throws {InvalidBindingsError} If `options.bindings` is not a plain object.
   */
  constructor(options: LoggerOptions = {}) {
    const level = options.level ?? 'info';
    if (!isLevel(level)) throw new InvalidLoggerOptionError('level must be debug, info, warn or error', { level });
    const sink = options.sink ?? ((line: string): void => void process.stdout.write(line + '\n'));
    if (typeof sink !== 'function') throw new InvalidLoggerOptionError('sink must be a function');
    const redactKeys = options.redactKeys ?? DEFAULT_REDACT_KEYS;
    if (!Array.isArray(redactKeys) || redactKeys.some((k) => typeof k !== 'string' || k.length === 0)) {
      throw new InvalidLoggerOptionError('redactKeys must be an array of non-empty strings');
    }
    const maxLineBytes = options.maxLineBytes ?? DEFAULT_MAX_LINE_BYTES;
    if (!Number.isInteger(maxLineBytes) || maxLineBytes < MIN_MAX_LINE_BYTES) {
      throw new InvalidLoggerOptionError(`maxLineBytes must be an integer >= ${MIN_MAX_LINE_BYTES}`, { maxLineBytes });
    }
    const sampling: Partial<Record<LogLevel, number>> = {};
    const rawSampling: Record<string, unknown> = options.sampling ?? {};
    for (const key of Object.keys(rawSampling)) {
      const rate = rawSampling[key];
      if (!isLevel(key)) throw new InvalidLoggerOptionError('sampling has an unknown level', { level: key });
      if (typeof rate !== 'number' || !Number.isFinite(rate) || rate < 0 || rate > 1) {
        throw new InvalidLoggerOptionError('sampling rate must be a number in [0,1]', { level: key, rate });
      }
      sampling[key] = rate;
    }
    const random = options.random ?? Math.random;
    if (typeof random !== 'function') throw new InvalidLoggerOptionError('random must be a function');
    const clock = options.clock ?? Date.now;
    if (typeof clock !== 'function') throw new InvalidLoggerOptionError('clock must be a function');
    this.core = {
      sink,
      redact: new Set(redactKeys.map((k) => k.toLowerCase())),
      includeStack: options.includeStack ?? false,
      maxLineBytes,
      sampling,
      random,
      clock,
      level,
      counters: {
        emitted: 0,
        filtered: 0,
        sampledOut: 0,
        truncated: 0,
        failed: 0,
        sinkErrors: 0,
        reservedKeyConflicts: 0,
        dropped: 0,
      },
    };
    this.bound = this.buildBound({}, options.bindings ?? {});
  }

  private buildBound(parent: Dict, bindings: unknown): Dict {
    if (!isPlainObject(bindings)) throw new InvalidBindingsError('bindings must be a plain object');
    const merged: Dict = Object.assign(Object.create(null) as Dict, parent);
    this.mergeInto(merged, bindings);
    return merged;
  }

  private mergeInto(target: Dict, source: Dict): void {
    const cfg: SanitizeConfig = { redact: this.core.redact, includeStack: this.core.includeStack };
    for (const key of Object.keys(source)) {
      if (RESERVED_KEYS.has(key)) {
        this.core.counters.reservedKeyConflicts++;
        continue;
      }
      if (this.core.redact.has(key.toLowerCase())) {
        target[key] = REDACTED;
        continue;
      }
      try {
        target[key] = sanitize(source[key], 1, new Set<object>(), cfg);
      } catch (e) {
        target[key] = `[Unserializable: ${describeError(e)}]`;
      }
    }
  }

  /** Effective level threshold of this logger. */
  get level(): LogLevel {
    return this.ownLevel ?? this.core.level;
  }

  /**
   * Change the level threshold. On the root logger this affects every child
   * that has not set its own level.
   *
   * @param level New threshold.
   * @throws {InvalidLoggerOptionError} If the level is not valid.
   */
  setLevel(level: LogLevel): void {
    if (!isLevel(level)) throw new InvalidLoggerOptionError('level must be debug, info, warn or error', { level });
    if (this === this.rootRef) this.core.level = level;
    else this.ownLevel = level;
  }

  private rootRef: Logger = this;

  /**
   * @param level Level to test.
   * @returns True when a record at `level` passes the threshold (sampling not considered).
   */
  isLevelEnabled(level: LogLevel): boolean {
    return isLevel(level) && LOG_LEVEL_RANK[level] >= LOG_LEVEL_RANK[this.level];
  }

  /**
   * Create a child logger sharing sink, configuration and statistics. Bindings
   * are snapshotted, sanitised and merged over the parent's bindings.
   *
   * @param bindings Fields added to every record. Reserved keys are ignored and counted.
   * @returns The child logger.
   * @throws {InvalidBindingsError} If `bindings` is not a plain object.
   */
  child(bindings: Record<string, unknown>): Logger {
    const merged = this.buildBound(this.bound, bindings);
    const child = Object.create(Logger.prototype) as { core: Core; bound: Dict; ownLevel: LogLevel | undefined; rootRef: Logger };
    child.core = this.core;
    child.bound = merged;
    child.ownLevel = this.ownLevel;
    child.rootRef = this.rootRef;
    return child as unknown as Logger;
  }

  /** Log at debug level. Never throws. @param msg Message. @param fields Extra fields. */
  debug(msg: string, fields?: unknown): void {
    this.log('debug', msg, fields);
  }

  /** Log at info level. Never throws. @param msg Message. @param fields Extra fields. */
  info(msg: string, fields?: unknown): void {
    this.log('info', msg, fields);
  }

  /** Log at warn level. Never throws. @param msg Message. @param fields Extra fields. */
  warn(msg: string, fields?: unknown): void {
    this.log('warn', msg, fields);
  }

  /** Log at error level. Never throws. @param msg Message. @param fields Extra fields. */
  error(msg: string, fields?: unknown): void {
    this.log('error', msg, fields);
  }

  /**
   * Log a record. This method never throws; failures are counted in {@link stats}.
   *
   * @param level Severity of the record.
   * @param msg Message; coerced to string.
   * @param fields Plain object of extra fields. An Error is logged under `err`;
   *   any other value is logged under `value`.
   */
  log(level: LogLevel, msg: string, fields?: unknown): void {
    const c = this.core.counters;
    try {
      if (!isLevel(level)) {
        c.failed++;
        c.dropped++;
        return;
      }
      if (LOG_LEVEL_RANK[level] < LOG_LEVEL_RANK[this.level]) {
        c.filtered++;
        return;
      }
      const rate = this.core.sampling[level];
      if (rate !== undefined && rate < 1) {
        const r = this.core.random();
        if (!(r < rate)) {
          c.sampledOut++;
          c.dropped++;
          return;
        }
      }
      const line = this.render(level, msg, fields);
      this.deliver(line);
    } catch {
      c.failed++;
      c.dropped++;
    }
  }

  private timestamp(): string {
    try {
      return new Date(this.core.clock()).toISOString();
    } catch {
      this.core.counters.failed++;
      return new Date().toISOString();
    }
  }

  private render(level: LogLevel, msg: unknown, fields: unknown): string {
    const time = this.timestamp();
    const c = this.core.counters;
    try {
      const record: Dict = { time, level, msg: String(msg) };
      for (const key of Object.keys(this.bound)) record[key] = this.bound[key];
      const extra: Dict = Object.create(null) as Dict;
      if (fields instanceof Error) this.mergeInto(extra, { err: fields });
      else if (isPlainObject(fields)) this.mergeInto(extra, fields);
      else if (fields !== undefined) this.mergeInto(extra, { value: fields });
      for (const key of Object.keys(extra)) record[key] = extra[key];
      const line = JSON.stringify(record);
      if (byteLength(line) <= this.core.maxLineBytes) return line;
      c.truncated++;
      return this.truncate(time, level, record['msg'] as string, byteLength(line));
    } catch {
      c.failed++;
      return JSON.stringify({ time, level, msg: '[LogSerializationError]', serializationFailed: true });
    }
  }

  private truncate(time: string, level: LogLevel, msg: string, originalBytes: number): string {
    const max = this.core.maxLineBytes;
    const build = (n: number): string =>
      JSON.stringify({ time, level, msg: msg.slice(0, n), truncated: true, originalBytes });
    let lo = 0;
    let hi = msg.length;
    while (lo < hi) {
      const mid = Math.ceil((lo + hi) / 2);
      if (byteLength(build(mid)) <= max) lo = mid;
      else hi = mid - 1;
    }
    const code = lo > 0 ? msg.charCodeAt(lo - 1) : 0;
    if (code >= 0xd800 && code <= 0xdbff) lo--;
    return build(lo);
  }

  private deliver(line: string): void {
    const c = this.core.counters;
    try {
      const result = this.core.sink(line) as unknown;
      if (result !== null && typeof result === 'object' && typeof (result as { then?: unknown }).then === 'function') {
        (result as PromiseLike<unknown>).then(undefined, () => {
          c.sinkErrors++;
          c.dropped++;
        });
      }
      c.emitted++;
    } catch {
      c.sinkErrors++;
      c.dropped++;
    }
  }

  /** @returns A snapshot of the shared counters (root and all children). */
  stats(): Readonly<LoggerStats> {
    return Object.freeze({ ...this.core.counters });
  }
}
