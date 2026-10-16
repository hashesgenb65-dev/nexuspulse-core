/**
 * @module sandbox
 * @description Secure plugin execution sandbox for NexusPulse.
 *
 * ## Isolation model (read this first)
 *
 * Node's `vm` module is NOT a security boundary against hostile code: a
 * determined plugin can escape a bare `vm` context. This sandbox therefore
 * layers several independent controls, and each one is enforced by the host
 * rather than trusted to the plugin:
 *
 *   1. **Process-level isolation.** Every plugin runs in its own child process.
 *      A plugin can block or exhaust its own process but cannot block the host
 *      event loop or corrupt the host heap.
 *   2. **Memory limit.** Each child is started with `--max-old-space-size`, which
 *      V8 enforces as a hard process limit. Exceeding it aborts only that child,
 *      which the host classifies as `PluginMemoryLimitError`.
 *      (Worker-thread `resourceLimits.maxOldGenerationSizeMb` was evaluated and
 *      rejected: on the Node version this module targets it is not enforced, so
 *      a runaway plugin could exhaust the whole host.)
 *   3. **Execution watchdog.** Every invocation has a host-side timer. If a
 *      plugin does not reply in time (including a synchronous `while (true)`),
 *      the host sends SIGKILL to the child, which stops it regardless of what
 *      the plugin is doing. The instance is then faulted and must be restarted
 *      explicitly.
 *   4. **Capability-based API.** The intended way to touch the outside world is
 *      `api.*`, which sends a message to the host. The host checks the requested
 *      capability against the plugin's manifest on every call and denies anything
 *      not declared. Host policy (`grant`) further caps what any plugin may ever
 *      declare. This is an authorisation layer for cooperating plugins: it is not
 *      what stops a malicious one (see the limits below).
 *   5. **Least-privilege process.** The child runs with Node's permission model
 *      (`--permission`, with no grants), so even code that escapes the plugin
 *      realm cannot read or write files, spawn processes, or load native addons.
 *      It also starts with an empty environment, so host secrets in `process.env`
 *      are not reachable.
 *   6. **Integrity pinning.** A manifest may pin the SHA-256 of the plugin
 *      source. Mismatched source is rejected before a process is spawned.
 *   7. **Resource quotas.** Per-plugin KV storage has a byte quota, and the log
 *      buffer is bounded.
 *
 * ### Known limits (do not rely on the sandbox beyond these)
 *
 * - **The `vm` context is not a security boundary.** Host functions handed to a
 *   plugin (the `api` object) carry their realm's `Function` constructor, so a
 *   hostile plugin can reach the child's `process` object. Control 5 contains
 *   that escape to "no filesystem, no subprocesses, no native code, no
 *   environment secrets". It does NOT stop network access, and the escaped code
 *   can still send forged IPC messages to the host. The host still validates
 *   every capability on each forged call, but results can be spoofed.
 * - Fully containing a hostile plugin needs an OS-level boundary (a container
 *   with no network, seccomp, or a separate unprivileged user). That is
 *   deployment infrastructure and outside this module's scope. Treat plugin
 *   code as trusted-but-fallible unless it runs inside such a boundary.
 *
 * ## Plugin contract
 *
 * A plugin is a script that assigns functions to `module.exports`:
 *
 * ```js
 * module.exports.double = async (args) => args.value * 2;
 * module.exports.remember = async (args) => api.kvSet('last', args);
 * ```
 *
 * Available to the plugin as globals: `module`, and the frozen `api` object
 * (`log`, `kvGet`, `kvSet`, `kvDelete`, `now`). Each `api` method requires the
 * matching capability (`log`, `kv`, or `clock`) in the manifest.
 *
 * ## Hot reload
 *
 * `reload` starts the replacement process first and only swaps it in after the
 * new source loads successfully. Calls already in flight on the old process run
 * to completion before that process is terminated. The per-plugin KV store and
 * log buffer belong to the sandbox, not the process, so state survives reloads.
 * A failed reload leaves the previous version running (rollback by construction).
 *
 * @packageDocumentation
 */

import { spawn, type ChildProcess } from 'node:child_process';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';

// ---------------------------------------------------------------------------
// Constants and policy
// ---------------------------------------------------------------------------

/** Every capability the host knows how to grant. */
export const CAPABILITIES = ['log', 'kv', 'clock'] as const;

/** A capability name. */
export type Capability = (typeof CAPABILITIES)[number];

/** Log severities a plugin may emit. */
export const LOG_LEVELS = ['debug', 'info', 'warn', 'error'] as const;

/** A plugin log severity. */
export type LogLevel = (typeof LOG_LEVELS)[number];

/** Resolved, bounded resource limits for one plugin. */
export interface SandboxLimits {
  /** Heap cap for the plugin process, in megabytes. */
  readonly memoryMb: number;
  /** Per-invocation execution watchdog, in milliseconds. */
  readonly timeoutMs: number;
  /** Time allowed for the plugin source to evaluate at load, in milliseconds. */
  readonly loadTimeoutMs: number;
  /** Maximum serialized size of the plugin's KV store, in bytes. */
  readonly kvQuotaBytes: number;
  /** Maximum number of retained log entries. */
  readonly maxLogEntries: number;
}

/** Defaults applied to any limit a manifest does not override. */
export const DEFAULT_LIMITS: Readonly<SandboxLimits> = Object.freeze({
  memoryMb: 64,
  timeoutMs: 1000,
  loadTimeoutMs: 1000,
  kvQuotaBytes: 64 * 1024,
  maxLogEntries: 100,
});

/** Inclusive `[min, max]` bounds that every limit must satisfy. */
const LIMIT_BOUNDS: Readonly<Record<keyof SandboxLimits, readonly [number, number]>> = Object.freeze({
  memoryMb: [8, 1024],
  timeoutMs: [1, 60_000],
  loadTimeoutMs: [1, 60_000],
  kvQuotaBytes: [0, 16 * 1024 * 1024],
  maxLogEntries: [0, 10_000],
});

/** Maximum accepted plugin source size, in bytes. */
export const MAX_SOURCE_BYTES = 1024 * 1024;

const NAME_RE = /^[a-z][a-z0-9-]{1,62}$/;
const VERSION_RE = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/;
const METHOD_RE = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;
const SHA256_RE = /^[0-9a-f]{64}$/;
const KV_KEY_MAX = 256;
const LOG_MESSAGE_MAX = 4096;
const LOG_LEVEL_SET: ReadonlySet<string> = new Set(LOG_LEVELS);
const CAPABILITY_SET: ReadonlySet<string> = new Set(CAPABILITIES);

// ---------------------------------------------------------------------------
// Error hierarchy
// ---------------------------------------------------------------------------

/** Root error for every sandbox failure. Carries a stable machine-readable `code`. */
export class SandboxError extends Error {
  public readonly code: string;
  public readonly details: Readonly<Record<string, unknown>>;

  constructor(message: string, code: string, details: Record<string, unknown> = {}) {
    super(message);
    this.name = 'SandboxError';
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

/** A manifest or policy input failed validation. */
export class ManifestError extends SandboxError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'MANIFEST_INVALID', details);
    this.name = 'ManifestError';
  }
}

/** The supplied source does not match the SHA-256 pinned in the manifest. */
export class IntegrityError extends SandboxError {
  constructor(name: string, expected: string, actual: string) {
    super(`Plugin '${name}' source does not match pinned sha256`, 'INTEGRITY_MISMATCH', {
      plugin: name,
      expected,
      actual,
    });
    this.name = 'IntegrityError';
  }
}

/** A manifest requests a capability the host policy does not grant. */
export class PolicyViolationError extends SandboxError {
  constructor(name: string, capability: string) {
    super(
      `Plugin '${name}' requests capability '${capability}', which host policy does not grant`,
      'POLICY_VIOLATION',
      { plugin: name, capability },
    );
    this.name = 'PolicyViolationError';
  }
}

/** A plugin with this name is already loaded. */
export class PluginAlreadyLoadedError extends SandboxError {
  constructor(name: string) {
    super(`Plugin '${name}' is already loaded`, 'PLUGIN_EXISTS', { plugin: name });
    this.name = 'PluginAlreadyLoadedError';
  }
}

/** No plugin with this name is loaded. */
export class PluginNotFoundError extends SandboxError {
  constructor(name: string) {
    super(`Plugin '${name}' is not loaded`, 'PLUGIN_NOT_FOUND', { plugin: name });
    this.name = 'PluginNotFoundError';
  }
}

/** The plugin source failed to evaluate, or did not finish evaluating in time. */
export class PluginLoadError extends SandboxError {
  constructor(name: string, reason: string) {
    super(`Plugin '${name}' failed to load: ${reason}`, 'PLUGIN_LOAD_FAILED', {
      plugin: name,
      reason,
    });
    this.name = 'PluginLoadError';
  }
}

/** An invocation exceeded its execution watchdog. The instance has been faulted. */
export class PluginTimeoutError extends SandboxError {
  constructor(name: string, method: string, timeoutMs: number) {
    super(
      `Plugin '${name}' method '${method}' exceeded ${timeoutMs}ms; process terminated`,
      'PLUGIN_TIMEOUT',
      { plugin: name, method, timeoutMs },
    );
    this.name = 'PluginTimeoutError';
  }
}

/** The plugin process exceeded its heap limit and was terminated. */
export class PluginMemoryLimitError extends SandboxError {
  constructor(name: string, memoryMb: number) {
    super(`Plugin '${name}' exceeded its ${memoryMb} MB heap limit`, 'PLUGIN_OOM', {
      plugin: name,
      memoryMb,
    });
    this.name = 'PluginMemoryLimitError';
  }
}

/** The plugin process died unexpectedly. Calls in flight at that moment fail with this. */
export class PluginCrashedError extends SandboxError {
  constructor(name: string, reason: string) {
    super(`Plugin '${name}' crashed: ${reason}`, 'PLUGIN_CRASHED', { plugin: name, reason });
    this.name = 'PluginCrashedError';
  }
}

/** The plugin's exported method threw. The original error name and code are preserved. */
export class PluginRuntimeError extends SandboxError {
  public readonly pluginErrorName: string;

  constructor(name: string, message: string, pluginCode: string | null, pluginErrorName: string) {
    super(`Plugin '${name}' threw ${pluginErrorName}: ${message}`, 'PLUGIN_RUNTIME_ERROR', {
      plugin: name,
      pluginCode,
      pluginErrorName,
    });
    this.name = 'PluginRuntimeError';
    this.pluginErrorName = pluginErrorName;
  }
}

/** A plugin called an `api` method whose capability is not in its manifest. */
export class CapabilityDeniedError extends SandboxError {
  constructor(plugin: string, capability: string, op: string) {
    super(
      `Plugin '${plugin}' has no '${capability}' capability (attempted ${capability}.${op})`,
      'CAPABILITY_DENIED',
      { plugin, capability, op },
    );
    this.name = 'CapabilityDeniedError';
  }
}

/** A KV write would push the plugin past its storage quota. */
export class KvQuotaExceededError extends SandboxError {
  constructor(plugin: string, quotaBytes: number, requiredBytes: number) {
    super(
      `Plugin '${plugin}' KV quota of ${quotaBytes} bytes would be exceeded (need ${requiredBytes})`,
      'KV_QUOTA_EXCEEDED',
      { plugin, quotaBytes, requiredBytes },
    );
    this.name = 'KvQuotaExceededError';
  }
}

/** A KV value cannot be represented as JSON. */
export class KvValueError extends SandboxError {
  constructor(message: string) {
    super(message, 'KV_INVALID_VALUE');
    this.name = 'KvValueError';
  }
}

/** A plugin sent a malformed host call. */
export class InvalidHostCallError extends SandboxError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'INVALID_HOST_CALL', details);
    this.name = 'InvalidHostCallError';
  }
}

/** A caller invoked a method or passed options that are not valid. */
export class InvalidInvocationError extends SandboxError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'INVALID_INVOCATION', details);
    this.name = 'InvalidInvocationError';
  }
}

/** The sandbox already holds its maximum number of plugins. */
export class SandboxCapacityError extends SandboxError {
  constructor(maxPlugins: number) {
    super(`Sandbox is at capacity (${maxPlugins} plugins)`, 'SANDBOX_CAPACITY', { maxPlugins });
    this.name = 'SandboxCapacityError';
  }
}

/** The sandbox has been shut down and accepts no further work. */
export class SandboxClosedError extends SandboxError {
  constructor() {
    super('Sandbox has been shut down', 'SANDBOX_CLOSED');
    this.name = 'SandboxClosedError';
  }
}

// ---------------------------------------------------------------------------
// Public domain types
// ---------------------------------------------------------------------------

/** A plugin manifest as supplied by the caller. Validated before use. */
export interface PluginManifest {
  readonly name: string;
  readonly version: string;
  readonly capabilities: readonly string[];
  readonly limits?: Partial<SandboxLimits>;
  /** Optional lowercase hex SHA-256 of the plugin source. */
  readonly sha256?: string;
}

/** A fully validated manifest. */
export interface ValidatedManifest {
  readonly name: string;
  readonly version: string;
  readonly capabilities: readonly Capability[];
  readonly limits: Readonly<SandboxLimits>;
  readonly declaredSha256: string | null;
}

/** Lifecycle state of a plugin instance. */
export type PluginState = 'loading' | 'ready' | 'faulted' | 'retiring' | 'terminated';

/** A structured log entry emitted by a plugin through `api.log`. */
export interface LogEntry {
  readonly level: LogLevel;
  readonly message: string;
  readonly at: number;
}

/** Point-in-time description of a loaded plugin. */
export interface PluginInfo {
  readonly name: string;
  readonly version: string;
  readonly state: PluginState;
  readonly capabilities: readonly Capability[];
  readonly limits: Readonly<SandboxLimits>;
  readonly sha256: string;
  readonly inFlight: number;
  readonly kvBytes: number;
  readonly kvEntries: number;
  readonly logCount: number;
}

/** Sandbox-wide operational counters. */
export interface SandboxMetrics {
  readonly activePlugins: number;
  readonly loads: number;
  readonly reloads: number;
  readonly restarts: number;
  readonly unloads: number;
  readonly invocations: number;
  readonly timeouts: number;
  readonly memoryFaults: number;
  readonly crashes: number;
  readonly hostCalls: number;
  readonly capabilityDenials: number;
}

/** Per-call options for `PluginSandbox.invoke`. */
export interface InvokeOptions {
  /** Overrides the plugin's default watchdog for this call only. */
  readonly timeoutMs?: number;
}

/** Construction options for `PluginSandbox`. */
export interface SandboxOptions {
  /** Host policy: the maximum set of capabilities any plugin may declare. */
  readonly grant?: readonly Capability[];
  /** Clock used by the `clock` capability and log timestamps. Defaults to `Date.now`. */
  readonly clock?: () => number;
  /** Maximum number of concurrently loaded plugins. Defaults to 32. */
  readonly maxPlugins?: number;
}

/** Events emitted by `PluginSandbox`. */
export interface SandboxEvents {
  loaded: (plugin: string, version: string) => void;
  reloaded: (plugin: string, version: string) => void;
  unloaded: (plugin: string) => void;
  log: (plugin: string, entry: LogEntry) => void;
  denied: (event: { plugin: string; capability: string; op: string }) => void;
  fault: (plugin: string, error: SandboxError) => void;
}

// ---------------------------------------------------------------------------
// Wire protocol (host <-> child process)
// ---------------------------------------------------------------------------

interface WireError {
  readonly name: string;
  readonly message: string;
  readonly code: string | null;
}

type HostToChild =
  | { readonly type: 'load'; readonly source: string; readonly loadTimeoutMs: number }
  | { readonly type: 'invoke'; readonly id: number; readonly method: string; readonly args: unknown }
  | {
      readonly type: 'host-result';
      readonly id: number;
      readonly ok: boolean;
      readonly value?: unknown;
      readonly error?: WireError;
    };

type ChildToHost =
  | { readonly type: 'loaded' }
  | { readonly type: 'load-error'; readonly error: WireError }
  | { readonly type: 'result'; readonly id: number; readonly ok: true; readonly value: unknown }
  | { readonly type: 'result'; readonly id: number; readonly ok: false; readonly error: WireError }
  | {
      readonly type: 'host-call';
      readonly id: number;
      readonly capability: string;
      readonly op: string;
      readonly args: unknown;
    };

/**
 * Source of the child-process bootstrap. It runs inside the child process as a
 * CommonJS eval script. It builds the frozen `api` object, evaluates the
 * plugin inside a `vm` context with a load-time timeout, and turns each
 * invocation into a reply message. It deliberately contains no host
 * authority: every side effect is a message the host must approve.
 */
const CHILD_BOOTSTRAP = `
'use strict';
const vm = require('node:vm');

// Thin shim over the IPC channel so the message handling below reads as a
// port. The child has no other channel to the host.
const parentPort = {
  postMessage: function (m) { process.send(m); },
  on: function (event, fn) { process.on(event, fn); }
};

// If the host disappears, do not leave an orphaned plugin process running.
process.on('disconnect', function () { process.exit(0); });

let seq = 0;
const pending = new Map();
let sandboxGlobal = null;
let loaded = false;

function serializeError(err) {
  if (err !== null && typeof err === 'object') {
    return {
      name: String(err.name || 'Error'),
      message: String(err.message || ''),
      code: typeof err.code === 'string' ? err.code : null
    };
  }
  return { name: 'Error', message: String(err), code: null };
}

function namedError(name, message, code) {
  const e = new Error(message);
  e.name = name;
  e.code = code;
  return e;
}

function hostCall(capability, op, args) {
  return new Promise(function (resolve, reject) {
    seq += 1;
    const id = seq;
    pending.set(id, { resolve: resolve, reject: reject });
    try {
      parentPort.postMessage({ type: 'host-call', id: id, capability: capability, op: op, args: args });
    } catch (err) {
      pending.delete(id);
      reject(err);
    }
  });
}

function buildGlobal() {
  const api = Object.freeze({
    log: function (level, message) {
      return hostCall('log', 'write', { level: String(level), message: String(message) });
    },
    kvGet: function (key) {
      return hostCall('kv', 'get', { key: String(key) });
    },
    kvSet: function (key, value) {
      return hostCall('kv', 'set', { key: String(key), value: value });
    },
    kvDelete: function (key) {
      return hostCall('kv', 'delete', { key: String(key) });
    },
    now: function () {
      return hostCall('clock', 'now', {});
    }
  });
  return { api: api, module: { exports: {} } };
}

function postResult(id, value) {
  try {
    parentPort.postMessage({ type: 'result', id: id, ok: true, value: value });
  } catch (err) {
    parentPort.postMessage({ type: 'result', id: id, ok: false, error: serializeError(err) });
  }
}

function handleHostResult(msg) {
  const entry = pending.get(msg.id);
  if (entry === undefined) return;
  pending.delete(msg.id);
  if (msg.ok) {
    entry.resolve(msg.value);
    return;
  }
  const e = new Error(msg.error.message);
  e.name = msg.error.name;
  if (msg.error.code) e.code = msg.error.code;
  entry.reject(e);
}

function handleLoad(msg) {
  try {
    const g = buildGlobal();
    vm.createContext(g);
    vm.runInContext(msg.source, g, { filename: 'plugin.js', timeout: msg.loadTimeoutMs });
    sandboxGlobal = g;
    loaded = true;
    parentPort.postMessage({ type: 'loaded' });
  } catch (err) {
    parentPort.postMessage({ type: 'load-error', error: serializeError(err) });
  }
}

async function handleInvoke(msg) {
  try {
    if (!loaded) {
      throw namedError('PluginNotLoaded', 'plugin source has not been loaded', 'NOT_LOADED');
    }
    const exportsObj = sandboxGlobal.module.exports;
    const exported = Object.prototype.hasOwnProperty.call(exportsObj, msg.method)
      ? exportsObj[msg.method]
      : undefined;
    if (typeof exported !== 'function') {
      throw namedError('MethodNotFoundError', 'method is not exported: ' + msg.method, 'METHOD_NOT_FOUND');
    }
    const value = await exported.call(exportsObj, msg.args);
    postResult(msg.id, value);
  } catch (err) {
    parentPort.postMessage({ type: 'result', id: msg.id, ok: false, error: serializeError(err) });
  }
}

parentPort.on('message', function (msg) {
  if (msg.type === 'host-result') {
    handleHostResult(msg);
  } else if (msg.type === 'load') {
    handleLoad(msg);
  } else if (msg.type === 'invoke') {
    handleInvoke(msg);
  }
});
`;

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/** Lowercase hex SHA-256 of a UTF-8 string. */
export function sha256Hex(source: string): string {
  return createHash('sha256').update(source, 'utf8').digest('hex');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function assertInt(value: unknown, field: string, min: number, max: number): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < min || value > max) {
    throw new ManifestError(`${field} must be an integer in [${min}, ${max}]`, {
      field,
      value,
      min,
      max,
    });
  }
  return value;
}

/**
 * Resolves a partial limits object into a complete, bounds-checked
 * `SandboxLimits`. Unknown keys are rejected rather than silently ignored,
 * so a typo cannot quietly leave a limit at its default.
 *
 * @throws {ManifestError} on a non-object, an unknown key, or an out-of-range value.
 */
export function resolveLimits(input: unknown): Readonly<SandboxLimits> {
  if (input === undefined) return DEFAULT_LIMITS;
  if (!isRecord(input)) {
    throw new ManifestError('limits must be an object');
  }
  for (const key of Object.keys(input)) {
    if (!(key in LIMIT_BOUNDS)) {
      throw new ManifestError(`unknown limit '${key}'`, { key });
    }
  }
  const out: { -readonly [K in keyof SandboxLimits]: number } = { ...DEFAULT_LIMITS };
  for (const key of Object.keys(LIMIT_BOUNDS) as Array<keyof SandboxLimits>) {
    const raw = input[key];
    if (raw === undefined) continue;
    const [min, max] = LIMIT_BOUNDS[key];
    out[key] = assertInt(raw, `limits.${key}`, min, max);
  }
  return Object.freeze(out);
}

/**
 * Validates a raw manifest into a `ValidatedManifest`.
 *
 * @throws {ManifestError} on any structural or bounds violation.
 */
export function validateManifest(input: unknown): ValidatedManifest {
  if (!isRecord(input)) {
    throw new ManifestError('manifest must be an object');
  }

  const name = input['name'];
  if (typeof name !== 'string' || !NAME_RE.test(name)) {
    throw new ManifestError(`name must match ${NAME_RE.source}`, { name });
  }

  const version = input['version'];
  if (typeof version !== 'string' || !VERSION_RE.test(version)) {
    throw new ManifestError('version must be semver (MAJOR.MINOR.PATCH[-pre])', { version });
  }

  const rawCaps = input['capabilities'];
  if (!Array.isArray(rawCaps)) {
    throw new ManifestError('capabilities must be an array');
  }
  const seen = new Set<string>();
  const capabilities: Capability[] = [];
  for (const cap of rawCaps) {
    if (typeof cap !== 'string' || !CAPABILITY_SET.has(cap)) {
      throw new ManifestError(`unknown capability ${JSON.stringify(cap)}`, { capability: cap });
    }
    if (seen.has(cap)) {
      throw new ManifestError(`duplicate capability '${cap}'`, { capability: cap });
    }
    seen.add(cap);
    capabilities.push(cap as Capability);
  }

  const limits = resolveLimits(input['limits']);

  const rawSha = input['sha256'];
  let declaredSha256: string | null = null;
  if (rawSha !== undefined) {
    if (typeof rawSha !== 'string' || !SHA256_RE.test(rawSha)) {
      throw new ManifestError('sha256 must be 64 lowercase hex characters');
    }
    declaredSha256 = rawSha;
  }

  return { name, version, capabilities, limits, declaredSha256 };
}

function validateMethod(method: unknown): asserts method is string {
  if (typeof method !== 'string' || !METHOD_RE.test(method)) {
    throw new InvalidInvocationError('method name is not a valid identifier', { method });
  }
}

function toWire(err: unknown): WireError {
  if (err instanceof Error) {
    const code = (err as { code?: unknown }).code;
    return {
      name: err.name,
      message: err.message,
      code: typeof code === 'string' ? code : null,
    };
  }
  return { name: 'Error', message: String(err), code: null };
}

// ---------------------------------------------------------------------------
// KV store and log buffer (owned by the sandbox, survive reloads)
// ---------------------------------------------------------------------------

/** Measures the UTF-8 byte size of a value's JSON form. */
function measureValue(value: unknown): number {
  let json: string | undefined;
  try {
    json = JSON.stringify(value);
  } catch (err) {
    throw new KvValueError(
      `value is not JSON-serialisable: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  if (json === undefined) {
    throw new KvValueError('value is not JSON-serialisable (undefined or function)');
  }
  return Buffer.byteLength(json, 'utf8');
}

/**
 * Per-plugin key/value store with an exact byte quota. The quota counts the
 * UTF-8 size of each key plus the JSON size of its value. Overwrites release
 * the previous entry's bytes before the new one is counted.
 */
export class KvStore {
  #quota: number;
  readonly #plugin: string;
  readonly #entries = new Map<string, { readonly value: unknown; readonly bytes: number }>();
  #bytes = 0;

  constructor(plugin: string, quotaBytes: number) {
    this.#plugin = plugin;
    this.#quota = quotaBytes;
  }

  /** Current quota in bytes. */
  get quota(): number {
    return this.#quota;
  }

  /** Bytes currently used. */
  get bytes(): number {
    return this.#bytes;
  }

  /** Number of stored keys. */
  get size(): number {
    return this.#entries.size;
  }

  /** Returns the stored value, or `undefined` if absent. */
  get(key: string): unknown {
    return this.#entries.get(key)?.value;
  }

  /**
   * Stores `value` under `key`.
   *
   * @throws {KvValueError}         if the value is not JSON-serialisable.
   * @throws {KvQuotaExceededError} if the write would exceed the quota. Nothing is changed.
   */
  set(key: string, value: unknown): void {
    const valueBytes = measureValue(value);
    const entryBytes = Buffer.byteLength(key, 'utf8') + valueBytes;
    const prev = this.#entries.get(key);
    const next = this.#bytes - (prev?.bytes ?? 0) + entryBytes;
    if (next > this.#quota) {
      throw new KvQuotaExceededError(this.#plugin, this.#quota, next);
    }
    this.#entries.set(key, { value, bytes: entryBytes });
    this.#bytes = next;
  }

  /** Removes `key`. Returns `true` if it existed. */
  delete(key: string): boolean {
    const prev = this.#entries.get(key);
    if (prev === undefined) return false;
    this.#entries.delete(key);
    this.#bytes -= prev.bytes;
    return true;
  }

  /**
   * Changes the quota. Refuses to shrink below current usage, so a reload
   * with a smaller quota cannot silently leave the store over budget.
   */
  setQuota(quotaBytes: number): void {
    if (this.#bytes > quotaBytes) {
      throw new KvQuotaExceededError(this.#plugin, quotaBytes, this.#bytes);
    }
    this.#quota = quotaBytes;
  }
}

/** Bounded, append-only log buffer. Oldest entries are evicted first. */
export class LogBuffer {
  #capacity: number;
  #entries: LogEntry[] = [];
  #total = 0;

  constructor(capacity: number) {
    this.#capacity = capacity;
  }

  /** Maximum number of retained entries. */
  get capacity(): number {
    return this.#capacity;
  }

  /** Total entries ever appended, including evicted ones. */
  get total(): number {
    return this.#total;
  }

  /** Currently retained entries, oldest first. */
  get entries(): readonly LogEntry[] {
    return [...this.#entries];
  }

  push(entry: LogEntry): void {
    this.#total += 1;
    if (this.#capacity === 0) return;
    this.#entries.push(entry);
    if (this.#entries.length > this.#capacity) {
      this.#entries.splice(0, this.#entries.length - this.#capacity);
    }
  }

  setCapacity(capacity: number): void {
    this.#capacity = capacity;
    if (this.#entries.length > capacity) {
      this.#entries.splice(0, this.#entries.length - capacity);
    }
  }
}

/** State that outlives individual process instances of the same plugin. */
interface PluginContext {
  readonly kv: KvStore;
  readonly logs: LogBuffer;
}

// ---------------------------------------------------------------------------
// PluginInstance: one child process plus its in-flight calls
// ---------------------------------------------------------------------------

interface InstanceHooks {
  onHostCall(): void;
  onDenied(plugin: string, capability: string, op: string): void;
  onLog(plugin: string, entry: LogEntry): void;
  onFault(plugin: string, error: SandboxError): void;
}

interface PendingInvocation {
  readonly resolve: (value: unknown) => void;
  readonly reject: (err: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

/** A plugin's validated identity and limits, plus the source it was loaded from. */
interface ResolvedPlugin {
  readonly name: string;
  readonly version: string;
  readonly capabilities: readonly Capability[];
  readonly limits: Readonly<SandboxLimits>;
  readonly sha256: string;
}

/**
 * One child process running one version of a plugin. Owns the process's
 * lifecycle, the per-call watchdogs, and the host side of the message
 * protocol. It does not own KV or logs; those come in through `ctx`.
 */
class PluginInstance {
  readonly plugin: ResolvedPlugin;
  readonly #source: string;
  readonly #ctx: PluginContext;
  readonly #hooks: InstanceHooks;
  readonly #clock: () => number;

  #child: ChildProcess | null = null;
  #state: PluginState = 'loading';
  #seq = 0;
  readonly #pending = new Map<number, PendingInvocation>();
  #idleWaiters: Array<() => void> = [];
  #onLoaded: ((err: Error | null) => void) | null = null;
  #loadTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    plugin: ResolvedPlugin,
    source: string,
    ctx: PluginContext,
    hooks: InstanceHooks,
    clock: () => number,
  ) {
    this.plugin = plugin;
    this.#source = source;
    this.#ctx = ctx;
    this.#hooks = hooks;
    this.#clock = clock;
  }

  get state(): PluginState {
    return this.#state;
  }

  /** Spawns the process and resolves once the plugin source has evaluated. */
  start(): Promise<void> {
    if (this.#state !== 'loading') {
      return Promise.reject(new SandboxError('instance is not in loading state', 'INVALID_STATE'));
    }
    const { limits, name } = this.plugin;
    // The heap cap is passed on the command line: V8 enforces it for the whole
    // process. The child runs the bootstrap via `-e` and talks over an IPC
    // channel using structured-clone ("advanced") serialization, so bigint,
    // Buffer and other rich values round-trip without manual encoding.
    // `--permission` with no grants denies filesystem, child-process, worker and
    // addon access to everything in the child, including code that escapes the
    // plugin realm. The empty environment keeps host secrets out of reach.
    const child = spawn(
      process.execPath,
      ['--permission', `--max-old-space-size=${limits.memoryMb}`, '-e', CHILD_BOOTSTRAP],
      {
        stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
        serialization: 'advanced',
        env: {},
      },
    );
    this.#child = child;

    // Keep only a bounded tail of stderr: V8 writes its heap-exhaustion report
    // there, and that report is how an out-of-memory exit is told apart from
    // an ordinary crash.
    let stderrTail = '';
    child.stderr?.on('data', (chunk: Buffer) => {
      stderrTail = (stderrTail + chunk.toString('utf8')).slice(-4096);
    });

    child.on('message', (msg: ChildToHost) => this.#onMessage(msg));
    child.on('error', (err: Error) => {
      if (this.#child === child) {
        this.#fault(new PluginCrashedError(name, `process error: ${err.message}`), null);
      }
    });
    // Classify on 'close', not 'exit': 'close' is emitted only after the child's
    // stderr has been fully read, so the out-of-memory report is available.
    child.on('close', (code: number | null, signal: NodeJS.Signals | null) => {
      if (this.#child !== child) return; // we terminated it ourselves
      this.#child = null;
      if (/heap out of memory/i.test(stderrTail)) {
        this.#fault(new PluginMemoryLimitError(name, limits.memoryMb), null);
      } else {
        const how = signal !== null ? `signal ${signal}` : `code ${String(code)}`;
        this.#fault(new PluginCrashedError(name, `process exited unexpectedly (${how})`), null);
      }
    });

    return new Promise<void>((resolve, reject) => {
      this.#onLoaded = (err: Error | null): void => {
        if (err === null) resolve();
        else reject(err);
      };
      // Host-side guard in addition to the in-VM timeout: covers process
      // start-up cost and a plugin that somehow stalls the IPC channel.
      this.#loadTimer = setTimeout(
        () => this.#fault(new PluginLoadError(name, 'load timed out'), null),
        limits.loadTimeoutMs + 3000,
      );
      this.#send({ type: 'load', source: this.#source, loadTimeoutMs: limits.loadTimeoutMs });
    });
  }

  /** Re-spawns a faulted instance with the same source. KV and logs are preserved. */
  async restart(): Promise<void> {
    if (this.#state !== 'faulted') {
      throw new SandboxError(`cannot restart an instance in state '${this.#state}'`, 'INVALID_STATE');
    }
    this.#state = 'loading';
    await this.start();
  }

  /**
   * Dispatches one method call to the plugin process under a watchdog.
   *
   * @throws {PluginTimeoutError} if the watchdog fires (the process is terminated).
   * @throws {PluginCrashedError} if the instance is not ready or dies mid-call.
   * @throws {PluginRuntimeError} if the plugin's method throws.
   */
  invoke(method: string, args: unknown, timeoutMs: number): Promise<unknown> {
    if (this.#state !== 'ready' || this.#child === null) {
      return Promise.reject(
        new PluginCrashedError(this.plugin.name, `plugin is ${this.#state}, not ready`),
      );
    }
    const id = ++this.#seq;
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#fault(new PluginTimeoutError(this.plugin.name, method, timeoutMs), id);
      }, timeoutMs);
      this.#pending.set(id, { resolve, reject, timer });
      // A send failure (for example, arguments that cannot be cloned) affects
      // only this call; the plugin itself is still healthy.
      this.#send({ type: 'invoke', id, method, args }, (err: Error) => {
        const call = this.#pending.get(id);
        if (call === undefined) return;
        clearTimeout(call.timer);
        this.#pending.delete(id);
        reject(
          new InvalidInvocationError('arguments could not be sent to the plugin', {
            reason: err.message,
          }),
        );
        this.#notifyIdle();
      });
    });
  }

  /**
   * Waits for in-flight calls to settle, then terminates the process. Calls that
   * never settle are bounded by their own watchdogs, so this always completes.
   */
  async retire(): Promise<void> {
    if (this.#state === 'terminated') return;
    this.#state = 'retiring';
    if (this.#pending.size > 0) {
      await new Promise<void>((resolve) => this.#idleWaiters.push(resolve));
    }
    this.#terminateChild();
    this.#state = 'terminated';
  }

  info(): PluginInfo {
    return {
      name: this.plugin.name,
      version: this.plugin.version,
      state: this.#state,
      capabilities: this.plugin.capabilities,
      limits: this.plugin.limits,
      sha256: this.plugin.sha256,
      inFlight: this.#pending.size,
      kvBytes: this.#ctx.kv.bytes,
      kvEntries: this.#ctx.kv.size,
      logCount: this.#ctx.logs.total,
    };
  }

  // -- message handling -----------------------------------------------------

  #onMessage(msg: ChildToHost): void {
    switch (msg.type) {
      case 'loaded': {
        if (this.#state !== 'loading') return;
        this.#clearLoadTimer();
        this.#state = 'ready';
        const cb = this.#onLoaded;
        this.#onLoaded = null;
        cb?.(null);
        return;
      }
      case 'load-error':
        this.#fault(new PluginLoadError(this.plugin.name, msg.error.message), null);
        return;
      case 'host-call':
        this.#onHostCall(msg);
        return;
      case 'result':
        this.#onResult(msg);
        return;
    }
  }

  #onResult(msg: Extract<ChildToHost, { type: 'result' }>): void {
    const call = this.#pending.get(msg.id);
    if (call === undefined) return; // already settled by a watchdog or fault
    clearTimeout(call.timer);
    this.#pending.delete(msg.id);
    if (msg.ok) {
      call.resolve(msg.value);
    } else {
      call.reject(
        new PluginRuntimeError(
          this.plugin.name,
          msg.error.message,
          msg.error.code,
          msg.error.name,
        ),
      );
    }
    this.#notifyIdle();
  }

  #onHostCall(msg: Extract<ChildToHost, { type: 'host-call' }>): void {
    this.#hooks.onHostCall();
    try {
      if (!this.plugin.capabilities.includes(msg.capability as Capability)) {
        this.#hooks.onDenied(this.plugin.name, msg.capability, msg.op);
        throw new CapabilityDeniedError(this.plugin.name, msg.capability, msg.op);
      }
      const value = this.#dispatch(msg.capability as Capability, msg.op, msg.args);
      this.#post({ type: 'host-result', id: msg.id, ok: true, value });
    } catch (err) {
      this.#post({ type: 'host-result', id: msg.id, ok: false, error: toWire(err) });
    }
  }

  #dispatch(capability: Capability, op: string, args: unknown): unknown {
    switch (capability) {
      case 'log':
        return this.#dispatchLog(op, args);
      case 'kv':
        return this.#dispatchKv(op, args);
      case 'clock':
        if (op !== 'now') throw new InvalidHostCallError(`unknown clock op '${op}'`, { op });
        return this.#clock();
    }
  }

  #dispatchLog(op: string, args: unknown): null {
    if (op !== 'write') throw new InvalidHostCallError(`unknown log op '${op}'`, { op });
    const a = expectRecord(args);
    const level = a['level'];
    const message = a['message'];
    if (typeof level !== 'string' || !LOG_LEVEL_SET.has(level)) {
      throw new InvalidHostCallError('log level must be one of debug|info|warn|error', { level });
    }
    if (typeof message !== 'string' || message.length > LOG_MESSAGE_MAX) {
      throw new InvalidHostCallError(`log message must be a string of at most ${LOG_MESSAGE_MAX} characters`);
    }
    const entry: LogEntry = { level: level as LogLevel, message, at: this.#clock() };
    this.#ctx.logs.push(entry);
    this.#hooks.onLog(this.plugin.name, entry);
    return null;
  }

  #dispatchKv(op: string, args: unknown): unknown {
    const a = expectRecord(args);
    const key = a['key'];
    if (typeof key !== 'string' || key.length === 0 || key.length > KV_KEY_MAX) {
      throw new InvalidHostCallError(`kv key must be a string of 1-${KV_KEY_MAX} characters`);
    }
    switch (op) {
      case 'get': {
        const value = this.#ctx.kv.get(key);
        return value === undefined ? null : value;
      }
      case 'set': {
        if (!('value' in a)) throw new InvalidHostCallError('kv set requires a value');
        this.#ctx.kv.set(key, a['value']);
        return null;
      }
      case 'delete':
        return this.#ctx.kv.delete(key);
      default:
        throw new InvalidHostCallError(`unknown kv op '${op}'`, { op });
    }
  }

  // -- internals ------------------------------------------------------------

  /**
   * Marks the instance faulted and rejects in-flight calls. When a specific
   * call caused the fault (`triggerId`, a watchdog timeout), that call receives
   * `reason` itself and every other call receives a `PluginCrashedError`. When
   * the process died as a whole (`triggerId` null: out of memory, crash), every
   * in-flight call receives `reason`, because they all died from the same cause.
   */
  #fault(reason: SandboxError, triggerId: number | null): void {
    if (this.#state === 'faulted' || this.#state === 'terminated') return;
    this.#state = 'faulted';
    this.#clearLoadTimer();
    const cb = this.#onLoaded;
    this.#onLoaded = null;
    cb?.(reason);
    for (const [id, call] of this.#pending) {
      clearTimeout(call.timer);
      const isCulprit = triggerId === null || id === triggerId;
      call.reject(isCulprit ? reason : new PluginCrashedError(this.plugin.name, reason.message));
    }
    this.#pending.clear();
    this.#terminateChild();
    this.#hooks.onFault(this.plugin.name, reason);
    this.#notifyIdle();
  }

  #clearLoadTimer(): void {
    if (this.#loadTimer !== null) {
      clearTimeout(this.#loadTimer);
      this.#loadTimer = null;
    }
  }

  /**
   * Kills the plugin process. SIGKILL is used deliberately: a plugin stuck in a
   * synchronous loop cannot handle SIGTERM, and the exit must not depend on it.
   */
  #terminateChild(): void {
    const child = this.#child;
    this.#child = null;
    if (child !== null && child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
    }
  }

  #notifyIdle(): void {
    if (this.#pending.size !== 0) return;
    const waiters = this.#idleWaiters;
    this.#idleWaiters = [];
    for (const wake of waiters) wake();
  }

  #post(msg: HostToChild): void {
    // Replies to host calls: if the send fails the plugin's own call will
    // surface the failure through its watchdog, so there is nothing to do here.
    this.#send(msg);
  }

  /**
   * Sends one message to the plugin process. `onError` receives asynchronous
   * channel failures and synchronous serialization failures.
   */
  #send(msg: HostToChild, onError?: (err: Error) => void): void {
    const child = this.#child;
    if (child === null) {
      onError?.(new PluginCrashedError(this.plugin.name, 'process is not running'));
      return;
    }
    try {
      child.send(msg, (err: Error | null) => {
        if (err !== null) onError?.(err);
      });
    } catch (err) {
      onError?.(err instanceof Error ? err : new Error(String(err)));
    }
  }
}

function expectRecord(args: unknown): Record<string, unknown> {
  if (!isRecord(args)) {
    throw new InvalidHostCallError('host call arguments must be an object');
  }
  return args;
}

// ---------------------------------------------------------------------------
// PluginSandbox: the public registry
// ---------------------------------------------------------------------------

/**
 * Registry and lifecycle manager for sandboxed plugins.
 *
 * ```ts
 * const sandbox = new PluginSandbox({ grant: ['log', 'kv'] });
 * await sandbox.load(
 *   { name: 'doubler', version: '1.0.0', capabilities: ['log'] },
 *   'module.exports.double = async (a) => a.value * 2;',
 * );
 * const out = await sandbox.invoke('doubler', 'double', { value: 21 }); // 42
 * await sandbox.shutdown();
 * ```
 *
 * Call `shutdown()` when finished: live child processes keep the event loop alive.
 */
export class PluginSandbox extends EventEmitter {
  readonly #grant: ReadonlySet<Capability>;
  readonly #clock: () => number;
  readonly #maxPlugins: number;
  readonly #plugins = new Map<string, PluginInstance>();
  readonly #contexts = new Map<string, PluginContext>();
  readonly #counters = {
    loads: 0,
    reloads: 0,
    restarts: 0,
    unloads: 0,
    invocations: 0,
    timeouts: 0,
    memoryFaults: 0,
    crashes: 0,
    hostCalls: 0,
    capabilityDenials: 0,
  };
  #closed = false;

  constructor(options: SandboxOptions = {}) {
    super();
    const grant = options.grant ?? [];
    for (const cap of grant) {
      if (!CAPABILITY_SET.has(cap)) {
        throw new SandboxError(`unknown capability in grant: ${cap}`, 'INVALID_POLICY', { capability: cap });
      }
    }
    this.#grant = new Set(grant);
    this.#clock = options.clock ?? Date.now;
    this.#maxPlugins = options.maxPlugins ?? 32;
    assertInt(this.#maxPlugins, 'maxPlugins', 1, 1024);
  }

  /**
   * Validates, integrity-checks, and loads a new plugin.
   *
   * @throws {ManifestError}          if the manifest or source is malformed.
   * @throws {IntegrityError}         if the source does not match the pinned sha256.
   * @throws {PolicyViolationError}   if the manifest requests an ungranted capability.
   * @throws {PluginAlreadyLoadedError} if the name is taken.
   * @throws {SandboxCapacityError}   if the sandbox is full.
   * @throws {PluginLoadError}        if the source fails to evaluate or times out.
   */
  async load(manifest: unknown, source: string): Promise<PluginInfo> {
    this.#assertOpen();
    const resolved = this.#prepare(manifest, source);
    if (this.#plugins.has(resolved.name)) {
      throw new PluginAlreadyLoadedError(resolved.name);
    }
    if (this.#plugins.size >= this.#maxPlugins) {
      throw new SandboxCapacityError(this.#maxPlugins);
    }
    const ctx: PluginContext = {
      kv: new KvStore(resolved.name, resolved.limits.kvQuotaBytes),
      logs: new LogBuffer(resolved.limits.maxLogEntries),
    };
    const instance = new PluginInstance(resolved, source, ctx, this.#hooks(), this.#clock);
    await instance.start();

    this.#plugins.set(resolved.name, instance);
    this.#contexts.set(resolved.name, ctx);
    this.#counters.loads += 1;
    this.emit('loaded', resolved.name, resolved.version);
    return instance.info();
  }

  /**
   * Atomically replaces a loaded plugin with a new version. The replacement
   * must load successfully before it takes traffic. Calls in flight on the old
   * version finish on it. On any failure the previous version keeps running.
   *
   * @throws {PluginNotFoundError} if no plugin with this name is loaded.
   * @throws {ManifestError | IntegrityError | PolicyViolationError | PluginLoadError} as for `load`.
   * @throws {KvQuotaExceededError} if the new quota is below current KV usage.
   */
  async reload(manifest: unknown, source: string): Promise<PluginInfo> {
    this.#assertOpen();
    const resolved = this.#prepare(manifest, source);
    const old = this.#require(resolved.name);
    const ctx = this.#contextOf(resolved.name);

    const prevQuota = ctx.kv.quota;
    const prevCapacity = ctx.logs.capacity;
    ctx.kv.setQuota(resolved.limits.kvQuotaBytes);
    ctx.logs.setCapacity(resolved.limits.maxLogEntries);

    const fresh = new PluginInstance(resolved, source, ctx, this.#hooks(), this.#clock);
    try {
      await fresh.start();
    } catch (err) {
      ctx.kv.setQuota(prevQuota);
      ctx.logs.setCapacity(prevCapacity);
      throw err;
    }

    this.#plugins.set(resolved.name, fresh);
    this.#counters.reloads += 1;
    await old.retire();
    this.emit('reloaded', resolved.name, resolved.version);
    return fresh.info();
  }

  /**
   * Invokes an exported method of a loaded plugin.
   *
   * @throws {PluginNotFoundError}     if the plugin is not loaded.
   * @throws {InvalidInvocationError}  if the method name or timeout is invalid.
   * @throws {PluginTimeoutError}      if the watchdog fires; the plugin is faulted.
   * @throws {PluginCrashedError}      if the plugin is faulted or dies mid-call.
   * @throws {PluginRuntimeError}      if the plugin's method throws.
   */
  async invoke(name: string, method: string, args?: unknown, options: InvokeOptions = {}): Promise<unknown> {
    this.#assertOpen();
    validateMethod(method);
    const instance = this.#require(name);
    const timeoutMs = options.timeoutMs ?? instance.plugin.limits.timeoutMs;
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 60_000) {
      throw new InvalidInvocationError('timeoutMs must be an integer in [1, 60000]', { timeoutMs });
    }
    this.#counters.invocations += 1;
    return instance.invoke(method, args, timeoutMs);
  }

  /**
   * Re-spawns a faulted plugin with its existing source. KV and logs are kept.
   *
   * @throws {PluginNotFoundError} if the plugin is not loaded.
   * @throws {SandboxError} with code INVALID_STATE if the plugin is not faulted.
   */
  async restart(name: string): Promise<PluginInfo> {
    this.#assertOpen();
    const instance = this.#require(name);
    await instance.restart();
    this.#counters.restarts += 1;
    return instance.info();
  }

  /**
   * Retires and removes a plugin. In-flight calls complete first. KV is discarded.
   *
   * @throws {PluginNotFoundError} if the plugin is not loaded.
   */
  async unload(name: string): Promise<void> {
    this.#assertOpen();
    const instance = this.#require(name);
    this.#plugins.delete(name);
    this.#contexts.delete(name);
    this.#counters.unloads += 1;
    await instance.retire();
    this.emit('unloaded', name);
  }

  /**
   * Describes one loaded plugin.
   *
   * @throws {PluginNotFoundError} if the plugin is not loaded.
   */
  info(name: string): PluginInfo {
    return this.#require(name).info();
  }

  /** Describes every loaded plugin, sorted by name. */
  list(): PluginInfo[] {
    return [...this.#plugins.values()]
      .map((p) => p.info())
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  }

  /** Returns the retained log entries for a plugin, oldest first. */
  logs(name: string): readonly LogEntry[] {
    this.#require(name);
    return this.#contextOf(name).logs.entries;
  }

  /** Sandbox-wide counters. */
  metrics(): SandboxMetrics {
    return { activePlugins: this.#plugins.size, ...this.#counters };
  }

  /**
   * Retires every plugin and refuses all further work. Idempotent.
   */
  async shutdown(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    const instances = [...this.#plugins.values()];
    this.#plugins.clear();
    this.#contexts.clear();
    await Promise.all(instances.map((i) => i.retire()));
  }

  // -- private --------------------------------------------------------------

  #assertOpen(): void {
    if (this.#closed) throw new SandboxClosedError();
  }

  #require(name: string): PluginInstance {
    const instance = this.#plugins.get(name);
    if (instance === undefined) throw new PluginNotFoundError(name);
    return instance;
  }

  #contextOf(name: string): PluginContext {
    const ctx = this.#contexts.get(name);
    if (ctx === undefined) throw new PluginNotFoundError(name);
    return ctx;
  }

  /**
   * Validates everything that can be checked before a process exists: the
   * manifest shape, source size, SHA-256 pin, and the host capability policy.
   */
  #prepare(manifest: unknown, source: string): ResolvedPlugin {
    const validated = validateManifest(manifest);
    if (typeof source !== 'string') {
      throw new ManifestError('source must be a string');
    }
    if (Buffer.byteLength(source, 'utf8') > MAX_SOURCE_BYTES) {
      throw new ManifestError(`source exceeds ${MAX_SOURCE_BYTES} bytes`);
    }
    const digest = sha256Hex(source);
    if (validated.declaredSha256 !== null && validated.declaredSha256 !== digest) {
      throw new IntegrityError(validated.name, validated.declaredSha256, digest);
    }
    for (const cap of validated.capabilities) {
      if (!this.#grant.has(cap)) {
        throw new PolicyViolationError(validated.name, cap);
      }
    }
    return {
      name: validated.name,
      version: validated.version,
      capabilities: validated.capabilities,
      limits: validated.limits,
      sha256: digest,
    };
  }

  #hooks(): InstanceHooks {
    return {
      onHostCall: (): void => {
        this.#counters.hostCalls += 1;
      },
      onDenied: (plugin, capability, op): void => {
        this.#counters.capabilityDenials += 1;
        this.emit('denied', { plugin, capability, op });
      },
      onLog: (plugin, entry): void => {
        this.emit('log', plugin, entry);
      },
      onFault: (plugin, error): void => {
        if (error instanceof PluginTimeoutError) this.#counters.timeouts += 1;
        else if (error instanceof PluginMemoryLimitError) this.#counters.memoryFaults += 1;
        else this.#counters.crashes += 1;
        this.emit('fault', plugin, error);
      },
    };
  }
}
