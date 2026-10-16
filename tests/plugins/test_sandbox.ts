/**
 * @file tests/plugins/test_sandbox.ts
 * @description Vitest suite for /src/plugins/sandbox.ts.
 *
 * Strategy:
 * - Pure validation and helper functions are unit-tested without spawning workers.
 * - Lifecycle, watchdog, memory, capability, quota, and hot-reload behaviour is
 *   tested end-to-end against real worker threads, because that is the only
 *   way to prove the isolation guarantees hold.
 * - Each worker-backed test gets an explicit timeout generous enough for
 *   worker start-up on a loaded CI runner.
 * - A fixed clock makes the `clock` capability deterministic.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  PluginSandbox,
  validateManifest,
  resolveLimits,
  sha256Hex,
  DEFAULT_LIMITS,
  ManifestError,
  IntegrityError,
  PolicyViolationError,
  PluginAlreadyLoadedError,
  PluginNotFoundError,
  PluginLoadError,
  PluginTimeoutError,
  PluginMemoryLimitError,
  PluginCrashedError,
  PluginRuntimeError,
  KvQuotaExceededError,
  SandboxCapacityError,
  SandboxClosedError,
  SandboxError,
  InvalidInvocationError,
  MAX_SOURCE_BYTES,
  type LogEntry,
} from '../../src/plugins/sandbox.js';

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/** Per-test timeout for anything that spawns worker threads. */
const WORKER_TEST_TIMEOUT = 20_000;

const FIXED_NOW = 1_760_000_000_000;
const fixedClock = (): number => FIXED_NOW;

const SRC_ECHO = 'module.exports.echo = async (args) => args;';
const SRC_DOUBLE = 'module.exports.double = (a) => a.value * 2;';
const SRC_V1 = 'module.exports.v = () => 1;';
const SRC_V2 = 'module.exports.v = () => 2;';
const SRC_SPIN = 'module.exports.spin = () => { while (true) {} };';
const SRC_HOG =
  'module.exports.hog = () => { const keep = []; while (true) keep.push(new Array(1000000).fill(7)); };';
const SRC_BOOM =
  "module.exports.boom = () => { const e = new TypeError('bad input'); e.code = 'E_BAD'; throw e; };";
const SRC_FN_RETURN = 'module.exports.fn = () => () => 1;';
const SRC_SYNTAX_ERROR = 'module.exports.x = ;';
const SRC_LOAD_SPIN = 'while (true) {}';
const SRC_NO_EXPORTS = 'const unused = 1;';
const SRC_LOGGER =
  "module.exports.say = async (a) => { await api.log('info', a.msg); return 'logged'; };";
const SRC_BAD_LOG =
  "module.exports.bad = async () => { try { await api.log('trace', 'x'); return 'ok'; } catch (e) { return e.code; } };";
const SRC_KV =
  'module.exports.put = async (a) => api.kvSet(a.key, a.value);' +
  ' module.exports.get = async (a) => api.kvGet(a.key);' +
  ' module.exports.del = async (a) => api.kvDelete(a.key);';
const SRC_KV_UNDEFINED =
  "module.exports.bad = async () => { try { await api.kvSet('k', undefined); return 'ok'; } catch (e) { return e.code; } };";
const SRC_KV_QUOTA =
  "module.exports.big = async () => { try { await api.kvSet('k', 'x'.repeat(500)); return 'ok'; } catch (e) { return e.code; } };";
const SRC_TRY_KV =
  "module.exports.tryKv = async () => { try { await api.kvGet('k'); return 'ok'; } catch (e) { return e.code; } };";
const SRC_CLOCK = 'module.exports.time = async () => api.now();';
const SRC_SLOW =
  'module.exports.slow = async () => { let s = 0; for (let i = 0; i < 3000000; i++) s += i; await null; return s; };';

/** Builds a manifest object for the given name and capabilities. */
function manifest(
  name: string,
  capabilities: string[] = [],
  extra: Record<string, unknown> = {},
): Record<string, unknown> {
  return { name, version: '1.0.0', capabilities, ...extra };
}

// ---------------------------------------------------------------------------
// Pure helpers (no workers)
// ---------------------------------------------------------------------------

describe('sha256Hex', () => {
  it('matches the known SHA-256 of the empty string', () => {
    expect(sha256Hex('')).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
  });

  it('is deterministic and 64 lowercase hex chars', () => {
    const h = sha256Hex('module.exports = {};');
    expect(h).toMatch(/^[0-9a-f]{64}$/);
    expect(sha256Hex('module.exports = {};')).toBe(h);
  });

  it('differs for any byte change', () => {
    expect(sha256Hex('a')).not.toBe(sha256Hex('b'));
  });
});

describe('resolveLimits', () => {
  it('returns defaults when undefined', () => {
    expect(resolveLimits(undefined)).toEqual(DEFAULT_LIMITS);
  });

  it('merges overrides over defaults', () => {
    const l = resolveLimits({ memoryMb: 32, timeoutMs: 250 });
    expect(l.memoryMb).toBe(32);
    expect(l.timeoutMs).toBe(250);
    expect(l.kvQuotaBytes).toBe(DEFAULT_LIMITS.kvQuotaBytes);
  });

  it('returns a frozen object', () => {
    expect(Object.isFrozen(resolveLimits({}))).toBe(true);
  });

  it('rejects a non-object', () => {
    expect(() => resolveLimits(42)).toThrow(ManifestError);
    expect(() => resolveLimits(null)).toThrow(ManifestError);
    expect(() => resolveLimits([])).toThrow(ManifestError);
  });

  it('rejects an unknown limit key rather than ignoring it', () => {
    expect(() => resolveLimits({ memoryMB: 64 })).toThrow(ManifestError);
  });

  it.each([
    ['memoryMb', 7],
    ['memoryMb', 1025],
    ['timeoutMs', 0],
    ['timeoutMs', 60_001],
    ['loadTimeoutMs', 0],
    ['kvQuotaBytes', -1],
    ['maxLogEntries', 10_001],
  ])('rejects out-of-range %s = %s', (key, value) => {
    expect(() => resolveLimits({ [key]: value })).toThrow(ManifestError);
  });

  it('rejects non-integer limits', () => {
    expect(() => resolveLimits({ timeoutMs: 1.5 })).toThrow(ManifestError);
    expect(() => resolveLimits({ timeoutMs: '100' })).toThrow(ManifestError);
    expect(() => resolveLimits({ timeoutMs: Number.NaN })).toThrow(ManifestError);
  });

  it('accepts the exact boundaries', () => {
    const l = resolveLimits({ memoryMb: 8, timeoutMs: 60_000, loadTimeoutMs: 1, kvQuotaBytes: 0, maxLogEntries: 0 });
    expect(l.memoryMb).toBe(8);
    expect(l.timeoutMs).toBe(60_000);
    expect(l.kvQuotaBytes).toBe(0);
  });
});

describe('validateManifest', () => {
  it('accepts a minimal manifest and applies defaults', () => {
    const v = validateManifest(manifest('good-plugin'));
    expect(v.name).toBe('good-plugin');
    expect(v.version).toBe('1.0.0');
    expect(v.capabilities).toEqual([]);
    expect(v.limits).toEqual(DEFAULT_LIMITS);
    expect(v.declaredSha256).toBeNull();
  });

  it('accepts pre-release versions', () => {
    expect(() => validateManifest(manifest('pre-rel', [], { version: '2.0.0-rc.1' }))).not.toThrow();
  });

  it('accepts the shortest and a trailing-hyphen name permitted by the pattern', () => {
    expect(() => validateManifest(manifest('ab'))).not.toThrow();
    expect(() => validateManifest(manifest('trailing-'))).not.toThrow();
  });

  it.each(['A', 'a', '1abc', 'has_underscore', 'x'.repeat(64)])(
    'rejects invalid name %j',
    (name) => {
      expect(() => validateManifest(manifest(name))).toThrow(ManifestError);
    },
  );

  it.each(['1.0', 'v1.0.0', '1.0.0.0', 'latest'])('rejects invalid version %j', (version) => {
    expect(() => validateManifest(manifest('p', [], { version }))).toThrow(ManifestError);
  });

  it('rejects an unknown capability', () => {
    expect(() => validateManifest(manifest('p', ['net']))).toThrow(ManifestError);
  });

  it('rejects duplicate capabilities', () => {
    expect(() => validateManifest(manifest('p', ['log', 'log']))).toThrow(ManifestError);
  });

  it('rejects non-array capabilities', () => {
    expect(() => validateManifest(manifest('p', 'log' as unknown as string[]))).toThrow(ManifestError);
  });

  it('rejects a malformed sha256 pin', () => {
    expect(() => validateManifest(manifest('p', [], { sha256: 'ABC' }))).toThrow(ManifestError);
    expect(() => validateManifest(manifest('p', [], { sha256: 'a'.repeat(63) }))).toThrow(ManifestError);
  });

  it('accepts a well-formed sha256 pin', () => {
    const pin = sha256Hex('x');
    expect(validateManifest(manifest('pin-check', [], { sha256: pin })).declaredSha256).toBe(pin);
  });

  it.each([null, undefined, 'string', 42, []])('rejects non-object manifest %j', (input) => {
    expect(() => validateManifest(input)).toThrow(ManifestError);
  });

  it('propagates limit errors', () => {
    expect(() => validateManifest(manifest('p', [], { limits: { memoryMb: 1 } }))).toThrow(ManifestError);
  });
});

describe('constructor policy', () => {
  it('rejects an unknown capability in grant', () => {
    expect(
      () => new PluginSandbox({ grant: ['shell' as never] }),
    ).toThrow(SandboxError);
  });

  it('rejects maxPlugins out of range', () => {
    expect(() => new PluginSandbox({ maxPlugins: 0 })).toThrow(ManifestError);
    expect(() => new PluginSandbox({ maxPlugins: 5000 })).toThrow(ManifestError);
  });

  it('exposes a zeroed metrics snapshot', () => {
    const s = new PluginSandbox();
    expect(s.metrics()).toEqual({
      activePlugins: 0,
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
    });
  });
});

// ---------------------------------------------------------------------------
// Rejections that happen before any worker is spawned
// ---------------------------------------------------------------------------

describe('load rejections before spawn', () => {
  let sandbox: PluginSandbox;

  beforeEach(() => {
    sandbox = new PluginSandbox({ grant: ['log', 'kv', 'clock'], clock: fixedClock });
  });

  afterEach(async () => {
    await sandbox.shutdown();
  });

  it('rejects a source that does not match the pinned sha256', async () => {
    const pin = sha256Hex('something else');
    await expect(
      sandbox.load(manifest('pinned', [], { sha256: pin }), SRC_ECHO),
    ).rejects.toBeInstanceOf(IntegrityError);
    expect(sandbox.metrics().loads).toBe(0);
  });

  it('accepts a source that matches the pinned sha256', async () => {
    const pin = sha256Hex(SRC_DOUBLE);
    const info = await sandbox.load(manifest('pinned-ok', [], { sha256: pin }), SRC_DOUBLE);
    expect(info.sha256).toBe(pin);
    await sandbox.unload('pinned-ok');
  }, WORKER_TEST_TIMEOUT);

  it('rejects a capability the host policy does not grant', async () => {
    const narrow = new PluginSandbox({ grant: ['log'] });
    await expect(
      narrow.load(manifest('needs-kv', ['kv']), SRC_KV),
    ).rejects.toBeInstanceOf(PolicyViolationError);
    await narrow.shutdown();
  });

  it('rejects a non-string source', async () => {
    await expect(
      sandbox.load(manifest('bad-src'), 123 as unknown as string),
    ).rejects.toBeInstanceOf(ManifestError);
  });

  it('rejects a source larger than the limit', async () => {
    const huge = 'a'.repeat(MAX_SOURCE_BYTES + 1);
    await expect(sandbox.load(manifest('huge'), huge)).rejects.toBeInstanceOf(ManifestError);
  });

  it('rejects an invalid manifest', async () => {
    await expect(sandbox.load({ name: 'NOPE' }, SRC_ECHO)).rejects.toBeInstanceOf(ManifestError);
  });

  it('a rejected load leaves no plugin registered', async () => {
    const narrow = new PluginSandbox({ grant: ['log'] });
    await expect(narrow.load(manifest('ghost', ['kv']), SRC_KV)).rejects.toBeInstanceOf(
      PolicyViolationError,
    );
    expect(narrow.list()).toEqual([]);
    await expect(narrow.invoke('ghost', 'echo')).rejects.toBeInstanceOf(PluginNotFoundError);
    await narrow.shutdown();
  });
});

// ---------------------------------------------------------------------------
// Load and invoke
// ---------------------------------------------------------------------------

describe('load and invoke', () => {
  let sandbox: PluginSandbox;

  beforeEach(() => {
    sandbox = new PluginSandbox({ grant: ['log', 'kv', 'clock'], clock: fixedClock });
  });

  afterEach(async () => {
    await sandbox.shutdown();
  });

  it('loads a plugin and returns its info', async () => {
    const info = await sandbox.load(manifest('echoer', ['log']), SRC_ECHO);
    expect(info.name).toBe('echoer');
    expect(info.version).toBe('1.0.0');
    expect(info.state).toBe('ready');
    expect(info.capabilities).toEqual(['log']);
    expect(info.sha256).toBe(sha256Hex(SRC_ECHO));
    expect(info.inFlight).toBe(0);
    expect(info.kvBytes).toBe(0);
  }, WORKER_TEST_TIMEOUT);

  it('invokes an exported function and returns its result', async () => {
    await sandbox.load(manifest('doubler'), SRC_DOUBLE);
    await expect(sandbox.invoke('doubler', 'double', { value: 21 })).resolves.toBe(42);
  }, WORKER_TEST_TIMEOUT);

  it('round-trips structured arguments and results', async () => {
    await sandbox.load(manifest('echoer'), SRC_ECHO);
    const payload = { nested: { list: [1, 'two', true, null] }, n: 3.5 };
    await expect(sandbox.invoke('echoer', 'echo', payload)).resolves.toEqual(payload);
  }, WORKER_TEST_TIMEOUT);

  it('invokes with undefined args', async () => {
    await sandbox.load(manifest('echoer'), SRC_ECHO);
    await expect(sandbox.invoke('echoer', 'echo')).resolves.toBeUndefined();
  }, WORKER_TEST_TIMEOUT);

  it('handles many sequential invocations', async () => {
    await sandbox.load(manifest('doubler'), SRC_DOUBLE);
    for (let i = 0; i < 50; i++) {
      await expect(sandbox.invoke('doubler', 'double', { value: i })).resolves.toBe(i * 2);
    }
    expect(sandbox.metrics().invocations).toBe(50);
  }, WORKER_TEST_TIMEOUT);

  it('handles concurrent invocations and resolves each to its own result', async () => {
    await sandbox.load(manifest('doubler'), SRC_DOUBLE);
    const results = await Promise.all(
      Array.from({ length: 20 }, (_, i) => sandbox.invoke('doubler', 'double', { value: i })),
    );
    expect(results).toEqual(Array.from({ length: 20 }, (_, i) => i * 2));
  }, WORKER_TEST_TIMEOUT);

  it('rejects a duplicate load name', async () => {
    await sandbox.load(manifest('dup'), SRC_ECHO);
    await expect(sandbox.load(manifest('dup'), SRC_ECHO)).rejects.toBeInstanceOf(
      PluginAlreadyLoadedError,
    );
  }, WORKER_TEST_TIMEOUT);

  it('enforces maxPlugins capacity', async () => {
    const small = new PluginSandbox({ maxPlugins: 1 });
    await small.load(manifest('only-one'), SRC_ECHO);
    await expect(small.load(manifest('two'), SRC_ECHO)).rejects.toBeInstanceOf(SandboxCapacityError);
    await small.shutdown();
  }, WORKER_TEST_TIMEOUT);

  it('rejects a plugin with a syntax error with PluginLoadError', async () => {
    await expect(sandbox.load(manifest('broken'), SRC_SYNTAX_ERROR)).rejects.toBeInstanceOf(
      PluginLoadError,
    );
    expect(sandbox.list()).toEqual([]);
  }, WORKER_TEST_TIMEOUT);

  it('terminates a plugin whose top-level code never finishes', async () => {
    await expect(
      sandbox.load(manifest('loop-at-load', [], { limits: { loadTimeoutMs: 200 } }), SRC_LOAD_SPIN),
    ).rejects.toBeInstanceOf(PluginLoadError);
  }, WORKER_TEST_TIMEOUT);

  it('reports a missing export as METHOD_NOT_FOUND', async () => {
    await sandbox.load(manifest('empty'), SRC_NO_EXPORTS);
    await expect(sandbox.invoke('empty', 'missing')).rejects.toMatchObject({
      pluginErrorName: 'MethodNotFoundError',
      details: { pluginCode: 'METHOD_NOT_FOUND' },
    });
  }, WORKER_TEST_TIMEOUT);

  it('does not treat inherited Object.prototype members as exports', async () => {
    await sandbox.load(manifest('proto'), SRC_ECHO);
    await expect(sandbox.invoke('proto', 'toString')).rejects.toBeInstanceOf(PluginRuntimeError);
    await expect(sandbox.invoke('proto', 'hasOwnProperty')).rejects.toBeInstanceOf(PluginRuntimeError);
  }, WORKER_TEST_TIMEOUT);

  it('surfaces a plugin exception as PluginRuntimeError with its name and code', async () => {
    await sandbox.load(manifest('thrower'), SRC_BOOM);
    const err = await sandbox.invoke('thrower', 'boom').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(PluginRuntimeError);
    expect(err).toMatchObject({
      pluginErrorName: 'TypeError',
      details: { pluginCode: 'E_BAD' },
    });
    expect((err as Error).message).toContain('bad input');
  }, WORKER_TEST_TIMEOUT);

  it('keeps the instance ready after a plugin exception', async () => {
    await sandbox.load(manifest('thrower'), SRC_BOOM);
    await expect(sandbox.invoke('thrower', 'boom')).rejects.toBeInstanceOf(PluginRuntimeError);
    expect(sandbox.info('thrower').state).toBe('ready');
  }, WORKER_TEST_TIMEOUT);

  it('reports a non-cloneable return value as a runtime error, not a hang', async () => {
    await sandbox.load(manifest('fnret'), SRC_FN_RETURN);
    await expect(sandbox.invoke('fnret', 'fn')).rejects.toBeInstanceOf(PluginRuntimeError);
  }, WORKER_TEST_TIMEOUT);

  it('rejects an invalid method identifier before dispatch', async () => {
    await sandbox.load(manifest('doubler'), SRC_DOUBLE);
    await expect(sandbox.invoke('doubler', '../etc')).rejects.toBeInstanceOf(InvalidInvocationError);
    await expect(sandbox.invoke('doubler', '')).rejects.toBeInstanceOf(InvalidInvocationError);
    await expect(sandbox.invoke('doubler', 42 as unknown as string)).rejects.toBeInstanceOf(
      InvalidInvocationError,
    );
  }, WORKER_TEST_TIMEOUT);

  it('rejects an out-of-range per-call timeout', async () => {
    await sandbox.load(manifest('doubler'), SRC_DOUBLE);
    await expect(sandbox.invoke('doubler', 'double', {}, { timeoutMs: 0 })).rejects.toBeInstanceOf(
      InvalidInvocationError,
    );
    await expect(
      sandbox.invoke('doubler', 'double', {}, { timeoutMs: 70_000 }),
    ).rejects.toBeInstanceOf(InvalidInvocationError);
  }, WORKER_TEST_TIMEOUT);

  it('invoking an unknown plugin rejects with PluginNotFoundError', async () => {
    await expect(sandbox.invoke('nobody', 'echo')).rejects.toBeInstanceOf(PluginNotFoundError);
  });

  it('counts invocations in metrics', async () => {
    await sandbox.load(manifest('doubler'), SRC_DOUBLE);
    await sandbox.invoke('doubler', 'double', { value: 1 });
    await sandbox.invoke('doubler', 'double', { value: 2 });
    expect(sandbox.metrics().invocations).toBe(2);
    expect(sandbox.metrics().loads).toBe(1);
    expect(sandbox.metrics().activePlugins).toBe(1);
  }, WORKER_TEST_TIMEOUT);

  it('lists plugins sorted by name', async () => {
    await sandbox.load(manifest('zeta'), SRC_ECHO);
    await sandbox.load(manifest('alpha'), SRC_ECHO);
    expect(sandbox.list().map((p) => p.name)).toEqual(['alpha', 'zeta']);
  }, WORKER_TEST_TIMEOUT);

  it('info() on an unknown plugin throws PluginNotFoundError', () => {
    expect(() => sandbox.info('nope')).toThrow(PluginNotFoundError);
  });

  it('emits a loaded event', async () => {
    const seen: Array<[string, string]> = [];
    sandbox.on('loaded', (name: string, version: string) => seen.push([name, version]));
    await sandbox.load(manifest('evented'), SRC_ECHO);
    expect(seen).toEqual([['evented', '1.0.0']]);
  }, WORKER_TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
// Watchdog, memory limit, and fault handling
// ---------------------------------------------------------------------------

describe('execution watchdog and memory limit', () => {
  let sandbox: PluginSandbox;

  beforeEach(() => {
    sandbox = new PluginSandbox({ grant: ['log', 'kv', 'clock'], clock: fixedClock });
  });

  afterEach(async () => {
    await sandbox.shutdown();
  });

  it('terminates a synchronous infinite loop with PluginTimeoutError', async () => {
    await sandbox.load(manifest('spinner', [], { limits: { timeoutMs: 200 } }), SRC_SPIN);
    await expect(sandbox.invoke('spinner', 'spin')).rejects.toBeInstanceOf(PluginTimeoutError);
    expect(sandbox.info('spinner').state).toBe('faulted');
    expect(sandbox.metrics().timeouts).toBe(1);
  }, WORKER_TEST_TIMEOUT);

  it('honours a per-call timeout override', async () => {
    await sandbox.load(manifest('spinner'), SRC_SPIN);
    await expect(
      sandbox.invoke('spinner', 'spin', undefined, { timeoutMs: 150 }),
    ).rejects.toMatchObject({ code: 'PLUGIN_TIMEOUT', details: { timeoutMs: 150 } });
  }, WORKER_TEST_TIMEOUT);

  it('emits a fault event carrying the timeout error', async () => {
    const faults: Array<{ plugin: string; error: unknown }> = [];
    sandbox.on('fault', (plugin: string, error: unknown) => faults.push({ plugin, error }));
    await sandbox.load(manifest('spinner', [], { limits: { timeoutMs: 200 } }), SRC_SPIN);
    await sandbox.invoke('spinner', 'spin').catch(() => undefined);
    expect(faults).toHaveLength(1);
    expect(faults[0]?.plugin).toBe('spinner');
    expect(faults[0]?.error).toBeInstanceOf(PluginTimeoutError);
  }, WORKER_TEST_TIMEOUT);

  it('rejects calls to a faulted plugin with PluginCrashedError', async () => {
    await sandbox.load(manifest('spinner', [], { limits: { timeoutMs: 200 } }), SRC_SPIN);
    await sandbox.invoke('spinner', 'spin').catch(() => undefined);
    await expect(sandbox.invoke('spinner', 'spin')).rejects.toBeInstanceOf(PluginCrashedError);
  }, WORKER_TEST_TIMEOUT);

  it('restarts a faulted plugin and serves calls again', async () => {
    await sandbox.load(manifest('phoenix', [], { limits: { timeoutMs: 200 } }), SRC_SPIN);
    await sandbox.invoke('phoenix', 'spin').catch(() => undefined);

    const info = await sandbox.restart('phoenix');
    expect(info.state).toBe('ready');
    await expect(sandbox.invoke('phoenix', 'spin')).rejects.toBeInstanceOf(PluginTimeoutError);
    expect(sandbox.metrics().restarts).toBe(1);
  }, WORKER_TEST_TIMEOUT);

  it('restart on a ready plugin is rejected with INVALID_STATE', async () => {
    await sandbox.load(manifest('healthy'), SRC_ECHO);
    await expect(sandbox.restart('healthy')).rejects.toMatchObject({ code: 'INVALID_STATE' });
  }, WORKER_TEST_TIMEOUT);

  it('restart preserves KV state across the crash', async () => {
    await sandbox.load(
      manifest('durable', ['kv'], { limits: { timeoutMs: 200 } }),
      SRC_KV + ' module.exports.spin = () => { while (true) {} };',
    );
    await sandbox.invoke('durable', 'put', { key: 'k', value: 'kept' });
    await sandbox.invoke('durable', 'spin').catch(() => undefined);
    await sandbox.restart('durable');
    await expect(sandbox.invoke('durable', 'get', { key: 'k' })).resolves.toBe('kept');
  }, WORKER_TEST_TIMEOUT);

  it('fails all other in-flight calls with PluginCrashedError when one times out', async () => {
    await sandbox.load(manifest('shared', [], { limits: { timeoutMs: 20_000 } }), SRC_SPIN + ' ' + SRC_ECHO);
    // The spin blocks the worker thread, so the echo can never be served; the
    // spin's own watchdog fires and the echo must fail as a crash, not hang.
    const spin = sandbox.invoke('shared', 'spin', undefined, { timeoutMs: 200 });
    const echo = sandbox.invoke('shared', 'echo', 'hi', { timeoutMs: 20_000 });
    const [spinResult, echoResult] = await Promise.allSettled([spin, echo]);
    expect(spinResult.status).toBe('rejected');
    expect((spinResult as PromiseRejectedResult).reason).toBeInstanceOf(PluginTimeoutError);
    expect(echoResult.status).toBe('rejected');
    expect((echoResult as PromiseRejectedResult).reason).toBeInstanceOf(PluginCrashedError);
  }, WORKER_TEST_TIMEOUT);

  it('terminates a plugin that exceeds its heap limit with PluginMemoryLimitError', async () => {
    await sandbox.load(
      manifest('hog', [], { limits: { memoryMb: 16, timeoutMs: 15_000 } }),
      SRC_HOG,
    );
    await expect(sandbox.invoke('hog', 'hog')).rejects.toBeInstanceOf(PluginMemoryLimitError);
    expect(sandbox.metrics().memoryFaults).toBe(1);
    expect(sandbox.info('hog').state).toBe('faulted');
  }, WORKER_TEST_TIMEOUT);

  it('a timed-out plugin does not affect a sibling plugin', async () => {
    await sandbox.load(manifest('bad-neighbour', [], { limits: { timeoutMs: 200 } }), SRC_SPIN);
    await sandbox.load(manifest('good-neighbour'), SRC_DOUBLE);
    await sandbox.invoke('bad-neighbour', 'spin').catch(() => undefined);
    await expect(sandbox.invoke('good-neighbour', 'double', { value: 5 })).resolves.toBe(10);
  }, WORKER_TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
// Capability sandboxing and host API
// ---------------------------------------------------------------------------

describe('capabilities', () => {
  let sandbox: PluginSandbox;

  beforeEach(() => {
    sandbox = new PluginSandbox({ grant: ['log', 'kv', 'clock'], clock: fixedClock });
  });

  afterEach(async () => {
    await sandbox.shutdown();
  });

  it('log capability writes an entry visible to the host', async () => {
    await sandbox.load(manifest('logger', ['log']), SRC_LOGGER);
    await expect(sandbox.invoke('logger', 'say', { msg: 'hello' })).resolves.toBe('logged');
    const entries = sandbox.logs('logger');
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({ level: 'info', message: 'hello', at: FIXED_NOW });
  }, WORKER_TEST_TIMEOUT);

  it('emits a log event for each entry', async () => {
    const seen: Array<{ plugin: string; entry: LogEntry }> = [];
    sandbox.on('log', (plugin: string, entry: LogEntry) => seen.push({ plugin, entry }));
    await sandbox.load(manifest('logger', ['log']), SRC_LOGGER);
    await sandbox.invoke('logger', 'say', { msg: 'a' });
    expect(seen).toHaveLength(1);
    expect(seen[0]?.plugin).toBe('logger');
  }, WORKER_TEST_TIMEOUT);

  it('rejects an invalid log level with INVALID_HOST_CALL inside the plugin', async () => {
    await sandbox.load(manifest('bad-logger', ['log']), SRC_BAD_LOG);
    await expect(sandbox.invoke('bad-logger', 'bad')).resolves.toBe('INVALID_HOST_CALL');
  }, WORKER_TEST_TIMEOUT);

  it('denies a capability missing from the manifest and reports it', async () => {
    const denials: Array<{ plugin: string; capability: string; op: string }> = [];
    sandbox.on('denied', (e: { plugin: string; capability: string; op: string }) => denials.push(e));
    await sandbox.load(manifest('no-kv'), SRC_TRY_KV);
    await expect(sandbox.invoke('no-kv', 'tryKv')).resolves.toBe('CAPABILITY_DENIED');
    expect(denials).toEqual([{ plugin: 'no-kv', capability: 'kv', op: 'get' }]);
    expect(sandbox.metrics().capabilityDenials).toBe(1);
  }, WORKER_TEST_TIMEOUT);

  it('a denied call does not disturb the plugin', async () => {
    await sandbox.load(manifest('no-kv'), SRC_TRY_KV);
    await sandbox.invoke('no-kv', 'tryKv');
    expect(sandbox.info('no-kv').state).toBe('ready');
  }, WORKER_TEST_TIMEOUT);

  it('clock capability returns the injected clock value', async () => {
    await sandbox.load(manifest('timekeeper', ['clock']), SRC_CLOCK);
    await expect(sandbox.invoke('timekeeper', 'time')).resolves.toBe(FIXED_NOW);
  }, WORKER_TEST_TIMEOUT);

  it('kv capability supports set, get, and delete', async () => {
    await sandbox.load(manifest('store', ['kv']), SRC_KV);
    await sandbox.invoke('store', 'put', { key: 'user', value: { id: 7, tags: ['a'] } });
    await expect(sandbox.invoke('store', 'get', { key: 'user' })).resolves.toEqual({
      id: 7,
      tags: ['a'],
    });
    await expect(sandbox.invoke('store', 'del', { key: 'user' })).resolves.toBe(true);
    await expect(sandbox.invoke('store', 'get', { key: 'user' })).resolves.toBeNull();
    await expect(sandbox.invoke('store', 'del', { key: 'user' })).resolves.toBe(false);
  }, WORKER_TEST_TIMEOUT);

  it('reports kv usage through info()', async () => {
    await sandbox.load(manifest('store', ['kv']), SRC_KV);
    await sandbox.invoke('store', 'put', { key: 'k', value: 'v' });
    const info = sandbox.info('store');
    expect(info.kvEntries).toBe(1);
    expect(info.kvBytes).toBeGreaterThan(0);
  }, WORKER_TEST_TIMEOUT);

  it('kv write past the quota is refused inside the plugin with KV_QUOTA_EXCEEDED', async () => {
    await sandbox.load(
      manifest('tight', ['kv'], { limits: { kvQuotaBytes: 200 } }),
      SRC_KV_QUOTA + ' ' + SRC_KV,
    );
    await expect(sandbox.invoke('tight', 'big')).resolves.toBe('KV_QUOTA_EXCEEDED');
    expect(sandbox.info('tight').kvBytes).toBe(0);
  }, WORKER_TEST_TIMEOUT);

  it('kv write of undefined is refused with KV_INVALID_VALUE', async () => {
    await sandbox.load(manifest('undef', ['kv']), SRC_KV_UNDEFINED);
    await expect(sandbox.invoke('undef', 'bad')).resolves.toBe('KV_INVALID_VALUE');
  }, WORKER_TEST_TIMEOUT);

  it('bounds retained log entries but counts all of them', async () => {
    await sandbox.load(manifest('chatty', ['log'], { limits: { maxLogEntries: 2 } }), SRC_LOGGER);
    await sandbox.invoke('chatty', 'say', { msg: '1' });
    await sandbox.invoke('chatty', 'say', { msg: '2' });
    await sandbox.invoke('chatty', 'say', { msg: '3' });
    expect(sandbox.logs('chatty').map((e) => e.message)).toEqual(['2', '3']);
    expect(sandbox.info('chatty').logCount).toBe(3);
  }, WORKER_TEST_TIMEOUT);

  it('logs() on an unknown plugin throws PluginNotFoundError', () => {
    expect(() => sandbox.logs('nobody')).toThrow(PluginNotFoundError);
  });

  // The vm realm is not a hard boundary: a plugin can reach the child's real
  // `process` through a host function's constructor. These tests pin the
  // containment that the process-level permission model is responsible for.
  const ESCAPE = "const p = () => api.log.constructor('return process')();";

  it('an escaped plugin cannot read host files (permission model)', async () => {
    await sandbox.load(
      manifest('escaper-fs', ['log']),
      ESCAPE +
        " module.exports.readFile = () => { try { return p().getBuiltinModule('node:fs')" +
        ".readFileSync('/etc/hostname', 'utf8'); } catch (e) { return e.code; } };",
    );
    await expect(sandbox.invoke('escaper-fs', 'readFile')).resolves.toBe('ERR_ACCESS_DENIED');
  }, WORKER_TEST_TIMEOUT);

  it('an escaped plugin cannot spawn subprocesses (permission model)', async () => {
    await sandbox.load(
      manifest('escaper-proc', ['log']),
      ESCAPE +
        " module.exports.spawn = () => { try { p().getBuiltinModule('node:child_process')" +
        ".execSync('echo pwned'); return 'spawned'; } catch (e) { return e.code; } };",
    );
    await expect(sandbox.invoke('escaper-proc', 'spawn')).resolves.toBe('ERR_ACCESS_DENIED');
  }, WORKER_TEST_TIMEOUT);

  it('an escaped plugin sees an empty environment, not host secrets', async () => {
    process.env['NEXUS_SANDBOX_TEST_SECRET'] = 'do-not-leak';
    try {
      await sandbox.load(
        manifest('escaper-env', ['log']),
        ESCAPE + " module.exports.env = () => p().env['NEXUS_SANDBOX_TEST_SECRET'] ?? null;",
      );
      await expect(sandbox.invoke('escaper-env', 'env')).resolves.toBeNull();
    } finally {
      delete process.env['NEXUS_SANDBOX_TEST_SECRET'];
    }
  }, WORKER_TEST_TIMEOUT);

  it('host policy grant caps what a manifest may declare', async () => {
    const readOnly = new PluginSandbox({ grant: ['log'] });
    await expect(readOnly.load(manifest('clock-user', ['clock']), SRC_CLOCK)).rejects.toBeInstanceOf(
      PolicyViolationError,
    );
    await readOnly.shutdown();
  });
});

// ---------------------------------------------------------------------------
// Hot reload
// ---------------------------------------------------------------------------

describe('hot reload', () => {
  let sandbox: PluginSandbox;

  beforeEach(() => {
    sandbox = new PluginSandbox({ grant: ['log', 'kv', 'clock'], clock: fixedClock });
  });

  afterEach(async () => {
    await sandbox.shutdown();
  });

  it('replaces the running version with the new source', async () => {
    await sandbox.load(manifest('versioned'), SRC_V1);
    await expect(sandbox.invoke('versioned', 'v')).resolves.toBe(1);

    const info = await sandbox.reload(manifest('versioned'), SRC_V2);
    expect(info.sha256).toBe(sha256Hex(SRC_V2));
    await expect(sandbox.invoke('versioned', 'v')).resolves.toBe(2);
    expect(sandbox.metrics().reloads).toBe(1);
  }, WORKER_TEST_TIMEOUT);

  it('can bump the version string on reload', async () => {
    await sandbox.load(manifest('versioned'), SRC_V1);
    const info = await sandbox.reload(manifest('versioned', [], { version: '1.1.0' }), SRC_V2);
    expect(info.version).toBe('1.1.0');
  }, WORKER_TEST_TIMEOUT);

  it('keeps KV state across a reload', async () => {
    await sandbox.load(manifest('stateful', ['kv']), SRC_KV + ' ' + SRC_V1);
    await sandbox.invoke('stateful', 'put', { key: 'counter', value: 41 });
    await sandbox.reload(manifest('stateful', ['kv']), SRC_KV + ' ' + SRC_V2);
    await expect(sandbox.invoke('stateful', 'get', { key: 'counter' })).resolves.toBe(41);
  }, WORKER_TEST_TIMEOUT);

  it('a call in flight on the old version completes on it', async () => {
    await sandbox.load(manifest('draining'), SRC_SLOW);
    const inFlight = sandbox.invoke('draining', 'slow');
    const reloaded = sandbox.reload(manifest('draining'), SRC_V2);
    await expect(inFlight).resolves.toBe(4499998500000);
    await reloaded;
    await expect(sandbox.invoke('draining', 'v')).resolves.toBe(2);
  }, WORKER_TEST_TIMEOUT);

  it('a failed reload keeps the previous version running (rollback)', async () => {
    await sandbox.load(manifest('stable'), SRC_V1);
    await expect(sandbox.reload(manifest('stable'), SRC_SYNTAX_ERROR)).rejects.toBeInstanceOf(
      PluginLoadError,
    );
    await expect(sandbox.invoke('stable', 'v')).resolves.toBe(1);
    expect(sandbox.metrics().reloads).toBe(0);
  }, WORKER_TEST_TIMEOUT);

  it('an integrity mismatch on reload is refused and the old version survives', async () => {
    await sandbox.load(manifest('pinned-r'), SRC_V1);
    await expect(
      sandbox.reload(manifest('pinned-r', [], { sha256: sha256Hex('other') }), SRC_V2),
    ).rejects.toBeInstanceOf(IntegrityError);
    await expect(sandbox.invoke('pinned-r', 'v')).resolves.toBe(1);
  }, WORKER_TEST_TIMEOUT);

  it('a reload that requests an ungranted capability is refused', async () => {
    await sandbox.load(manifest('narrow'), SRC_V1);
    const narrow = new PluginSandbox({ grant: ['log'] });
    await narrow.load(manifest('narrow'), SRC_V1);
    await expect(narrow.reload(manifest('narrow', ['kv']), SRC_KV)).rejects.toBeInstanceOf(
      PolicyViolationError,
    );
    await expect(narrow.invoke('narrow', 'v')).resolves.toBe(1);
    await narrow.shutdown();
  }, WORKER_TEST_TIMEOUT);

  it('reloading an unknown plugin rejects with PluginNotFoundError', async () => {
    await expect(sandbox.reload(manifest('never-loaded'), SRC_V2)).rejects.toBeInstanceOf(
      PluginNotFoundError,
    );
  });

  it('a reload that shrinks the KV quota below current usage is refused and quota is restored', async () => {
    await sandbox.load(manifest('shrink', ['kv']), SRC_KV);
    await sandbox.invoke('shrink', 'put', { key: 'k', value: 'x'.repeat(100) });
    await expect(
      sandbox.reload(manifest('shrink', ['kv'], { limits: { kvQuotaBytes: 16 } }), SRC_KV),
    ).rejects.toBeInstanceOf(KvQuotaExceededError);
    // Original quota still applies, so a small write still succeeds.
    await sandbox.invoke('shrink', 'put', { key: 'small', value: 1 });
    await expect(sandbox.invoke('shrink', 'get', { key: 'small' })).resolves.toBe(1);
  }, WORKER_TEST_TIMEOUT);

  it('reload can recover a faulted plugin with corrected source', async () => {
    await sandbox.load(manifest('recover', [], { limits: { timeoutMs: 200 } }), SRC_SPIN);
    await sandbox.invoke('recover', 'spin').catch(() => undefined);
    await sandbox.reload(manifest('recover', [], { limits: { timeoutMs: 200 } }), SRC_V2);
    await expect(sandbox.invoke('recover', 'v')).resolves.toBe(2);
  }, WORKER_TEST_TIMEOUT);

  it('emits a reloaded event', async () => {
    const seen: Array<[string, string]> = [];
    sandbox.on('reloaded', (name: string, version: string) => seen.push([name, version]));
    await sandbox.load(manifest('evt'), SRC_V1);
    await sandbox.reload(manifest('evt', [], { version: '2.0.0' }), SRC_V2);
    expect(seen).toEqual([['evt', '2.0.0']]);
  }, WORKER_TEST_TIMEOUT);
});

// ---------------------------------------------------------------------------
// Unload and shutdown
// ---------------------------------------------------------------------------

describe('unload and shutdown', () => {
  let sandbox: PluginSandbox;

  beforeEach(() => {
    sandbox = new PluginSandbox({ grant: ['kv'], clock: fixedClock });
  });

  afterEach(async () => {
    await sandbox.shutdown();
  });

  it('unload removes the plugin so later calls are not found', async () => {
    await sandbox.load(manifest('gone'), SRC_ECHO);
    await sandbox.unload('gone');
    await expect(sandbox.invoke('gone', 'echo')).rejects.toBeInstanceOf(PluginNotFoundError);
    expect(sandbox.list()).toEqual([]);
    expect(sandbox.metrics().unloads).toBe(1);
  }, WORKER_TEST_TIMEOUT);

  it('unloading twice rejects with PluginNotFoundError', async () => {
    await sandbox.load(manifest('once'), SRC_ECHO);
    await sandbox.unload('once');
    await expect(sandbox.unload('once')).rejects.toBeInstanceOf(PluginNotFoundError);
  }, WORKER_TEST_TIMEOUT);

  it('unload discards KV state, so a re-load starts empty', async () => {
    await sandbox.load(manifest('forgetful', ['kv']), SRC_KV);
    await sandbox.invoke('forgetful', 'put', { key: 'k', value: 'v' });
    await sandbox.unload('forgetful');
    await sandbox.load(manifest('forgetful', ['kv']), SRC_KV);
    await expect(sandbox.invoke('forgetful', 'get', { key: 'k' })).resolves.toBeNull();
  }, WORKER_TEST_TIMEOUT);

  it('unload waits for in-flight calls to finish', async () => {
    await sandbox.load(manifest('finisher'), SRC_SLOW);
    const call = sandbox.invoke('finisher', 'slow');
    await sandbox.unload('finisher');
    await expect(call).resolves.toBe(4499998500000);
  }, WORKER_TEST_TIMEOUT);

  it('emits an unloaded event', async () => {
    const seen: string[] = [];
    sandbox.on('unloaded', (name: string) => seen.push(name));
    await sandbox.load(manifest('bye'), SRC_ECHO);
    await sandbox.unload('bye');
    expect(seen).toEqual(['bye']);
  }, WORKER_TEST_TIMEOUT);

  it('shutdown retires every plugin and clears the registry', async () => {
    await sandbox.load(manifest('alpha-one'), SRC_ECHO);
    await sandbox.load(manifest('beta-two'), SRC_ECHO);
    await sandbox.shutdown();
    expect(sandbox.list()).toEqual([]);
    expect(sandbox.metrics().activePlugins).toBe(0);
  }, WORKER_TEST_TIMEOUT);

  it('after shutdown, load, invoke, and reload are refused with SandboxClosedError', async () => {
    await sandbox.shutdown();
    await expect(sandbox.load(manifest('late'), SRC_ECHO)).rejects.toBeInstanceOf(SandboxClosedError);
    await expect(sandbox.invoke('late', 'echo')).rejects.toBeInstanceOf(SandboxClosedError);
    await expect(sandbox.reload(manifest('late'), SRC_ECHO)).rejects.toBeInstanceOf(SandboxClosedError);
  });

  it('shutdown is idempotent', async () => {
    await sandbox.shutdown();
    await expect(sandbox.shutdown()).resolves.toBeUndefined();
  });

  it('shutdown completes even when a plugin is faulted', async () => {
    await sandbox.load(manifest('dead', [], { limits: { timeoutMs: 200 } }), SRC_SPIN);
    await sandbox.invoke('dead', 'spin').catch(() => undefined);
    await expect(sandbox.shutdown()).resolves.toBeUndefined();
  }, WORKER_TEST_TIMEOUT);

  it('errors thrown by the sandbox are SandboxError instances with a stable code', async () => {
    const err = await sandbox.invoke('absent', 'x').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(SandboxError);
    expect((err as SandboxError).code).toBe('PLUGIN_NOT_FOUND');
  });
});
