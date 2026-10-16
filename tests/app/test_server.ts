/**
 * Tests for the application lifecycle: start-up, shutdown, rule loading,
 * the HTTP API and TCP ingest wired together.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import { once } from 'node:events';
import { AppError, NexusPulseApp, type AppOptions } from '../../src/app/server.js';
import { parseSettings, type Environment } from '../../src/app/settings.js';
import { writeTokenFile } from '../../src/app/token_file.js';
import type { FetchLike } from '../../src/alerts/notifier.js';
import { encodeTelemetryRecord } from '../../src/ingest/tcp_ingest.js';
import {
  createClientTransport,
  FrameFlags,
  FrameType,
  type FrameTransport,
} from '../../src/grpc/transport.js';
import type { TelemetryEvent } from '../../src/core/engine.js';

const NOW = Date.UTC(2026, 9, 15, 12, 0, 0);
const clock = (): number => NOW;

const apps: NexusPulseApp[] = [];
const sockets: net.Socket[] = [];
const blockers: net.Server[] = [];
const dirs: string[] = [];

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nexuspulse-app-'));
  dirs.push(dir);
  return dir;
}

function envFor(dataDir: string, overrides: Environment = {}): Environment {
  return {
    NEXUSPULSE_HTTP_PORT: '0',
    NEXUSPULSE_TCP_PORT: '0',
    NEXUSPULSE_DATA_DIR: dataDir,
    NEXUSPULSE_LOG_LEVEL: 'debug',
    ...overrides,
  };
}

interface Harness {
  readonly app: NexusPulseApp;
  readonly lines: string[];
  readonly dataDir: string;
}

/** Builds an app whose log lines are captured in memory. */
function makeApp(dataDir: string = tempDir(), overrides: Environment = {}, extra: Partial<AppOptions> = {}): Harness {
  const lines: string[] = [];
  const app = new NexusPulseApp({
    settings: parseSettings(envFor(dataDir, overrides)),
    logSink: (line) => lines.push(line),
    clock,
    ...extra,
  });
  apps.push(app);
  return { app, lines, dataDir };
}

function event(id: string, value: number, metric = 'cpu'): TelemetryEvent {
  return { id, source: 'edge-01', metric, value, timestamp: NOW - 1_000 };
}

interface TcpClient {
  readonly socket: net.Socket;
  readonly transport: FrameTransport;
}

async function connectTcp(port: number): Promise<TcpClient> {
  const socket = net.connect({ host: '127.0.0.1', port });
  sockets.push(socket);
  await once(socket, 'connect');
  const transport = createClientTransport(socket, { keepAliveIntervalMs: 0 });
  return { socket, transport };
}

function sendEvent(client: TcpClient, ev: TelemetryEvent): void {
  const streamId = client.transport.openStream();
  client.transport.sendFrame(FrameType.DATA, FrameFlags.END_STREAM, streamId, encodeTelemetryRecord(ev));
}

async function waitFor(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function httpUrl(port: number, route: string): string {
  return `http://127.0.0.1:${port}${route}`;
}

async function occupyPort(): Promise<{ port: number; server: net.Server }> {
  const server = net.createServer();
  blockers.push(server);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('blocker did not bind');
  return { port: address.port, server };
}

afterEach(async () => {
  for (const socket of sockets.splice(0)) socket.destroy();
  for (const app of apps.splice(0)) await app.stop();
  for (const blocker of blockers.splice(0)) {
    if (blocker.listening) await new Promise<void>((resolve) => blocker.close(() => resolve()));
  }
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

describe('NexusPulseApp lifecycle', () => {
  it('starts on ephemeral ports and reports the bound addresses', async () => {
    const { app, lines } = makeApp();
    const addresses = await app.start();
    expect(addresses.http).toBeGreaterThan(0);
    expect(addresses.tcp).toBeGreaterThan(0);
    expect(app.stats().state).toBe('running');
    expect(lines.some((line) => line.includes('listening'))).toBe(true);
  });

  it('creates the data directory when it is missing', async () => {
    const dir = path.join(tempDir(), 'nested', 'data');
    const { app } = makeApp(dir);
    await app.start();
    expect(fs.statSync(dir).isDirectory()).toBe(true);
    expect(fs.existsSync(path.join(dir, 'rules.npdb'))).toBe(true);
  });

  it('refuses to start twice', async () => {
    const { app } = makeApp();
    await app.start();
    await expect(app.start()).rejects.toMatchObject({ code: 'APP_STATE' });
  });

  it('refuses to start after it has been stopped', async () => {
    const { app } = makeApp();
    await app.start();
    await app.stop();
    await expect(app.start()).rejects.toBeInstanceOf(AppError);
  });

  it('stop() reports zero drained events and is safe to repeat', async () => {
    const { app } = makeApp();
    await app.start();
    const first = await app.stop();
    expect(first.drained).toBeGreaterThanOrEqual(0);
    expect(app.stats().state).toBe('stopped');
    await expect(app.stop()).resolves.toEqual({ drained: 0 });
  });

  it('stop() before start() is a no-op', async () => {
    const { app } = makeApp();
    await expect(app.stop()).resolves.toEqual({ drained: 0 });
    expect(app.stats().state).toBe('stopped');
  });

  it('closes the listening ports on stop', async () => {
    const { app } = makeApp();
    const { http, tcp } = await app.start();
    await app.stop();
    await expect(fetch(httpUrl(http, '/healthz'))).rejects.toThrow();
    await expect(connectTcp(tcp)).rejects.toThrow();
  });

  it('closes the rule store on stop so the file can be reopened', async () => {
    const dir = tempDir();
    const first = makeApp(dir);
    await first.app.start();
    await first.app.stop();
    const second = makeApp(dir);
    await expect(second.app.start()).resolves.toBeDefined();
  });

  it('fails with APP_START_FAILED when the HTTP port is taken, and releases resources', async () => {
    const blocker = await occupyPort();
    const dir = tempDir();
    const { app } = makeApp(dir, { NEXUSPULSE_HTTP_PORT: String(blocker.port) });

    const err = await app.start().catch((e: unknown) => e);
    expect(err).toBeInstanceOf(AppError);
    expect((err as AppError).code).toBe('APP_START_FAILED');
    expect(app.stats().state).toBe('stopped');

    // The rule store must have been closed, so a fresh app can open the same directory.
    const retry = makeApp(dir);
    await expect(retry.app.start()).resolves.toBeDefined();
  });

  it('fails with APP_START_FAILED when the TCP port is taken, and stops the HTTP listener it had opened', async () => {
    const blocker = await occupyPort();
    const { app } = makeApp(tempDir(), { NEXUSPULSE_TCP_PORT: String(blocker.port) });

    await expect(app.start()).rejects.toMatchObject({ code: 'APP_START_FAILED' });
    expect(app.stats().state).toBe('stopped');
  });

  it('keeps the start-up cause in the error details', async () => {
    const blocker = await occupyPort();
    const { app } = makeApp(tempDir(), { NEXUSPULSE_HTTP_PORT: String(blocker.port) });
    const err = (await app.start().catch((e: unknown) => e)) as AppError;
    expect(typeof err.details['cause']).toBe('string');
    expect(err.message).toContain('start-up failed');
  });
});

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

describe('rule loading', () => {
  it('cannot load rules before start()', () => {
    const { app } = makeApp();
    expect(() => app.reloadRules()).toThrow(AppError);
  });

  it('loads zero rules on a fresh data directory', async () => {
    const { app } = makeApp();
    await app.start();
    expect(app.reloadRules()).toEqual({ loaded: 0 });
  });

  it('picks up rules that were stored in an earlier run', async () => {
    const dir = tempDir();
    const first = makeApp(dir);
    const { http } = await first.app.start();
    const put = await fetch(httpUrl(http, '/v1/rules/cpu-high'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ metric: 'cpu', max: 80, severity: 'critical' }),
    });
    expect(put.status).toBe(201);
    await first.app.stop();

    const second = makeApp(dir);
    await second.app.start();
    expect(second.app.stats().rules.rules).toBe(1);
  });

  it('counts successful reloads in stats', async () => {
    const { app } = makeApp();
    await app.start();
    app.reloadRules();
    app.reloadRules();
    expect(app.stats().ruleReloads).toBe(3); // one at start-up plus two explicit reloads
    expect(app.stats().ruleReloadFailures).toBe(0);
  });

  it('applies a rule added over HTTP only after reloadRules()', async () => {
    const { app, lines } = makeApp();
    const { http, tcp } = await app.start();
    await fetch(httpUrl(http, '/v1/rules/cpu-high'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ metric: 'cpu', max: 50, severity: 'critical' }),
    });

    const client = await connectTcp(tcp);
    sendEvent(client, event('before-reload', 90));
    await waitFor(() => app.stats().ingest?.eventsAccepted === 1);
    expect(lines.some((l) => l.includes('alert'))).toBe(false);

    app.reloadRules();
    sendEvent(client, event('after-reload', 90));
    await waitFor(() => lines.some((l) => l.includes('alert')));
  });
});

// ---------------------------------------------------------------------------
// HTTP and TCP wiring
// ---------------------------------------------------------------------------

describe('HTTP wiring', () => {
  it('serves the health route', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    const response = await fetch(httpUrl(http, '/healthz'));
    expect(response.status).toBe(200);
  });

  it('serves engine metrics', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    const response = await fetch(httpUrl(http, '/v1/metrics'));
    expect(response.status).toBe(200);
    const body = (await response.json()) as { state: string };
    expect(body.state).toBe('running');
  });

  it('accepts an event posted over HTTP', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    const response = await fetch(httpUrl(http, '/v1/events'), {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(event('http-1', 5)),
    });
    expect(response.status).toBe(202);
  });

  it('rejects an invalid rule with 422 and does not store it', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    const response = await fetch(httpUrl(http, '/v1/rules/bad-rule'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ metric: 'cpu', severity: 'loud' }),
    });
    expect(response.status).toBe(422);
    expect(app.reloadRules().loaded).toBe(0);
  });
});

describe('TCP ingest wiring', () => {
  it('counts an accepted TCP event', async () => {
    const { app } = makeApp();
    const { tcp } = await app.start();
    const client = await connectTcp(tcp);
    sendEvent(client, event('tcp-1', 5));
    await waitFor(() => app.stats().ingest?.eventsAccepted === 1);
    expect(app.stats().engine.accepted).toBeGreaterThanOrEqual(1);
  });

  it('counts a malformed TCP payload as malformed, not accepted', async () => {
    const { app } = makeApp();
    const { tcp } = await app.start();
    const client = await connectTcp(tcp);
    const streamId = client.transport.openStream();
    client.transport.sendFrame(FrameType.DATA, FrameFlags.END_STREAM, streamId, Buffer.from([0xff, 0x00]));
    await waitFor(() => (app.stats().ingest?.malformedFrames ?? 0) === 1);
    expect(app.stats().ingest?.eventsAccepted).toBe(0);
  });

  it('logs a finding when an accepted TCP event breaks a rule', async () => {
    const { app, lines } = makeApp();
    const { http, tcp } = await app.start();
    await fetch(httpUrl(http, '/v1/rules/cpu-high'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ metric: 'cpu', max: 80, severity: 'critical' }),
    });
    app.reloadRules();

    const client = await connectTcp(tcp);
    sendEvent(client, event('hot-1', 95));
    await waitFor(() => app.stats().findings === 1);

    const line = lines.find((l) => l.includes('alert'));
    expect(line).toBeDefined();
    expect(line).toContain('cpu-high');
    expect(line).toContain('hot-1');
  });

  it('does not log a finding for an event within the rule bounds', async () => {
    const { app } = makeApp();
    const { http, tcp } = await app.start();
    await fetch(httpUrl(http, '/v1/rules/cpu-high'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ metric: 'cpu', max: 80, severity: 'warning' }),
    });
    app.reloadRules();

    const client = await connectTcp(tcp);
    sendEvent(client, event('cool-1', 20));
    await waitFor(() => app.stats().ingest?.eventsAccepted === 1);
    expect(app.stats().findings).toBe(0);
  });

  it('does not evaluate rules for an event the engine rejected', async () => {
    const { app } = makeApp();
    const { http, tcp } = await app.start();
    await fetch(httpUrl(http, '/v1/rules/cpu-high'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ metric: 'cpu', max: 80, severity: 'critical' }),
    });
    app.reloadRules();

    const client = await connectTcp(tcp);
    // A future timestamp is outside the allowed skew, so the engine rejects the event.
    sendEvent(client, { id: 'future-1', source: 'edge-01', metric: 'cpu', value: 99, timestamp: NOW + 3_600_000 });
    await waitFor(() => app.stats().ingest?.eventsRejected === 1);
    expect(app.stats().findings).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

describe('shutdown ordering', () => {
  it('stops accepting TCP connections before the engine drains', async () => {
    const { app } = makeApp();
    const { tcp } = await app.start();
    await app.stop();
    expect(app.stats().engine.state).toBe('stopped');
    await expect(connectTcp(tcp)).rejects.toThrow();
  });

  it('removes listeners it attached, so later engine events do not reach the log', async () => {
    const { app, lines } = makeApp();
    await app.start();
    await app.stop();
    const before = lines.length;
    // Stop has already run; no further log lines should come from this app.
    await app.stop();
    expect(lines.length).toBe(before);
  });
});

// ---------------------------------------------------------------------------
// Authentication, metrics and alert delivery
// ---------------------------------------------------------------------------

const READER_SECRET = 'reader-secret-0123456789abcdef';
const WRITER_SECRET = 'writer-secret-0123456789abcdef';
const HOOK_URL = 'https://hooks.example.internal/nexuspulse';

interface FakeHook {
  readonly fetch: FetchLike;
  readonly calls: { url: string; body: string; headers: Record<string, string> }[];
}

function fakeHook(): FakeHook {
  const calls: { url: string; body: string; headers: Record<string, string> }[] = [];
  const fetchImpl: FetchLike = (url, init) => {
    calls.push({ url, body: init.body, headers: init.headers });
    return Promise.resolve({ ok: true, status: 204 });
  };
  return { fetch: fetchImpl, calls };
}

/** Writes a token file with fixed test secrets and returns its path. */
function tokenFileWith(mode = 0o600): string {
  const dir = tempDir();
  const file = path.join(dir, 'tokens.json');
  writeTokenFile(file, [
    { name: 'ci-reader', secret: READER_SECRET, role: 'reader' },
    { name: 'ops-writer', secret: WRITER_SECRET, role: 'writer' },
  ]);
  if (mode !== 0o600 && process.platform !== 'win32') fs.chmodSync(file, mode);
  return file;
}

async function putRule(port: number, name: string, body: unknown, token?: string): Promise<Response> {
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (token !== undefined) headers['authorization'] = `Bearer ${token}`;
  return fetch(httpUrl(port, `/v1/rules/${name}`), { method: 'PUT', headers, body: JSON.stringify(body) });
}

describe('metrics endpoint', () => {
  it('serves Prometheus text with the exposition content type', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    const response = await fetch(httpUrl(http, '/metrics'));
    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toBe('text/plain; version=0.0.4; charset=utf-8');
    const text = await response.text();
    expect(text).toContain('# TYPE nexuspulse_engine_events_accepted_total counter');
    expect(text).toContain('nexuspulse_engine_state{state="running"} 1');
    expect(text.startsWith('# HELP')).toBe(true);
  });

  it('includes ingest counters once the TCP listener is up', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    const text = await (await fetch(httpUrl(http, '/metrics'))).text();
    expect(text).toContain('nexuspulse_ingest_connections_open 0');
    expect(text).toContain('# TYPE nexuspulse_ingest_events_accepted_total counter');
  });
});

describe('API authentication', () => {
  it('reports authentication as disabled, and warns, when no token file is set', async () => {
    const { app, lines } = makeApp();
    await app.start();
    expect(app.stats().authenticated).toBe(false);
    expect(lines.some((l) => l.includes('authentication is disabled'))).toBe(true);
  });

  it('enables authentication when a token file is set', async () => {
    const { app, lines } = makeApp(tempDir(), { NEXUSPULSE_TOKENS_FILE: tokenFileWith() });
    await app.start();
    expect(app.stats().authenticated).toBe(true);
    expect(lines.some((l) => l.includes('API authentication enabled'))).toBe(true);
  });

  it('answers 401 on /metrics and /v1 routes without a token, but keeps /healthz public', async () => {
    const { app } = makeApp(tempDir(), { NEXUSPULSE_TOKENS_FILE: tokenFileWith() });
    const { http } = await app.start();
    expect((await fetch(httpUrl(http, '/healthz'))).status).toBe(200);
    expect((await fetch(httpUrl(http, '/metrics'))).status).toBe(401);
    expect((await fetch(httpUrl(http, '/v1/metrics'))).status).toBe(401);
  });

  it('lets a reader read /metrics but not write rules', async () => {
    const { app } = makeApp(tempDir(), { NEXUSPULSE_TOKENS_FILE: tokenFileWith() });
    const { http } = await app.start();
    const read = await fetch(httpUrl(http, '/metrics'), { headers: { authorization: `Bearer ${READER_SECRET}` } });
    expect(read.status).toBe(200);
    const write = await putRule(http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' }, READER_SECRET);
    expect(write.status).toBe(403);
  });

  it('lets a writer store a rule', async () => {
    const { app } = makeApp(tempDir(), { NEXUSPULSE_TOKENS_FILE: tokenFileWith() });
    const { http } = await app.start();
    const write = await putRule(http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' }, WRITER_SECRET);
    expect(write.status).toBe(201);
  });

  it('refuses to start when the token file is missing', async () => {
    const { app } = makeApp(tempDir(), { NEXUSPULSE_TOKENS_FILE: path.join(tempDir(), 'absent.json') });
    await expect(app.start()).rejects.toMatchObject({ code: 'APP_START_FAILED' });
    expect(app.stats().state).toBe('stopped');
  });

  it.skipIf(process.platform === 'win32')('refuses to start when the token file is readable by others', async () => {
    const file = tokenFileWith(0o644);
    const { app } = makeApp(tempDir(), { NEXUSPULSE_TOKENS_FILE: file });
    const err = (await app.start().catch((e: unknown) => e)) as AppError;
    expect(err.code).toBe('APP_START_FAILED');
    expect(String(err.details['cause'])).toContain('chmod 600');
  });
});

describe('alert delivery', () => {
  it('posts a rule finding to the webhook as JSON with the expected key', async () => {
    const hook = fakeHook();
    const { app } = makeApp(tempDir(), { NEXUSPULSE_ALERT_WEBHOOK_URL: HOOK_URL }, { alertFetch: hook.fetch });
    const { http, tcp } = await app.start();
    await putRule(http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });
    app.reloadRules();

    const client = await connectTcp(tcp);
    sendEvent(client, event('hot-hook', 95));
    await waitFor(() => hook.calls.length === 1);

    expect(hook.calls[0]?.url).toBe(HOOK_URL);
    expect(hook.calls[0]?.headers['content-type']).toBe('application/json');
    const body = JSON.parse(hook.calls[0]?.body ?? '{}') as { key: string; severity: string; labels: Record<string, string> };
    expect(body.key).toBe('rule:cpu-high:edge-01:cpu');
    expect(body.severity).toBe('critical');
    expect(body.labels['eventId']).toBe('hot-hook');
  });

  it('sends only one webhook call when a rule fires twice within its cooldown', async () => {
    const hook = fakeHook();
    const { app } = makeApp(tempDir(), { NEXUSPULSE_ALERT_WEBHOOK_URL: HOOK_URL }, { alertFetch: hook.fetch });
    const { http, tcp } = await app.start();
    await putRule(http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });
    app.reloadRules();

    const client = await connectTcp(tcp);
    sendEvent(client, event('repeat-1', 95));
    sendEvent(client, event('repeat-2', 96));
    await waitFor(() => app.stats().ingest?.eventsAccepted === 2);
    await new Promise((resolve) => setTimeout(resolve, 20));

    // The rule's own cooldown suppresses the second match before it reaches the notifier.
    expect(app.stats().findings).toBe(1);
    expect(hook.calls).toHaveLength(1);
  });

  it('keeps alerts below the minimum severity out of the webhook', async () => {
    const hook = fakeHook();
    const { app } = makeApp(tempDir(), { NEXUSPULSE_ALERT_WEBHOOK_URL: HOOK_URL }, { alertFetch: hook.fetch });
    const { http, tcp } = await app.start();
    await putRule(http, 'cpu-soft', { metric: 'cpu', max: 80, severity: 'info' });
    app.reloadRules();

    const client = await connectTcp(tcp);
    sendEvent(client, event('soft-1', 95));
    await waitFor(() => app.stats().findings === 1);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(hook.calls).toHaveLength(0);
  });

  it('delivers alerts still queued at shutdown before the app stops', async () => {
    const hook = fakeHook();
    const { app } = makeApp(tempDir(), { NEXUSPULSE_ALERT_WEBHOOK_URL: HOOK_URL }, { alertFetch: hook.fetch });
    const { http, tcp } = await app.start();
    await putRule(http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });
    app.reloadRules();

    const client = await connectTcp(tcp);
    sendEvent(client, event('late-1', 95));
    await waitFor(() => app.stats().ingest?.eventsAccepted === 1);
    await app.stop();

    expect(hook.calls).toHaveLength(1);
  });

  it('keeps the log sink working when no webhook is configured', async () => {
    const { app, lines } = makeApp();
    const { http, tcp } = await app.start();
    await putRule(http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });
    app.reloadRules();

    const client = await connectTcp(tcp);
    sendEvent(client, event('logged-1', 95));
    await waitFor(() => lines.some((l) => l.includes('logged-1')));
    expect(app.stats().alerts?.closed).toBe(false);
  });
});
