/**
 * Rule evaluation for events submitted over the HTTP API.
 *
 * The TCP ingest path has its own coverage in test_server.ts. These tests make
 * sure an event posted to /v1/events is checked against the stored rules in the
 * same way, for single events and for batches.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { AppError, NexusPulseApp, type AppOptions } from '../../src/app/server.js';
import { parseSettings, type Environment } from '../../src/app/settings.js';
import type { TelemetryEvent } from '../../src/core/engine.js';

const NOW = Date.UTC(2026, 9, 15, 12, 0, 0);
const clock = (): number => NOW;

const apps: NexusPulseApp[] = [];
const dirs: string[] = [];

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nexuspulse-http-rules-'));
  dirs.push(dir);
  return dir;
}

function envFor(dataDir: string): Environment {
  return {
    NEXUSPULSE_HTTP_PORT: '0',
    NEXUSPULSE_TCP_PORT: '0',
    NEXUSPULSE_DATA_DIR: dataDir,
    NEXUSPULSE_LOG_LEVEL: 'debug',
  };
}

interface Harness {
  readonly app: NexusPulseApp;
  readonly lines: string[];
}

function makeApp(extra: Partial<AppOptions> = {}): Harness {
  const dataDir = tempDir();
  const lines: string[] = [];
  const app = new NexusPulseApp({
    settings: parseSettings(envFor(dataDir)),
    logSink: (line) => lines.push(line),
    clock,
    ...extra,
  });
  apps.push(app);
  return { app, lines };
}

function event(id: string, value: number, metric = 'cpu', timestamp = NOW - 1_000): TelemetryEvent {
  return { id, source: 'edge-01', metric, value, timestamp };
}

function httpUrl(port: number, route: string): string {
  return `http://127.0.0.1:${port}${route}`;
}

async function waitFor(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

/** Stores a rule over HTTP and reloads so that it takes effect. */
async function storeRule(app: NexusPulseApp, port: number, name: string, body: Record<string, unknown>): Promise<void> {
  const response = await fetch(httpUrl(port, `/v1/rules/${name}`), {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!response.ok) throw new Error(`rule PUT failed with ${response.status}`);
  app.reloadRules();
}

async function postEvents(port: number, payload: unknown): Promise<Response> {
  return fetch(httpUrl(port, '/v1/events'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

afterEach(async () => {
  for (const app of apps.splice(0)) await app.stop();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe('rule evaluation for a single HTTP event', () => {
  it('logs a finding when a posted event breaks a maximum', async () => {
    const { app, lines } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });

    const response = await postEvents(http, event('hot-http-1', 95));
    expect(response.status).toBe(202);
    await waitFor(() => app.stats().findings === 1);

    const line = lines.find((l) => l.includes('alert'));
    expect(line).toBeDefined();
    expect(line).toContain('cpu-high');
    expect(line).toContain('hot-http-1');
  });

  it('logs a finding when a posted event breaks a minimum', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'disk-low', { metric: 'disk', min: 10, severity: 'warning' });

    await postEvents(http, event('low-http-1', 3, 'disk'));
    await waitFor(() => app.stats().findings === 1);
  });

  it('does not log a finding for a value inside the bounds', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'warning' });

    const response = await postEvents(http, event('cool-http-1', 20));
    expect(response.status).toBe(202);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(app.stats().findings).toBe(0);
  });

  it('ignores a rule whose metric does not match the event', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });

    await postEvents(http, event('mem-http-1', 99, 'memory'));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(app.stats().findings).toBe(0);
  });

  it('does not evaluate rules for an event the engine rejected', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });

    // A timestamp an hour in the future is outside the allowed skew, so the engine refuses it.
    const response = await postEvents(http, event('future-http-1', 99, 'cpu', NOW + 3_600_000));
    expect(response.status).not.toBe(202);
    expect(app.stats().findings).toBe(0);
  });
});

describe('rule evaluation for an HTTP batch', () => {
  it('evaluates every accepted event in the batch', async () => {
    const { app, lines } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });

    // Cooldown is per rule and per source, so the two hot events use different sources.
    const response = await postEvents(http, [
      event('batch-ok', 10),
      event('batch-hot-1', 90),
      { ...event('batch-hot-2', 97), source: 'edge-02' },
    ]);
    expect(response.status).toBe(202);
    await waitFor(() => app.stats().findings === 2);

    expect(lines.some((l) => l.includes('batch-hot-1'))).toBe(true);
    expect(lines.some((l) => l.includes('batch-hot-2'))).toBe(true);
    expect(lines.some((l) => l.includes('batch-ok'))).toBe(false);
  });

  it('applies the rule cooldown to repeats from one source inside a batch', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });

    // Both events come from edge-01 within the default 30 s cooldown, so only the first alerts.
    await postEvents(http, [event('repeat-1', 90), event('repeat-2', 95)]);
    await waitFor(() => app.stats().findings === 1);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(app.stats().findings).toBe(1);
  });

  it('reports a finding only for the events the engine accepted in a mixed batch', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'critical' });

    const response = await postEvents(http, [
      event('mixed-hot', 99),
      event('mixed-future', 99, 'cpu', NOW + 3_600_000),
    ]);
    expect(response.status).toBe(202);
    await waitFor(() => app.stats().findings === 1);
    expect(app.stats().ingest?.eventsAccepted ?? 0).toBe(0);
  });

  it('shows a finding from a batch in the rule statistics without a rule reload', async () => {
    const { app } = makeApp();
    const { http } = await app.start();
    await storeRule(app, http, 'cpu-high', { metric: 'cpu', max: 80, severity: 'warning' });

    await postEvents(http, [event('stat-hot', 85)]);
    await waitFor(() => app.stats().findings === 1);
    expect(app.stats().rules.rules).toBe(1);
  });
});

describe('the HTTP evaluation path', () => {
  it('keeps the rule set empty until a rule is stored and reloaded', async () => {
    const { app } = makeApp();
    const { http } = await app.start();

    // Stored but not reloaded: the running rule set does not see it yet.
    const put = await fetch(httpUrl(http, '/v1/rules/cpu-high'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ metric: 'cpu', max: 80, severity: 'critical' }),
    });
    expect(put.ok).toBe(true);

    await postEvents(http, event('pre-reload', 99));
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(app.stats().findings).toBe(0);
  });
});

describe('start-up guard', () => {
  it('refuses to reload rules before start()', () => {
    const { app } = makeApp();
    expect(() => app.reloadRules()).toThrow(AppError);
  });
});
