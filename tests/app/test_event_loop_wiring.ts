/**
 * The running app measures event-loop delay and publishes it on /metrics. The figures exist only
 * while the app is running.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { NexusPulseApp } from '../../src/app/server.js';
import { parseSettings, type Environment } from '../../src/app/settings.js';

const apps: NexusPulseApp[] = [];
const dirs: string[] = [];

function makeApp(): NexusPulseApp {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'nexuspulse-lag-'));
  dirs.push(dataDir);
  const env: Environment = {
    NEXUSPULSE_HTTP_PORT: '0',
    NEXUSPULSE_TCP_PORT: '0',
    NEXUSPULSE_DATA_DIR: dataDir,
    NEXUSPULSE_LOG_LEVEL: 'error',
  };
  const app = new NexusPulseApp({ settings: parseSettings(env), logSink: () => undefined });
  apps.push(app);
  return app;
}

afterEach(async () => {
  for (const app of apps.splice(0)) await app.stop();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe('event-loop monitoring in the running app', () => {
  it('reports no event-loop figures before start', () => {
    expect(makeApp().stats().eventLoop).toBeUndefined();
  });

  it('reports event-loop figures while running', async () => {
    const app = makeApp();
    await app.start();
    await new Promise((resolve) => setTimeout(resolve, 50));

    const lag = app.stats().eventLoop;
    expect(lag).toBeDefined();
    expect(lag?.samples).toBeGreaterThan(0);
  });

  it('publishes the lag gauges on /metrics', async () => {
    const app = makeApp();
    const { http } = await app.start();
    await new Promise((resolve) => setTimeout(resolve, 50));

    const response = await fetch(`http://127.0.0.1:${http}/metrics`);
    expect(response.status).toBe(200);
    const text = await response.text();
    expect(text).toContain('nexuspulse_event_loop_lag_p99_ms');
    expect(text).toContain('nexuspulse_event_loop_lag_max_ms');
    expect(text).toContain('nexuspulse_event_loop_samples_total');
  });

  it('stops measuring after stop', async () => {
    const app = makeApp();
    await app.start();
    await app.stop();
    expect(app.stats().eventLoop).toBeUndefined();
  });
});
