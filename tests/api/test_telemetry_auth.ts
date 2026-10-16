/**
 * HTTP-level tests for route guards on the telemetry API, and for raw text responses.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import { createDefaultEngine, type StreamEngine } from '../../src/core/engine.js';
import { createApiServer, Router } from '../../src/api/http_router.js';
import { createTelemetryApi } from '../../src/api/telemetry_api.js';
import { TokenAuth } from '../../src/api/auth.js';

const NOW = Date.UTC(2026, 9, 15, 12, 0, 0);
const clock = (): number => NOW;

const READER = 'reader-secret-0123456789abcdef';
const WRITER = 'writer-secret-0123456789abcdef';

interface Reply {
  readonly status: number;
  readonly headers: http.IncomingHttpHeaders;
  readonly body: string;
}

let server: http.Server | undefined;
let engine: StreamEngine | undefined;

function start(router: Router): Promise<number> {
  server = createApiServer(router);
  return new Promise((resolve, reject) => {
    server?.once('error', reject);
    server?.listen(0, '127.0.0.1', () => {
      resolve((server?.address() as AddressInfo).port);
    });
  });
}

function request(
  port: number,
  method: string,
  route: string,
  options: { token?: string; authorization?: string; body?: unknown } = {},
): Promise<Reply> {
  const payload = options.body === undefined ? undefined : JSON.stringify(options.body);
  const headers: Record<string, string> = {};
  if (options.authorization !== undefined) headers['authorization'] = options.authorization;
  if (options.token !== undefined) headers['authorization'] = `Bearer ${options.token}`;
  if (payload !== undefined) {
    headers['content-type'] = 'application/json';
    headers['content-length'] = String(Buffer.byteLength(payload));
  }
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method, path: route, headers }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => {
        resolve({ status: res.statusCode ?? 0, headers: res.headers, body: Buffer.concat(chunks).toString('utf8') });
      });
    });
    req.on('error', reject);
    if (payload !== undefined) req.write(payload);
    req.end();
  });
}

function event(id: string): Record<string, unknown> {
  return { id, source: 'edge-01', metric: 'cpu', value: 10, timestamp: NOW - 1_000 };
}

function guardedRouter(): Router {
  engine = createDefaultEngine({ clock });
  const auth = new TokenAuth([
    { name: 'ci-reader', secret: READER, role: 'reader' },
    { name: 'ops-writer', secret: WRITER, role: 'writer' },
  ]);
  return createTelemetryApi({ engine, auth });
}

afterEach(async () => {
  if (server !== undefined) {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server?.close(() => resolve()));
    server = undefined;
  }
  if (engine !== undefined) {
    if (engine.metrics().state !== 'stopped') engine.stop();
    engine = undefined;
  }
});

describe('guarded telemetry routes', () => {
  let port = 0;

  beforeEach(async () => {
    port = await start(guardedRouter());
  });

  it('keeps /healthz public', async () => {
    const reply = await request(port, 'GET', '/healthz');
    expect(reply.status).toBe(200);
  });

  it('answers 401 with a challenge for a GET without a token', async () => {
    const reply = await request(port, 'GET', '/v1/metrics');
    expect(reply.status).toBe(401);
    expect(reply.headers['www-authenticate']).toBe('Bearer realm="nexuspulse"');
    expect(JSON.parse(reply.body)).toMatchObject({ error: { code: 'UNAUTHORIZED' } });
  });

  it('answers 401 for an unknown token', async () => {
    const reply = await request(port, 'GET', '/v1/metrics', { token: 'not-a-real-token-0123456789' });
    expect(reply.status).toBe(401);
  });

  it('answers 401 for a non-bearer authorization header', async () => {
    const reply = await request(port, 'GET', '/v1/metrics', { authorization: 'Basic dXNlcjpwYXNz' });
    expect(reply.status).toBe(401);
  });

  it('lets a reader read metrics', async () => {
    const reply = await request(port, 'GET', '/v1/metrics', { token: READER });
    expect(reply.status).toBe(200);
    expect(JSON.parse(reply.body)).toMatchObject({ state: 'idle' });
  });

  it('answers 403 when a reader tries to submit an event', async () => {
    const reply = await request(port, 'POST', '/v1/events', { token: READER, body: event('r-1') });
    expect(reply.status).toBe(403);
    expect(JSON.parse(reply.body)).toMatchObject({ error: { code: 'FORBIDDEN' } });
  });

  it('lets a writer submit an event', async () => {
    const reply = await request(port, 'POST', '/v1/events', { token: WRITER, body: event('w-1') });
    expect(reply.status).toBe(202);
  });

  it('lets a writer through a reader route', async () => {
    const reply = await request(port, 'GET', '/v1/series?source=edge-01&metric=cpu', { token: WRITER });
    // No series exists yet because the engine never processed an event. A 404 here means the guard let the writer through.
    expect(reply.status).toBe(404);
  });

  it('answers 403 when a reader tries to delete a rule', async () => {
    const reply = await request(port, 'DELETE', '/v1/rules/cpu-high', { token: READER });
    // The rule store is not configured here, so a writer would see 503. A reader must see 403 first.
    expect(reply.status).toBe(403);
  });

  it('does not run the handler for an unauthenticated write', async () => {
    const before = engine?.metrics().submitted ?? 0;
    await request(port, 'POST', '/v1/events', { body: event('anon-1') });
    expect(engine?.metrics().submitted ?? 0).toBe(before);
  });

  it('rejects a bad token on a write without accepting the event', async () => {
    const before = engine?.metrics().submitted ?? 0;
    const reply = await request(port, 'POST', '/v1/events', { token: 'wrong-token-0123456789abcdef', body: event('bad-1') });
    expect(reply.status).toBe(401);
    expect(engine?.metrics().submitted ?? 0).toBe(before);
  });
});

describe('telemetry routes without authentication', () => {
  it('leaves every route open when no auth is configured', async () => {
    engine = createDefaultEngine({ clock });
    const port = await start(createTelemetryApi({ engine }));
    const read = await request(port, 'GET', '/v1/metrics');
    const write = await request(port, 'POST', '/v1/events', { body: event('open-1') });
    expect(read.status).toBe(200);
    expect(write.status).toBe(202);
  });
});

describe('raw text responses', () => {
  it('sends raw text without JSON quoting and defaults to text/plain', async () => {
    const router = new Router().get('/text', () => ({ status: 200, raw: 'metric 1\n' }));
    const port = await start(router);
    const reply = await request(port, 'GET', '/text');
    expect(reply.body).toBe('metric 1\n');
    expect(reply.headers['content-type']).toBe('text/plain; charset=utf-8');
    expect(reply.headers['content-length']).toBe(String(Buffer.byteLength('metric 1\n')));
  });

  it('keeps an explicit content type from the handler', async () => {
    const router = new Router().get('/text', () => ({
      status: 200,
      raw: '# TYPE x gauge\n',
      headers: { 'content-type': 'text/plain; version=0.0.4; charset=utf-8' },
    }));
    const port = await start(router);
    const reply = await request(port, 'GET', '/text');
    expect(reply.headers['content-type']).toBe('text/plain; version=0.0.4; charset=utf-8');
  });

  it('takes precedence over a body in the same response', async () => {
    const router = new Router().get('/text', () => ({ status: 200, body: { ignored: true }, raw: 'raw wins' }));
    const port = await start(router);
    const reply = await request(port, 'GET', '/text');
    expect(reply.body).toBe('raw wins');
  });

  it('still JSON-encodes a body when raw is absent', async () => {
    const router = new Router().get('/json', () => ({ status: 200, body: { ok: true } }));
    const port = await start(router);
    const reply = await request(port, 'GET', '/json');
    expect(reply.headers['content-type']).toBe('application/json; charset=utf-8');
    expect(JSON.parse(reply.body)).toEqual({ ok: true });
  });
});
