/**
 * @file tests/api/test_telemetry_api.ts
 * @description Vitest suite for /src/api/telemetry_api.ts.
 *
 * Runs the real router over a loopback HTTP server, backed by a real
 * StreamEngine (fixed clock, so timestamps are deterministic) and a real
 * RecordStore on a temporary file.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import { createDefaultEngine, type StreamEngine } from '../../src/core/engine.js';
import { RecordStore } from '../../src/db/record_store.js';
import { createApiServer, type Router } from '../../src/api/http_router.js';
import {
  createTelemetryApi,
  RULE_SCHEMA,
  DEFAULT_MAX_BATCH,
  MAX_BATCH_LIMIT,
  type RuleDefinition,
} from '../../src/api/telemetry_api.js';

const NOW = 1_700_000_000_000;
const clock = (): number => NOW;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface Reply {
  readonly status: number;
  readonly headers: http.IncomingHttpHeaders;
  readonly body: string;
}

const servers: http.Server[] = [];
const engines: StreamEngine[] = [];
const stores: RecordStore<RuleDefinition>[] = [];
let dir: string;

async function start(router: Router): Promise<number> {
  const server = createApiServer(router);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return (server.address() as AddressInfo).port;
}

function send(port: number, method: string, pathAndQuery: string, body?: unknown, contentType = 'application/json'): Promise<Reply> {
  const payload = body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body);
  return new Promise<Reply>((resolve, reject) => {
    const headers: Record<string, string> = {};
    if (payload !== undefined) {
      headers['content-type'] = contentType;
      headers['content-length'] = String(Buffer.byteLength(payload));
    }
    const req = http.request(
      { host: '127.0.0.1', port, method, path: pathAndQuery, headers, agent: false },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () =>
          resolve({ status: res.statusCode ?? 0, headers: res.headers, body: Buffer.concat(chunks).toString('utf8') }),
        );
        res.on('error', reject);
      },
    );
    req.on('error', reject);
    if (payload !== undefined) req.write(payload);
    req.end();
  });
}

const parse = (reply: Reply): Record<string, unknown> => JSON.parse(reply.body) as Record<string, unknown>;
const errorOf = (reply: Reply): { code: string; details?: Record<string, unknown> } =>
  (parse(reply)['error'] as { code: string; details?: Record<string, unknown> });

function event(id: string, value = 10, timestamp = NOW - 1_000, source = 'edge-01', metric = 'cpu'): Record<string, unknown> {
  return { id, source, metric, value, timestamp };
}

interface Setup {
  readonly port: number;
  readonly engine: StreamEngine;
  readonly store?: RecordStore<RuleDefinition>;
}

/** Builds an engine, an optional rule store, and the API, then starts the server. */
async function setup(
  opts: { engineConfig?: Parameters<typeof createDefaultEngine>[0]; withRules?: boolean; maxBatch?: number } = {},
): Promise<Setup> {
  const engine = createDefaultEngine({ clock, ...(opts.engineConfig ?? {}) });
  engines.push(engine);
  let store: RecordStore<RuleDefinition> | undefined;
  if (opts.withRules === true) {
    store = RecordStore.open<RuleDefinition>({ file: path.join(dir, 'rules.npdb'), schema: RULE_SCHEMA });
    stores.push(store);
  }
  const router = createTelemetryApi({
    engine,
    ...(store === undefined ? {} : { rules: store }),
    ...(opts.maxBatch === undefined ? {} : { maxBatch: opts.maxBatch }),
  });
  const port = await start(router);
  return store === undefined ? { port, engine } : { port, engine, store };
}

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'np-api-'));
});

afterEach(async () => {
  for (const engine of engines.splice(0)) {
    if (engine.metrics().state !== 'stopped') engine.stop();
  }
  for (const store of stores.splice(0)) store.close();
  await Promise.all(
    servers.splice(0).map(
      (s) =>
        new Promise<void>((resolve) => {
          s.closeAllConnections();
          s.close(() => resolve());
        }),
    ),
  );
  fs.rmSync(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

describe('construction', () => {
  it.each([0, -1, 1.5, MAX_BATCH_LIMIT + 1])('rejects maxBatch %s', (maxBatch) => {
    const engine = createDefaultEngine({ clock });
    engines.push(engine);
    expect(() => createTelemetryApi({ engine, maxBatch })).toThrow(RangeError);
  });

  it('exposes the documented defaults', () => {
    expect(DEFAULT_MAX_BATCH).toBe(500);
    expect(MAX_BATCH_LIMIT).toBe(10_000);
  });

  it('the rule schema rejects unknown keys and bad severities', async () => {
    const { validate } = await import('../../src/db/schema.js');
    expect(validate(RULE_SCHEMA, { metric: 'cpu', severity: 'warning' }).valid).toBe(true);
    expect(validate(RULE_SCHEMA, { metric: 'cpu', severity: 'loud' }).valid).toBe(false);
    expect(validate(RULE_SCHEMA, { metric: 'cpu', severity: 'info', extra: 1 }).valid).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Health and metrics
// ---------------------------------------------------------------------------

describe('health and metrics', () => {
  it('reports ok while the engine is idle', async () => {
    const { port } = await setup();
    const reply = await send(port, 'GET', '/healthz');
    expect(reply.status).toBe(200);
    expect(parse(reply)).toEqual({ status: 'ok', state: 'idle' });
  });

  it('reports 503 once the engine is stopped', async () => {
    const { port, engine } = await setup();
    engine.stop();
    const reply = await send(port, 'GET', '/healthz');
    expect(reply.status).toBe(503);
    expect(parse(reply)).toEqual({ status: 'unavailable', state: 'stopped' });
  });

  it('returns the engine metrics object', async () => {
    const { port } = await setup();
    await send(port, 'POST', '/v1/events', event('m1'));
    const reply = await send(port, 'GET', '/v1/metrics');
    expect(reply.status).toBe(200);
    expect(parse(reply)).toMatchObject({ state: 'idle', submitted: 1, accepted: 1 });
  });
});

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------

describe('POST /v1/events: single event', () => {
  it('accepts a valid event with 202', async () => {
    const { port, engine } = await setup();
    const reply = await send(port, 'POST', '/v1/events', event('s1'));
    expect(reply.status).toBe(202);
    expect(parse(reply)).toEqual({ mode: 'single', accepted: true });
    expect(engine.metrics().accepted).toBe(1);
  });

  it('answers 422 EVENT_INVALID for an event that fails validation', async () => {
    const { port, engine } = await setup();
    const reply = await send(port, 'POST', '/v1/events', { ...event('s2'), value: 'not-a-number' });
    expect(reply.status).toBe(422);
    expect(errorOf(reply).code).toBe('EVENT_INVALID');
    expect(errorOf(reply).details).toEqual({ reason: 'invalid' });
    expect(engine.metrics().rejectedInvalid).toBe(1);
  });

  it('answers 400 when the body is missing', async () => {
    const { port } = await setup();
    const reply = await send(port, 'POST', '/v1/events');
    expect(reply.status).toBe(400);
    expect(errorOf(reply).code).toBe('BODY_REQUIRED');
  });

  it('answers 415 for a non-JSON body', async () => {
    const { port } = await setup();
    const reply = await send(port, 'POST', '/v1/events', 'id=1', 'text/plain');
    expect(reply.status).toBe(415);
  });

  it('answers 503 ENGINE_STOPPED once the engine is stopped', async () => {
    const { port, engine } = await setup();
    engine.stop();
    const reply = await send(port, 'POST', '/v1/events', event('s3'));
    expect(reply.status).toBe(503);
    expect(errorOf(reply).code).toBe('ENGINE_STOPPED');
  });

  it('answers 503 QUEUE_FULL with Retry-After under the reject policy', async () => {
    const { port } = await setup({ engineConfig: { queueCapacity: 1, backpressure: 'reject' } });
    expect((await send(port, 'POST', '/v1/events', event('q1'))).status).toBe(202);
    const reply = await send(port, 'POST', '/v1/events', event('q2'));
    expect(reply.status).toBe(503);
    expect(errorOf(reply).code).toBe('QUEUE_FULL');
    expect(reply.headers['retry-after']).toBe('1');
  });

  it('answers 503 QUEUE_FULL under the drop-newest policy', async () => {
    const { port } = await setup({ engineConfig: { queueCapacity: 1, backpressure: 'drop-newest' } });
    await send(port, 'POST', '/v1/events', event('d1'));
    const reply = await send(port, 'POST', '/v1/events', event('d2'));
    expect(reply.status).toBe(503);
    expect(reply.headers['retry-after']).toBe('1');
  });
});

describe('POST /v1/events: batch', () => {
  it('reports per-event outcomes for a mixed batch', async () => {
    const { port } = await setup();
    const reply = await send(port, 'POST', '/v1/events', [
      event('b1'),
      { ...event('b2'), value: 'bad' },
      event('b3'),
    ]);
    expect(reply.status).toBe(202);
    expect(parse(reply)).toMatchObject({
      mode: 'batch',
      total: 3,
      accepted: 2,
      rejected: 1,
      rejectedByReason: { invalid: 1 },
    });
  });

  it('tallies queue-full rejections instead of failing the whole batch', async () => {
    const { port } = await setup({ engineConfig: { queueCapacity: 2, backpressure: 'reject' } });
    const reply = await send(port, 'POST', '/v1/events', [event('t1'), event('t2'), event('t3'), event('t4')]);
    expect(reply.status).toBe(202);
    expect(parse(reply)).toMatchObject({ total: 4, accepted: 2, rejectedByReason: { 'queue-full': 2 } });
  });

  it('rejects an empty batch with 400 EMPTY_BATCH', async () => {
    const { port } = await setup();
    const reply = await send(port, 'POST', '/v1/events', []);
    expect(reply.status).toBe(400);
    expect(errorOf(reply).code).toBe('EMPTY_BATCH');
  });

  it('rejects a batch above maxBatch with 413 and the limit in details', async () => {
    const { port } = await setup({ maxBatch: 2 });
    const reply = await send(port, 'POST', '/v1/events', [event('x1'), event('x2'), event('x3')]);
    expect(reply.status).toBe(413);
    expect(errorOf(reply)).toMatchObject({ code: 'BATCH_TOO_LARGE', details: { limit: 2 } });
  });

  it('accepts a batch of exactly maxBatch events', async () => {
    const { port } = await setup({ maxBatch: 2 });
    const reply = await send(port, 'POST', '/v1/events', [event('y1'), event('y2')]);
    expect(reply.status).toBe(202);
  });
});

// ---------------------------------------------------------------------------
// Series snapshots
// ---------------------------------------------------------------------------

describe('GET /v1/series', () => {
  it('returns the snapshot of a processed series', async () => {
    const { port, engine } = await setup();
    await send(port, 'POST', '/v1/events', [event('p1', 1), event('p2', 2), event('p3', 3)]);
    engine.flush();
    const reply = await send(port, 'GET', '/v1/series?source=edge-01&metric=cpu');
    expect(reply.status).toBe(200);
    expect(parse(reply)).toMatchObject({ source: 'edge-01', metric: 'cpu', processed: 3 });
  });

  it('answers 404 for a series that has not been processed', async () => {
    const { port } = await setup();
    const reply = await send(port, 'GET', '/v1/series?source=edge-01&metric=cpu');
    expect(reply.status).toBe(404);
    expect(errorOf(reply).code).toBe('SERIES_NOT_FOUND');
  });

  it('answers 400 when a query parameter is missing or empty', async () => {
    const { port } = await setup();
    expect(errorOf(await send(port, 'GET', '/v1/series?source=edge-01')).code).toBe('MISSING_QUERY');
    expect(errorOf(await send(port, 'GET', '/v1/series?source=&metric=cpu')).code).toBe('MISSING_QUERY');
  });

  it('answers 400 when a query parameter exceeds 256 characters', async () => {
    const { port } = await setup();
    const long = 'a'.repeat(257);
    const reply = await send(port, 'GET', `/v1/series?source=${long}&metric=cpu`);
    expect(reply.status).toBe(400);
    expect(errorOf(reply).code).toBe('QUERY_TOO_LONG');
  });

  it('decodes percent-encoded query values', async () => {
    const { port, engine } = await setup();
    await send(port, 'POST', '/v1/events', event('e1', 1, NOW - 1_000, 'rack-a', 'mem.used'));
    engine.flush();
    const reply = await send(port, 'GET', '/v1/series?source=rack-a&metric=mem%2Eused');
    expect(reply.status).toBe(200);
    expect(parse(reply)).toMatchObject({ metric: 'mem.used' });
  });
});

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

describe('rules without a store', () => {
  it('answers 503 RULES_UNAVAILABLE on every rule route', async () => {
    const { port } = await setup();
    expect(errorOf(await send(port, 'GET', '/v1/rules')).code).toBe('RULES_UNAVAILABLE');
    expect(errorOf(await send(port, 'GET', '/v1/rules/high-cpu')).code).toBe('RULES_UNAVAILABLE');
    expect(errorOf(await send(port, 'PUT', '/v1/rules/high-cpu', { metric: 'cpu', severity: 'info' })).code).toBe(
      'RULES_UNAVAILABLE',
    );
    expect(errorOf(await send(port, 'DELETE', '/v1/rules/high-cpu')).code).toBe('RULES_UNAVAILABLE');
  });
});

describe('rules with a store', () => {
  const rule: RuleDefinition = { metric: 'cpu', max: 90, severity: 'critical' };

  it('creates a rule with 201, then replaces it with 200', async () => {
    const { port } = await setup({ withRules: true });
    expect((await send(port, 'PUT', '/v1/rules/high-cpu', rule)).status).toBe(201);
    const replaced = await send(port, 'PUT', '/v1/rules/high-cpu', { ...rule, max: 80 });
    expect(replaced.status).toBe(200);
    expect(parse(replaced)).toEqual({ ...rule, max: 80 });
  });

  it('reads a single rule and lists all rules', async () => {
    const { port } = await setup({ withRules: true });
    await send(port, 'PUT', '/v1/rules/high-cpu', rule);
    await send(port, 'PUT', '/v1/rules/low-mem', { metric: 'mem', min: 5, severity: 'warning' });

    const one = await send(port, 'GET', '/v1/rules/high-cpu');
    expect(parse(one)).toEqual(rule);

    const all = await send(port, 'GET', '/v1/rules');
    expect(parse(all)).toEqual({
      rules: {
        'high-cpu': rule,
        'low-mem': { metric: 'mem', min: 5, severity: 'warning' },
      },
    });
  });

  it('deletes a rule with 204, after which it is gone', async () => {
    const { port } = await setup({ withRules: true });
    await send(port, 'PUT', '/v1/rules/high-cpu', rule);
    const deleted = await send(port, 'DELETE', '/v1/rules/high-cpu');
    expect(deleted.status).toBe(204);
    expect((await send(port, 'GET', '/v1/rules/high-cpu')).status).toBe(404);
  });

  it('answers 404 when reading or deleting a rule that does not exist', async () => {
    const { port } = await setup({ withRules: true });
    expect(errorOf(await send(port, 'GET', '/v1/rules/ghost')).code).toBe('RULE_NOT_FOUND');
    expect(errorOf(await send(port, 'DELETE', '/v1/rules/ghost')).code).toBe('RULE_NOT_FOUND');
  });

  it.each(['Bad_Name', '-leading', 'UPPER', 'x'.repeat(64)])('rejects the rule name %s with 400', async (name) => {
    const { port } = await setup({ withRules: true });
    const reply = await send(port, 'PUT', `/v1/rules/${name}`, rule);
    expect(reply.status).toBe(400);
    expect(errorOf(reply).code).toBe('INVALID_RULE_NAME');
  });

  it('rejects a rule that fails the schema with 422 and the issues', async () => {
    const { port } = await setup({ withRules: true });
    const reply = await send(port, 'PUT', '/v1/rules/bad', { metric: 'cpu', severity: 'loud' });
    expect(reply.status).toBe(422);
    expect(errorOf(reply).code).toBe('RULE_INVALID');
    const issues = errorOf(reply).details?.['issues'] as { path: string }[];
    expect(issues.map((i) => i.path)).toEqual(['/severity']);
  });

  it('answers 422 for a null body instead of failing inside the handler', async () => {
    const { port } = await setup({ withRules: true });
    const reply = await send(port, 'PUT', '/v1/rules/nul', null);
    expect(reply.status).toBe(422);
    expect(errorOf(reply).code).toBe('RULE_INVALID');
  });

  it('rejects inverted bounds with 422 RULE_BOUNDS_INVERTED', async () => {
    const { port } = await setup({ withRules: true });
    const reply = await send(port, 'PUT', '/v1/rules/inv', { metric: 'cpu', min: 10, max: 1, severity: 'info' });
    expect(reply.status).toBe(422);
    expect(errorOf(reply).code).toBe('RULE_BOUNDS_INVERTED');
  });

  it('requires a body on PUT', async () => {
    const { port } = await setup({ withRules: true });
    const reply = await send(port, 'PUT', '/v1/rules/none');
    expect(reply.status).toBe(400);
    expect(errorOf(reply).code).toBe('BODY_REQUIRED');
  });

  it('keeps rules across a store reopen', async () => {
    const { port, store } = await setup({ withRules: true });
    await send(port, 'PUT', '/v1/rules/durable', rule);
    store?.close();
    stores.length = 0;
    const reopened = RecordStore.open<RuleDefinition>({ file: path.join(dir, 'rules.npdb'), schema: RULE_SCHEMA });
    stores.push(reopened);
    expect(reopened.get('durable')).toEqual(rule);
  });
});

// ---------------------------------------------------------------------------
// Routing as seen through the API
// ---------------------------------------------------------------------------

describe('routing', () => {
  it('answers 404 for an unknown path', async () => {
    const { port } = await setup();
    expect((await send(port, 'GET', '/v2/nothing')).status).toBe(404);
  });

  it('answers 405 with Allow for a known path and the wrong method', async () => {
    const { port } = await setup();
    const reply = await send(port, 'DELETE', '/v1/events');
    expect(reply.status).toBe(405);
    expect(reply.headers['allow']).toBe('OPTIONS, POST');
  });
});
