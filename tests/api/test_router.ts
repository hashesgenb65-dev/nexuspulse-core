/**
 * @file tests/api/test_router.ts
 * @description Vitest suite for /src/api/http_router.ts.
 *
 * Each test starts a real node:http server on 127.0.0.1 with an ephemeral port
 * and talks to it with node:http directly, so arbitrary methods and raw request
 * paths reach the router unchanged.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  Router,
  ApiError,
  RouteDefinitionError,
  createApiServer,
  DEFAULT_MAX_BODY_BYTES,
  type RequestContext,
} from '../../src/api/http_router.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface Reply {
  readonly status: number;
  readonly headers: http.IncomingHttpHeaders;
  readonly body: string;
}

const servers: http.Server[] = [];

/** Starts a server for the router and returns its port. */
async function start(router: Router): Promise<number> {
  const server = createApiServer(router);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return (server.address() as AddressInfo).port;
}

interface SendOptions {
  readonly body?: string;
  readonly headers?: Record<string, string>;
}

/** Sends a raw request and collects the full response. */
function send(port: number, method: string, path: string, options: SendOptions = {}): Promise<Reply> {
  return new Promise<Reply>((resolve, reject) => {
    const headers: Record<string, string> = { ...(options.headers ?? {}) };
    if (options.body !== undefined) headers['content-length'] = String(Buffer.byteLength(options.body));
    const req = http.request(
      { host: '127.0.0.1', port, method, path, headers, agent: false },
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
    if (options.body !== undefined) req.write(options.body);
    req.end();
  });
}

const json = (body: string): unknown => JSON.parse(body) as unknown;

/**
 * Drives router.handle with a stub request and response, for cases the HTTP
 * parser never lets through (such as an unknown method token).
 */
async function handleWithStubs(router: Router, method: string, url: string): Promise<{ status: number; body: string }> {
  const req = Object.assign(new (await import('node:stream')).Readable({ read() {} }), {
    method,
    url,
    headers: {} as http.IncomingHttpHeaders,
  }) as unknown as http.IncomingMessage;
  req.push(null);
  const captured = { status: 0, body: '' };
  const headers: Record<string, string> = {};
  let sent = false;
  const res = {
    get headersSent(): boolean {
      return sent;
    },
    statusCode: 200,
    setHeader(name: string, value: string) {
      headers[name.toLowerCase()] = value;
    },
    writeHead(status: number, extra: Record<string, string> = {}) {
      captured.status = status;
      Object.assign(headers, extra);
      sent = true;
    },
    end(payload?: string) {
      captured.body = payload ?? '';
    },
    destroy() {
      /* not expected in these tests */
    },
  } as unknown as http.ServerResponse;
  await router.handle(req, res);
  return captured;
}

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (s) =>
        new Promise<void>((resolve) => {
          s.closeAllConnections();
          s.close(() => resolve());
        }),
    ),
  );
});

// ---------------------------------------------------------------------------
// Route definition
// ---------------------------------------------------------------------------

describe('route definition', () => {
  const noop = (): void => undefined;

  it.each<[string, string]>([
    ['pattern without leading slash', 'users'],
    ['pattern with trailing slash', '/users/'],
    ['empty segment', '/users//x'],
    ['parameter with invalid name', '/users/:1bad'],
    ['parameter with a dash', '/users/:user-id'],
    ['duplicate parameter names', '/a/:id/b/:id'],
    ['segment mixing literal and parameter', '/a/x:id'],
  ])('rejects %s', (_label, pattern) => {
    expect(() => new Router().get(pattern, noop)).toThrow(RouteDefinitionError);
  });

  it('rejects an unsupported method', () => {
    expect(() => new Router().add('BREW' as never, '/x', noop)).toThrow(/unsupported method BREW/);
  });

  it('rejects a non-function handler', () => {
    expect(() => new Router().get('/x', 'nope' as never)).toThrow(/handler must be a function/);
  });

  it('rejects a duplicate method and pattern pair', () => {
    const router = new Router().get('/x', noop);
    expect(() => router.get('/x', noop)).toThrow(/duplicate route GET \/x/);
  });

  it('allows the same pattern under a different method', () => {
    expect(() => new Router().get('/x', noop).post('/x', noop)).not.toThrow();
  });

  it('rejects an invalid body limit', () => {
    expect(() => new Router({ maxBodyBytes: 0 })).toThrow(/maxBodyBytes must be a positive integer/);
    expect(() => new Router({ maxBodyBytes: 1.5 })).toThrow(RouteDefinitionError);
  });

  it('describe lists routes in registration order', () => {
    const router = new Router()
      .get('/a', noop)
      .post('/a', noop)
      .delete('/a/:id', noop);
    expect(router.describe()).toEqual([
      { method: 'GET', pattern: '/a' },
      { method: 'POST', pattern: '/a' },
      { method: 'DELETE', pattern: '/a/:id' },
    ]);
  });

  it('exposes a default body limit of 1 MiB', () => {
    expect(DEFAULT_MAX_BODY_BYTES).toBe(1024 * 1024);
  });

  it('registers every convenience method', () => {
    const router = new Router()
      .get('/r', noop)
      .post('/r', noop)
      .put('/r', noop)
      .patch('/r', noop)
      .delete('/r', noop);
    expect(router.describe().map((r) => r.method)).toEqual(['GET', 'POST', 'PUT', 'PATCH', 'DELETE']);
  });
});

// ---------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------

describe('path matching', () => {
  it('matches literal paths and passes an empty param map', async () => {
    const router = new Router().get('/health', () => ({ status: 200, body: { ok: true } }));
    const port = await start(router);
    const reply = await send(port, 'GET', '/health');
    expect(reply.status).toBe(200);
    expect(json(reply.body)).toEqual({ ok: true });
  });

  it('captures and decodes a parameter', async () => {
    const router = new Router().get('/streams/:id', (ctx) => ({ status: 200, body: { id: ctx.params['id'] } }));
    const port = await start(router);
    const reply = await send(port, 'GET', '/streams/a%20b%2Fc');
    expect(json(reply.body)).toEqual({ id: 'a b/c' });
  });

  it('answers 400 for malformed percent-encoding in a parameter', async () => {
    const router = new Router().get('/x/:id', () => ({ status: 200 }));
    const port = await start(router);
    const reply = await send(port, 'GET', '/x/%E0%A4%A');
    expect(reply.status).toBe(400);
    expect((json(reply.body) as { error: { code: string } }).error.code).toBe('BAD_PATH_ENCODING');
  });

  it('ignores one trailing slash', async () => {
    const router = new Router().get('/users', () => ({ status: 200, body: 'list' }));
    const port = await start(router);
    expect((await send(port, 'GET', '/users/')).status).toBe(200);
  });

  it('matches the root path', async () => {
    const router = new Router().get('/', () => ({ status: 200, body: 'root' }));
    const port = await start(router);
    expect(json((await send(port, 'GET', '/')).body)).toBe('root');
  });

  it('matches literal segments case-sensitively', async () => {
    const router = new Router().get('/Users', () => ({ status: 200 }));
    const port = await start(router);
    expect((await send(port, 'GET', '/users')).status).toBe(404);
  });

  it('does not match a parameter against an empty segment', async () => {
    const router = new Router().get('/users/:id', () => ({ status: 200 }));
    const port = await start(router);
    expect((await send(port, 'GET', '/users')).status).toBe(404);
  });

  it('prefers a literal segment over a parameter at the same position', async () => {
    const router = new Router()
      .get('/items/:id', () => ({ status: 200, body: 'param' }))
      .get('/items/new', () => ({ status: 200, body: 'literal' }));
    const port = await start(router);
    expect(json((await send(port, 'GET', '/items/new')).body)).toBe('literal');
    expect(json((await send(port, 'GET', '/items/7')).body)).toBe('param');
  });

  it('matches the first registered route when two candidates have equal specificity', async () => {
    const router = new Router()
      .get('/p/:a', () => ({ status: 200, body: 'first' }))
      .get('/p/:b', () => ({ status: 200, body: 'second' }));
    const port = await start(router);
    expect(json((await send(port, 'GET', '/p/x')).body)).toBe('first');
  });

  it('exposes the query string through the context', async () => {
    const router = new Router().get('/search', (ctx: RequestContext) => ({
      status: 200,
      body: { q: ctx.query.get('q'), page: ctx.query.get('page') },
    }));
    const port = await start(router);
    expect(json((await send(port, 'GET', '/search?q=cpu%20load&page=2')).body)).toEqual({ q: 'cpu load', page: '2' });
  });

  it('exposes params as a frozen object', async () => {
    let frozen = false;
    const router = new Router().get('/x/:id', (ctx) => {
      frozen = Object.isFrozen(ctx.params);
      return { status: 204 };
    });
    const port = await start(router);
    await send(port, 'GET', '/x/1');
    expect(frozen).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// 404, 405, 501, OPTIONS and HEAD
// ---------------------------------------------------------------------------

describe('unmatched requests', () => {
  it('answers 404 with a JSON error and a request id', async () => {
    const port = await start(new Router().get('/a', () => ({ status: 200 })));
    const reply = await send(port, 'GET', '/missing');
    expect(reply.status).toBe(404);
    expect(reply.headers['content-type']).toBe('application/json; charset=utf-8');
    const error = (json(reply.body) as { error: { code: string; requestId: string } }).error;
    expect(error.code).toBe('NOT_FOUND');
    expect(error.requestId).toBe(reply.headers['x-request-id']);
  });

  it('answers 405 with a sorted Allow header and the allowed list in details', async () => {
    const noop = (): { status: number } => ({ status: 200 });
    const router = new Router().post('/items', noop).get('/items', noop).delete('/items', noop);
    const port = await start(router);
    const reply = await send(port, 'PUT', '/items');
    expect(reply.status).toBe(405);
    expect(reply.headers['allow']).toBe('DELETE, GET, HEAD, OPTIONS, POST');
    const error = (json(reply.body) as { error: { code: string; details: { allow: string[] } } }).error;
    expect(error.code).toBe('METHOD_NOT_ALLOWED');
    expect(error.details.allow).toEqual(['DELETE', 'GET', 'HEAD', 'OPTIONS', 'POST']);
  });

  it('lets Node reject a method token its parser does not know, before the router sees it', async () => {
    const port = await start(new Router().get('/a', () => ({ status: 200 })));
    const reply = await send(port, 'BREW', '/a');
    expect(reply.status).toBe(400);
  });

  it('answers 501 for a method the router does not know (reached without the HTTP parser)', async () => {
    const router = new Router().get('/a', () => ({ status: 200 }));
    const captured = await handleWithStubs(router, 'BREW', '/a');
    expect(captured.status).toBe(501);
    expect(JSON.parse(captured.body)).toMatchObject({ error: { code: 'NOT_IMPLEMENTED' } });
  });

  it('answers 414 for an overlong path and closes the connection', async () => {
    const port = await start(new Router().get('/a', () => ({ status: 200 })));
    const reply = await send(port, 'GET', `/${'a'.repeat(9000)}`);
    expect(reply.status).toBe(414);
    expect(reply.headers['connection']).toBe('close');
  });

  it('answers OPTIONS on a known path with 204 and Allow when no handler is registered', async () => {
    const port = await start(new Router().get('/a', () => ({ status: 200 })).post('/a', () => ({ status: 201 })));
    const reply = await send(port, 'OPTIONS', '/a');
    expect(reply.status).toBe(204);
    expect(reply.headers['allow']).toBe('GET, HEAD, OPTIONS, POST');
    expect(reply.body).toBe('');
  });

  it('uses an explicit OPTIONS handler when one is registered', async () => {
    const port = await start(
      new Router().add('OPTIONS', '/a', () => ({ status: 200, body: 'custom' })),
    );
    expect(json((await send(port, 'OPTIONS', '/a')).body)).toBe('custom');
  });

  it('answers OPTIONS on an unknown path with 404', async () => {
    const port = await start(new Router().get('/a', () => ({ status: 200 })));
    expect((await send(port, 'OPTIONS', '/zzz')).status).toBe(404);
  });
});

describe('HEAD', () => {
  it('falls back to the GET handler and sends no body', async () => {
    const seen: string[] = [];
    const router = new Router().get('/doc', (ctx) => {
      seen.push(ctx.method);
      return { status: 200, body: { big: 'payload' } };
    });
    const port = await start(router);
    const reply = await send(port, 'HEAD', '/doc');
    expect(reply.status).toBe(200);
    expect(reply.body).toBe('');
    expect(seen).toEqual(['HEAD']);
  });

  it('prefers an explicit HEAD handler over the GET fallback', async () => {
    const router = new Router()
      .get('/doc', () => ({ status: 200 }))
      .add('HEAD', '/doc', () => ({ status: 204 }));
    const port = await start(router);
    expect((await send(port, 'HEAD', '/doc')).status).toBe(204);
  });
});

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

describe('responses', () => {
  it('JSON-encodes the body with a correct content-length', async () => {
    const port = await start(new Router().get('/x', () => ({ status: 201, body: { name: 'né' } })));
    const reply = await send(port, 'GET', '/x');
    expect(reply.status).toBe(201);
    expect(reply.headers['content-type']).toBe('application/json; charset=utf-8');
    expect(Number(reply.headers['content-length'])).toBe(Buffer.byteLength(reply.body));
    expect(json(reply.body)).toEqual({ name: 'né' });
  });

  it('returns 204 with an empty body when the handler returns nothing', async () => {
    const port = await start(new Router().delete('/x', () => undefined));
    const reply = await send(port, 'DELETE', '/x');
    expect(reply.status).toBe(204);
    expect(reply.body).toBe('');
  });

  it('sends an empty body when the response body is undefined', async () => {
    const port = await start(new Router().get('/x', () => ({ status: 202 })));
    const reply = await send(port, 'GET', '/x');
    expect(reply.status).toBe(202);
    expect(reply.body).toBe('');
    expect(reply.headers['content-type']).toBeUndefined();
  });

  it('applies custom headers and keeps a caller-supplied content-type', async () => {
    const port = await start(
      new Router().get('/x', () => ({
        status: 200,
        body: 'plain',
        headers: { 'X-Custom': 'yes', 'Content-Type': 'text/plain' },
      })),
    );
    const reply = await send(port, 'GET', '/x');
    expect(reply.headers['x-custom']).toBe('yes');
    expect(reply.headers['content-type']).toBe('text/plain');
  });

  it('awaits an async handler', async () => {
    const port = await start(
      new Router().get('/x', async () => {
        await new Promise((r) => setTimeout(r, 5));
        return { status: 200, body: 'late' };
      }),
    );
    expect(json((await send(port, 'GET', '/x')).body)).toBe('late');
  });

  it('maps a returned status outside 200..599 to 500', async () => {
    const errors: unknown[] = [];
    const port = await start(
      new Router({ onError: (e) => errors.push(e) }).get('/x', () => ({ status: 42 })),
    );
    const reply = await send(port, 'GET', '/x');
    expect(reply.status).toBe(500);
    expect(errors).toHaveLength(1);
  });

  it('maps a non-JSON-serialisable body to 500', async () => {
    const port = await start(new Router().get('/x', () => ({ status: 200, body: () => 1 })));
    expect((await send(port, 'GET', '/x')).status).toBe(500);
  });
});

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

describe('error handling', () => {
  it('maps a thrown ApiError to its status, code, message and details', async () => {
    const port = await start(
      new Router().get('/x', () => {
        throw new ApiError(409, 'CONFLICT', 'already exists', { id: 'abc' });
      }),
    );
    const reply = await send(port, 'GET', '/x');
    expect(reply.status).toBe(409);
    expect(json(reply.body)).toMatchObject({
      error: { code: 'CONFLICT', message: 'already exists', details: { id: 'abc' } },
    });
  });

  it('freezes ApiError details', () => {
    const err = new ApiError(400, 'X', 'x', { a: 1 });
    expect(Object.isFrozen(err.details)).toBe(true);
    expect(err.name).toBe('ApiError');
  });

  it('hides the message of an unexpected error and reports it to onError', async () => {
    const seen: { err: unknown; method: string; path: string; requestId: string }[] = [];
    const boom = new Error('database password is hunter2');
    const port = await start(
      new Router({
        onError: (err, info) => seen.push({ err, ...info }),
      }).get('/crash/:id', () => {
        throw boom;
      }),
    );
    const reply = await send(port, 'GET', '/crash/9');
    expect(reply.status).toBe(500);
    expect(reply.body).not.toContain('hunter2');
    expect(json(reply.body)).toMatchObject({ error: { code: 'INTERNAL', message: 'internal server error' } });
    expect(seen).toHaveLength(1);
    expect(seen[0]?.err).toBe(boom);
    expect(seen[0]).toMatchObject({ method: 'GET', path: '/crash/9', requestId: reply.headers['x-request-id'] });
  });

  it('a throwing onError hook does not change the response', async () => {
    const port = await start(
      new Router({
        onError: () => {
          throw new Error('hook broke');
        },
      }).get('/x', () => {
        throw new Error('handler broke');
      }),
    );
    const reply = await send(port, 'GET', '/x');
    expect(reply.status).toBe(500);
    expect(json(reply.body)).toMatchObject({ error: { code: 'INTERNAL' } });
  });

  it('turns a rejected promise from a handler into a 500', async () => {
    const port = await start(
      new Router().get('/x', async () => {
        throw new Error('async failure');
      }),
    );
    expect((await send(port, 'GET', '/x')).status).toBe(500);
  });
});

// ---------------------------------------------------------------------------
// Request bodies
// ---------------------------------------------------------------------------

describe('readJson', () => {
  const echo = new Router().post('/echo', async (ctx) => ({ status: 200, body: { got: await ctx.readJson() } }));

  it('parses a JSON body', async () => {
    const port = await start(echo);
    const reply = await send(port, 'POST', '/echo', {
      body: '{"a":[1,2,{"b":null}]}',
      headers: { 'content-type': 'application/json' },
    });
    expect(json(reply.body)).toEqual({ got: { a: [1, 2, { b: null }] } });
  });

  it('accepts a content type with parameters, in any letter case', async () => {
    const port = await start(echo);
    const reply = await send(port, 'POST', '/echo', {
      body: '{"x":1}',
      headers: { 'content-type': 'Application/JSON; charset=utf-8' },
    });
    expect(reply.status).toBe(200);
  });

  it('resolves to undefined for an empty body without checking the content type', async () => {
    const port = await start(echo);
    const reply = await send(port, 'POST', '/echo', { body: '', headers: { 'content-type': 'text/plain' } });
    expect(reply.status).toBe(200);
    expect(json(reply.body)).toEqual({});
  });

  it('answers 415 for a non-JSON content type', async () => {
    const port = await start(echo);
    const reply = await send(port, 'POST', '/echo', { body: 'a=1', headers: { 'content-type': 'text/plain' } });
    expect(reply.status).toBe(415);
    expect((json(reply.body) as { error: { code: string } }).error.code).toBe('UNSUPPORTED_MEDIA_TYPE');
  });

  it('answers 400 for invalid JSON', async () => {
    const port = await start(echo);
    const reply = await send(port, 'POST', '/echo', {
      body: '{"a":',
      headers: { 'content-type': 'application/json' },
    });
    expect(reply.status).toBe(400);
    expect((json(reply.body) as { error: { code: string } }).error.code).toBe('INVALID_JSON');
  });

  it('answers 413 when the body is over the limit, and closes the connection', async () => {
    const port = await start(new Router({ maxBodyBytes: 16 }).post('/echo', async (ctx) => ({
      status: 200,
      body: await ctx.readJson(),
    })));
    const reply = await send(port, 'POST', '/echo', {
      body: JSON.stringify({ padding: 'x'.repeat(100) }),
      headers: { 'content-type': 'application/json' },
    });
    expect(reply.status).toBe(413);
    expect(reply.headers['connection']).toBe('close');
    expect((json(reply.body) as { error: { details: { limit: number } } }).error.details.limit).toBe(16);
  });

  it('accepts a body of exactly the limit', async () => {
    const body = '{"a":"bbbbbbbbb"}';
    const port = await start(new Router({ maxBodyBytes: body.length }).post('/echo', async (ctx) => ({
      status: 200,
      body: await ctx.readJson(),
    })));
    const reply = await send(port, 'POST', '/echo', { body, headers: { 'content-type': 'application/json' } });
    expect(reply.status).toBe(200);
  });

  it('returns the same result on repeated calls within one request', async () => {
    const port = await start(
      new Router().post('/twice', async (ctx) => {
        const first = await ctx.readJson();
        const second = await ctx.readJson();
        return { status: 200, body: { same: first === second, value: first } };
      }),
    );
    const reply = await send(port, 'POST', '/twice', {
      body: '{"k":1}',
      headers: { 'content-type': 'application/json' },
    });
    expect(json(reply.body)).toEqual({ same: true, value: { k: 1 } });
  });

  it('ignores the body when a handler never reads it', async () => {
    const port = await start(new Router().post('/skip', () => ({ status: 202 })));
    const reply = await send(port, 'POST', '/skip', {
      body: 'not json at all',
      headers: { 'content-type': 'text/plain' },
    });
    expect(reply.status).toBe(202);
  });
});

// ---------------------------------------------------------------------------
// Request identifiers
// ---------------------------------------------------------------------------

describe('request ids', () => {
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

  it('generates a UUID when none is supplied', async () => {
    const port = await start(new Router().get('/x', () => ({ status: 204 })));
    const reply = await send(port, 'GET', '/x');
    expect(reply.headers['x-request-id']).toMatch(uuid);
  });

  it('honours a safe incoming request id', async () => {
    const port = await start(new Router().get('/x', () => ({ status: 204 })));
    const reply = await send(port, 'GET', '/x', { headers: { 'x-request-id': 'trace.abc-123_XY' } });
    expect(reply.headers['x-request-id']).toBe('trace.abc-123_XY');
  });

  it.each<[string, string]>([
    ['contains a space', 'bad id'],
    ['contains a disallowed character', 'a;b'],
    ['exceeds 128 characters', 'x'.repeat(129)],
  ])('replaces an incoming id that %s', async (_label, incoming) => {
    const port = await start(new Router().get('/x', () => ({ status: 204 })));
    const reply = await send(port, 'GET', '/x', { headers: { 'x-request-id': incoming } });
    expect(reply.headers['x-request-id']).toMatch(uuid);
  });

  it('exposes the id to handlers', async () => {
    const port = await start(new Router().get('/x', (ctx) => ({ status: 200, body: { id: ctx.requestId } })));
    const reply = await send(port, 'GET', '/x', { headers: { 'x-request-id': 'known-1' } });
    expect(json(reply.body)).toEqual({ id: 'known-1' });
  });
});

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

describe('request context', () => {
  it('provides the normalised path and method', async () => {
    const port = await start(
      new Router().put('/a/:x', (ctx) => ({ status: 200, body: { method: ctx.method, path: ctx.path } })),
    );
    const reply = await send(port, 'PUT', '/a/q/');
    expect(json(reply.body)).toEqual({ method: 'PUT', path: '/a/q' });
  });

  it('exposes the underlying IncomingMessage', async () => {
    let hasHeader = false;
    const port = await start(
      new Router().get('/x', (ctx) => {
        hasHeader = ctx.req.headers['x-probe'] === 'yes';
        return { status: 204 };
      }),
    );
    await send(port, 'GET', '/x', { headers: { 'x-probe': 'yes' } });
    expect(hasHeader).toBe(true);
  });
});
