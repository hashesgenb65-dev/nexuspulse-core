/**
 * NexusPulse API layer: a dependency-free HTTP router built on node:http.
 *
 * Routes are declared with a method and a path pattern such as
 * `/streams/:streamId/events`. Literal segments are matched exactly and
 * case-sensitively. A `:name` segment captures one decoded path segment.
 *
 * Responses. A handler returns an {@link ApiResponse} (JSON-encoded unless the
 * body is undefined), or nothing (204). Thrown {@link ApiError}s become JSON
 * error responses with a stable `code`. Any other exception becomes a generic
 * 500, and the real error goes to the `onError` hook, never to the client.
 *
 * Request handling order:
 *   1. Assign a request id (honours a safe incoming `x-request-id`).
 *   2. Match the path and method. Unknown path: 404. Known path, wrong method:
 *      405 with an `Allow` header. Unknown method token: 501.
 *   3. OPTIONS on a known path without a handler: 204 with `Allow`.
 *      HEAD without a handler falls back to the GET handler.
 *   4. Run the handler. Bodies are read only through `ctx.readJson()`, which
 *      enforces the size limit and the JSON content type.
 */

import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** An error that maps directly to an HTTP response. */
export class ApiError extends Error {
  public readonly status: number;
  public readonly code: string;
  public readonly details: Readonly<Record<string, unknown>>;
  /** Extra response headers, such as `retry-after`. */
  public readonly headers: Readonly<Record<string, string>>;

  constructor(
    status: number,
    code: string,
    message: string,
    details: Record<string, unknown> = {},
    headers: Readonly<Record<string, string>> = {},
  ) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.details = Object.freeze({ ...details });
    this.headers = Object.freeze({ ...headers });
  }
}

/** A route was declared incorrectly. Code: ROUTER_INVALID_ROUTE. */
export class RouteDefinitionError extends Error {
  public readonly code = 'ROUTER_INVALID_ROUTE';

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'RouteDefinitionError';
  }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** HTTP methods the router understands. */
export type Method = 'GET' | 'HEAD' | 'POST' | 'PUT' | 'PATCH' | 'DELETE' | 'OPTIONS';

const METHODS: ReadonlySet<string> = new Set<Method>(['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS']);

/** Successful response description returned by a handler. */
export interface ApiResponse {
  readonly status: number;
  /** JSON-encoded when not undefined. When undefined the body is empty. Ignored when `raw` is set. */
  readonly body?: unknown;
  /**
   * Sent as-is, without JSON encoding, for example Prometheus text. Takes
   * precedence over `body`. Defaults the content type to text/plain.
   */
  readonly raw?: string;
  readonly headers?: Readonly<Record<string, string>>;
}

/** What a handler receives for each request. */
export interface RequestContext {
  readonly method: Method;
  readonly path: string;
  readonly params: Readonly<Record<string, string>>;
  readonly query: URLSearchParams;
  readonly requestId: string;
  readonly req: IncomingMessage;
  /**
   * Parses the JSON request body. Resolves to undefined for an empty body.
   * Repeated calls return the same result.
   *
   * @throws {ApiError} 413 above the size limit, 415 for a non-JSON content type, 400 for invalid JSON.
   */
  readJson(): Promise<unknown>;
}

/** A route handler. */
export type Handler = (ctx: RequestContext) => ApiResponse | void | Promise<ApiResponse | void>;

/** Hook invoked for every unexpected (non-ApiError) failure. */
export type ErrorHook = (err: unknown, info: { readonly requestId: string; readonly method: string; readonly path: string }) => void;

/** Router configuration. */
export interface RouterOptions {
  /** Largest accepted request body in bytes. Default 1 MiB. */
  readonly maxBodyBytes?: number;
  /** Receives unexpected failures. Exceptions thrown by the hook are swallowed. */
  readonly onError?: ErrorHook;
}

/** A declared route, for introspection. */
export interface RouteInfo {
  readonly method: Method;
  readonly pattern: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Default body limit. */
export const DEFAULT_MAX_BODY_BYTES = 1024 * 1024;

const REQUEST_ID_PATTERN = /^[A-Za-z0-9._-]{1,128}$/;
const PARAM_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;
const MAX_PATH_BYTES = 8192;

// ---------------------------------------------------------------------------
// Pattern compilation and matching
// ---------------------------------------------------------------------------

type Segment = { readonly kind: 'literal'; readonly value: string } | { readonly kind: 'param'; readonly name: string };

interface CompiledRoute {
  readonly method: Method;
  readonly pattern: string;
  readonly segments: readonly Segment[];
  readonly literalCount: number;
  readonly handler: Handler;
}

function splitPath(pathname: string): string[] {
  if (pathname === '/') return [];
  return pathname.slice(1).split('/');
}

function compilePattern(pattern: string): Segment[] {
  if (!pattern.startsWith('/')) throw new RouteDefinitionError(`pattern must start with "/": ${pattern}`);
  if (pattern.length > 1 && pattern.endsWith('/')) {
    throw new RouteDefinitionError(`pattern must not end with "/": ${pattern}`);
  }
  const names = new Set<string>();
  const segments: Segment[] = [];
  for (const raw of splitPath(pattern)) {
    if (raw.length === 0) throw new RouteDefinitionError(`pattern has an empty segment: ${pattern}`);
    if (raw.startsWith(':')) {
      const name = raw.slice(1);
      if (!PARAM_NAME_PATTERN.test(name)) {
        throw new RouteDefinitionError(`invalid parameter name "${name}" in ${pattern}`);
      }
      if (names.has(name)) throw new RouteDefinitionError(`duplicate parameter "${name}" in ${pattern}`);
      names.add(name);
      segments.push({ kind: 'param', name });
    } else {
      if (raw.includes(':')) throw new RouteDefinitionError(`segment "${raw}" mixes literal and parameter syntax`);
      segments.push({ kind: 'literal', value: raw });
    }
  }
  return segments;
}

/** Strips one trailing slash (except on root). Returns the normalised path. */
function normalisePath(pathname: string): string {
  if (pathname.length > 1 && pathname.endsWith('/')) return pathname.slice(0, -1);
  return pathname;
}

/** Matches a path against compiled segments. Returns params, or null when it does not match. */
function matchSegments(segments: readonly Segment[], parts: readonly string[]): Record<string, string> | null {
  if (segments.length !== parts.length) return null;
  const params: Record<string, string> = {};
  for (let i = 0; i < segments.length; i++) {
    const seg = segments[i];
    const part = parts[i];
    if (seg === undefined || part === undefined) return null;
    if (seg.kind === 'literal') {
      if (seg.value !== part) return null;
    } else {
      if (part.length === 0) return null;
      let decoded: string;
      try {
        decoded = decodeURIComponent(part);
      } catch {
        throw new ApiError(400, 'BAD_PATH_ENCODING', 'path parameter is not valid percent-encoding', {
          parameter: seg.name,
        });
      }
      params[seg.name] = decoded;
    }
  }
  return params;
}

function pickRequestId(header: string | string[] | undefined): string {
  const candidate = Array.isArray(header) ? header[0] : header;
  if (candidate !== undefined && REQUEST_ID_PATTERN.test(candidate)) return candidate;
  return randomUUID();
}

// ---------------------------------------------------------------------------
// Body reading
// ---------------------------------------------------------------------------

function readJsonBody(req: IncomingMessage, maxBytes: number): Promise<unknown> {
  return new Promise<unknown>((resolve, reject) => {
    const contentLength = req.headers['content-length'];
    if (contentLength !== undefined && Number(contentLength) > maxBytes) {
      // Drain the rejected body so the client can finish sending and read the 413.
      req.resume();
      reject(new ApiError(413, 'PAYLOAD_TOO_LARGE', `body exceeds ${maxBytes} bytes`, { limit: maxBytes }));
      return;
    }

    const chunks: Buffer[] = [];
    let total = 0;
    let rejected = false;

    req.on('data', (chunk: Buffer) => {
      if (rejected) return;
      total += chunk.length;
      if (total > maxBytes) {
        rejected = true;
        reject(new ApiError(413, 'PAYLOAD_TOO_LARGE', `body exceeds ${maxBytes} bytes`, { limit: maxBytes }));
        return;
      }
      chunks.push(chunk);
    });

    req.on('error', (err: Error) => {
      if (!rejected) {
        rejected = true;
        reject(new ApiError(400, 'BAD_REQUEST_BODY', `request stream failed: ${err.message}`));
      }
    });

    req.on('end', () => {
      if (rejected) return;
      rejected = true;
      if (total === 0) {
        resolve(undefined);
        return;
      }
      const type = (req.headers['content-type'] ?? '').split(';')[0]?.trim().toLowerCase();
      if (type !== 'application/json') {
        reject(new ApiError(415, 'UNSUPPORTED_MEDIA_TYPE', 'request body must be application/json', { type: type ?? '' }));
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')) as unknown);
      } catch {
        reject(new ApiError(400, 'INVALID_JSON', 'request body is not valid JSON'));
      }
    });
  });
}

// ---------------------------------------------------------------------------
// Response writing
// ---------------------------------------------------------------------------

function writeResponse(res: ServerResponse, response: ApiResponse | void): void {
  if (response === undefined) {
    res.statusCode = 204;
    res.end();
    return;
  }
  if (!Number.isInteger(response.status) || response.status < 200 || response.status > 599) {
    throw new TypeError(`handler returned invalid status ${String(response.status)}`);
  }
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(response.headers ?? {})) {
    headers[name.toLowerCase()] = value;
  }
  if (response.raw !== undefined) {
    headers['content-type'] ??= 'text/plain; charset=utf-8';
    headers['content-length'] = String(Buffer.byteLength(response.raw, 'utf8'));
    res.writeHead(response.status, headers);
    res.end(response.raw);
    return;
  }
  if (response.body === undefined) {
    res.writeHead(response.status, headers);
    res.end();
    return;
  }
  const payload = JSON.stringify(response.body);
  if (payload === undefined) throw new TypeError('response body is not JSON-serialisable');
  headers['content-type'] ??= 'application/json; charset=utf-8';
  headers['content-length'] = String(Buffer.byteLength(payload, 'utf8'));
  res.writeHead(response.status, headers);
  res.end(payload);
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/**
 * Wraps a router in an un-started node:http server. Call `listen` on the result.
 * Request handling never rejects, so the returned promise from `handle` is discarded safely.
 */
export function createApiServer(router: Router): Server {
  return createServer((req, res) => {
    void router.handle(req, res);
  });
}

/**
 * Routes HTTP requests by method and path pattern. Create one per server and
 * register routes before calling {@link Router.handle}.
 */
export class Router {
  private readonly routes: CompiledRoute[] = [];
  private readonly maxBodyBytes: number;
  private readonly onError: ErrorHook | undefined;

  constructor(options: RouterOptions = {}) {
    const max = options.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES;
    if (!Number.isInteger(max) || max < 1) {
      throw new RouteDefinitionError('maxBodyBytes must be a positive integer');
    }
    this.maxBodyBytes = max;
    this.onError = options.onError;
  }

  /**
   * Registers a route.
   *
   * @throws {RouteDefinitionError} For an invalid method, pattern, or duplicate route.
   */
  add(method: Method, pattern: string, handler: Handler): this {
    if (!METHODS.has(method)) throw new RouteDefinitionError(`unsupported method ${String(method)}`);
    if (typeof handler !== 'function') throw new RouteDefinitionError('handler must be a function');
    const segments = compilePattern(pattern);
    const duplicate = this.routes.some((r) => r.method === method && r.pattern === pattern);
    if (duplicate) throw new RouteDefinitionError(`duplicate route ${method} ${pattern}`);
    this.routes.push({
      method,
      pattern,
      segments,
      literalCount: segments.filter((s) => s.kind === 'literal').length,
      handler,
    });
    return this;
  }

  get(pattern: string, handler: Handler): this {
    return this.add('GET', pattern, handler);
  }

  post(pattern: string, handler: Handler): this {
    return this.add('POST', pattern, handler);
  }

  put(pattern: string, handler: Handler): this {
    return this.add('PUT', pattern, handler);
  }

  patch(pattern: string, handler: Handler): this {
    return this.add('PATCH', pattern, handler);
  }

  delete(pattern: string, handler: Handler): this {
    return this.add('DELETE', pattern, handler);
  }

  /** Declared routes in registration order. */
  describe(): RouteInfo[] {
    return this.routes.map((r) => ({ method: r.method, pattern: r.pattern }));
  }

  /**
   * Handles one request. Never rejects: every failure becomes a response.
   */
  async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const requestId = pickRequestId(req.headers['x-request-id']);
    res.setHeader('x-request-id', requestId);
    const method = (req.method ?? '').toUpperCase();
    let pathForLog = '';
    try {
      const url = new URL(req.url ?? '/', 'http://localhost');
      if (url.pathname.length > MAX_PATH_BYTES) {
        throw new ApiError(414, 'URI_TOO_LONG', 'request path is too long');
      }
      const path = normalisePath(url.pathname);
      pathForLog = path;

      if (!METHODS.has(method)) {
        throw new ApiError(501, 'NOT_IMPLEMENTED', `method ${method} is not supported`);
      }
      const parts = splitPath(path);
      const matches = this.matchAll(parts);
      if (matches.length === 0) {
        throw new ApiError(404, 'NOT_FOUND', `no route for ${path}`);
      }

      const allowed = this.allowedMethods(matches);
      const requested = method as Method;
      const chosen = this.choose(matches, requested);

      if (chosen === undefined) {
        if (requested === 'OPTIONS') {
          writeResponse(res, { status: 204, headers: { allow: allowed.join(', ') } });
          return;
        }
        throw new ApiError(405, 'METHOD_NOT_ALLOWED', `${method} is not allowed for ${path}`, { allow: allowed });
      }

      const ctx = this.buildContext(req, requestId, requested, path, chosen.params, url);
      const result = await chosen.route.handler(ctx);
      writeResponse(res, result);
    } catch (err) {
      this.fail(res, err, requestId, method, pathForLog);
    }
  }

  private matchAll(parts: readonly string[]): { route: CompiledRoute; params: Record<string, string> }[] {
    const out: { route: CompiledRoute; params: Record<string, string> }[] = [];
    for (const route of this.routes) {
      const params = matchSegments(route.segments, parts);
      if (params !== null) out.push({ route, params });
    }
    return out;
  }

  private allowedMethods(matches: readonly { route: CompiledRoute }[]): string[] {
    const set = new Set<string>(matches.map((m) => m.route.method));
    if (set.has('GET')) set.add('HEAD');
    set.add('OPTIONS');
    return [...set].sort();
  }

  /** Picks the route for a request: exact method first, then HEAD via GET. Most literal segments win. */
  private choose(
    matches: readonly { route: CompiledRoute; params: Record<string, string> }[],
    method: Method,
  ): { route: CompiledRoute; params: Record<string, string> } | undefined {
    const candidates = matches.filter((m) => m.route.method === method);
    if (candidates.length === 0 && method === 'HEAD') {
      return this.choose(matches, 'GET');
    }
    let best: { route: CompiledRoute; params: Record<string, string> } | undefined;
    for (const candidate of candidates) {
      if (best === undefined || candidate.route.literalCount > best.route.literalCount) best = candidate;
    }
    return best;
  }

  private buildContext(
    req: IncomingMessage,
    requestId: string,
    method: Method,
    path: string,
    params: Record<string, string>,
    url: URL,
  ): RequestContext {
    let bodyPromise: Promise<unknown> | undefined;
    return {
      method,
      path,
      params: Object.freeze({ ...params }),
      query: url.searchParams,
      requestId,
      req,
      readJson: () => {
        bodyPromise ??= readJsonBody(req, this.maxBodyBytes);
        return bodyPromise;
      },
    };
  }

  private fail(res: ServerResponse, err: unknown, requestId: string, method: string, path: string): void {
    if (res.headersSent) {
      res.destroy();
      return;
    }
    if (err instanceof ApiError) {
      const extra: Record<string, string> = { ...err.headers };
      const allow = err.details['allow'];
      if (err.status === 405 && Array.isArray(allow)) extra['allow'] = allow.join(', ');
      // After a rejected body the unread remainder would poison keep-alive, so close the connection.
      if (err.status === 413 || err.status === 414) extra['connection'] = 'close';
      this.send(res, err.status, { code: err.code, message: err.message, details: err.details, requestId }, extra);
      return;
    }
    if (this.onError !== undefined) {
      try {
        this.onError(err, { requestId, method, path });
      } catch {
        /* a broken hook must not affect the response */
      }
    }
    this.send(res, 500, { code: 'INTERNAL', message: 'internal server error', requestId });
  }

  private send(
    res: ServerResponse,
    status: number,
    error: Record<string, unknown>,
    extraHeaders: Readonly<Record<string, string>> = {},
  ): void {
    const payload = JSON.stringify({ error });
    res.writeHead(status, {
      'content-type': 'application/json; charset=utf-8',
      'content-length': String(Buffer.byteLength(payload, 'utf8')),
      ...extraHeaders,
    });
    res.end(payload);
  }
}
