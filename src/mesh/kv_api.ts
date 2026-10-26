/**
 * HTTP key-value API for one mesh member.
 *
 *   GET    /v1/kv/:key   reads the value this node has applied. A follower may lag the leader
 *                        by a few entries. The response carries `appliedIndex`.
 *   PUT    /v1/kv/:key   body {"value": <any JSON>}. Replicated as a SET.
 *   DELETE /v1/kv/:key   Replicated as a DELETE. Deleting a missing key succeeds.
 *
 * Writes go through the leader:
 * - On a follower, a write answers 307 with the leader's address in `location`. The client
 *   repeats the same request there. With no known leader, it answers 503.
 * - A write answers 200 only after the entry is committed and applied on this node.
 * - 409 means a later leader replaced the entry before it committed, so it was not applied.
 * - 504 means the entry did not commit before the timeout. Its outcome is unknown.
 *
 * Authentication. With a token store (`auth`), GET and the status endpoint need a reader token or
 * better, and PUT and DELETE need a writer token. Requests without a valid token get 401; a
 * token with too few rights gets 403. Without a token store the API is open, so bind it to a
 * trusted network in that case.
 */

import * as http from 'node:http';
import { roleSatisfies, type Authenticator } from '../api/auth.js';
import { SESSION_LOGIN_PATH } from '../api/session_auth.js';
import type { AuthManager } from '../auth/manager.js';
import { LoginFailureLimiter } from '../auth/login_limiter.js';

/** Reads `{ "apiKey": "..." }`. Returns undefined for any other shape, or a key that is empty or too long. */
function parseLoginBody(body: string | undefined): string | undefined {
  if (body === undefined) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return undefined;
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return undefined;
  const apiKey = (parsed as { apiKey?: unknown }).apiKey;
  if (typeof apiKey !== 'string' || apiKey.length === 0 || apiKey.length > 512) return undefined;
  return apiKey;
}
import type { DistributedRateLimiterEngine } from '../ratelimit/limiter.js';
import type { AuditLedgerEngine } from '../audit/ledger.js';
import type { LogEntry, RaftConsensusNode } from './consensus.js';
import type { KeyValueStore } from './state_machine.js';
import { MAX_KEY_LENGTH } from './state_machine.js';
import { nodeStatus, STATUS_PATH } from './status.js';

export const KV_PATH_PREFIX = '/v1/kv/';
/** Lists audit records. Needs the audit ledger and a reader token. */
export const AUDIT_PATH = '/v1/audit';
/** Checks the audit chain's hashes. Needs the audit ledger and a reader token. */
export const AUDIT_VERIFY_PATH = '/v1/audit/verify';
export const DEFAULT_AUDIT_LIMIT = 100;
export const MAX_AUDIT_LIMIT = 1_000;
export const MAX_KV_BODY_BYTES = 64 * 1024;
export const DEFAULT_COMMIT_TIMEOUT_MS = 2_000;
const POLL_INTERVAL_MS = 5;

/** Where another member's HTTP API listens. */
export interface KvPeerHttp {
  readonly host: string;
  readonly httpPort: number;
}

export interface KvApiOptions {
  readonly node: RaftConsensusNode;
  readonly store: KeyValueStore;
  /** Applies committed entries to the store. Implemented by CommitApplier and StateHost. */
  readonly applier: { applyCommitted(): number };
  /** HTTP addresses of the other members, keyed by node id. Used to redirect writes. */
  readonly peerHttp: ReadonlyMap<string, KvPeerHttp>;
  readonly commitTimeoutMs?: number;
  /** Saves durable state after a write is appended locally, before the write waits for commit. */
  readonly persist?: () => void;
  /** Whether this member writes its state to disk. Reported by GET /v1/status. */
  readonly durable?: boolean;
  /** Token store. When set, every request needs a bearer token of the right role. */
  readonly auth?: Authenticator;
  /**
   * Rate limiter, keyed by token name. Each request costs one token. A request over the limit gets
   * a 429 with `retry-after`. Requires `auth`: without a token store there is no client identity, so
   * requests are not limited.
   */
  readonly rateLimiter?: DistributedRateLimiterEngine;
  /**
   * Session store. When set, POST /v1/sessions exchanges an API key for a session id. Use it together
   * with an authenticator that accepts sessions, such as SessionAuthenticator.
   */
  readonly sessions?: AuthManager;
  /**
   * Limits wrong API keys per key id on POST /v1/sessions. Defaults to 5 failures per 15 minutes.
   * Only used together with `sessions`.
   */
  readonly loginLimiter?: LoginFailureLimiter;
  /**
   * Audit ledger. Every committed write is appended with the token name that made it, the key, the
   * Raft index and term, and the size of the value. Values themselves are not recorded.
   */
  readonly audit?: AuditLedgerEngine;
}

export interface KvRequest {
  readonly method: string;
  /** Path and optional query, as sent on the request line. */
  readonly url: string;
  /** Raw body text, if any. */
  readonly body?: string;
  /** The Authorization header, if the request sent one. */
  readonly authorization?: string;
}

export interface KvResponse {
  readonly status: number;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: unknown;
}

type ParsedBody = { readonly ok: true; readonly value: unknown } | { readonly ok: false; readonly reason: string };

function json(status: number, body: unknown, headers: Record<string, string> = {}): KvResponse {
  return { status, headers: { 'content-type': 'application/json', ...headers }, body };
}

function failure(status: number, message: string, headers: Record<string, string> = {}): KvResponse {
  return json(status, { error: message }, headers);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Returns the key in a path, or undefined if the path does not hold a valid key. */
function keyFromPath(path: string): string | undefined {
  let key: string;
  try {
    key = decodeURIComponent(path.slice(KV_PATH_PREFIX.length));
  } catch {
    return undefined;
  }
  return key.length >= 1 && key.length <= MAX_KEY_LENGTH ? key : undefined;
}

function parsePutBody(body: string | undefined): ParsedBody {
  if (body === undefined || body.length === 0) {
    return { ok: false, reason: 'body must be JSON with a value field' };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(body) as unknown;
  } catch {
    return { ok: false, reason: 'body is not valid JSON' };
  }
  if (!isRecord(parsed) || !('value' in parsed)) {
    return { ok: false, reason: 'body must be an object with a value field' };
  }
  return { ok: true, value: parsed.value };
}

function hostForUrl(host: string): string {
  return host.includes(':') ? `[${host}]` : host;
}

export class KvApi {
  private readonly node: RaftConsensusNode;
  private readonly store: KeyValueStore;
  private readonly applier: { applyCommitted(): number };
  private readonly peerHttp: ReadonlyMap<string, KvPeerHttp>;
  private readonly commitTimeoutMs: number;
  private readonly persist: (() => void) | undefined;
  private readonly durable: boolean;
  private readonly auth: Authenticator | undefined;
  private readonly rateLimiter: DistributedRateLimiterEngine | undefined;
  private readonly audit: AuditLedgerEngine | undefined;
  private readonly sessions: AuthManager | undefined;
  private readonly loginLimiter: LoginFailureLimiter;

  constructor(options: KvApiOptions) {
    this.auth = options.auth;
    this.sessions = options.sessions;
    this.loginLimiter = options.loginLimiter ?? new LoginFailureLimiter();
    this.rateLimiter = options.rateLimiter;
    this.audit = options.audit;
    this.node = options.node;
    this.store = options.store;
    this.applier = options.applier;
    this.peerHttp = options.peerHttp;
    this.commitTimeoutMs = options.commitTimeoutMs ?? DEFAULT_COMMIT_TIMEOUT_MS;
    this.persist = options.persist;
    this.durable = options.durable ?? false;
  }

  /** Answers one request. Never throws: unexpected errors become a 500 without details. */
  async handle(request: KvRequest): Promise<KvResponse> {
    try {
      return await this.route(request);
    } catch {
      return failure(500, 'internal error');
    }
  }

  /**
   * Checks the bearer token against the role a request needs. Returns the response to send when the
   * request must stop, or the token name to rate-limit under. The name is undefined when there is no
   * token store.
   */
  /**
   * Exchanges an API key for a session id, which is then sent as the bearer token. The key is never
   * stored in the reply, and the reply is not cached.
   */
  private login(request: KvRequest): KvResponse {
    if (this.sessions === undefined) return failure(404, 'not found');
    if (request.method !== 'POST') return failure(405, 'method not allowed', { allow: 'POST' });
    const apiKey = parseLoginBody(request.body);
    if (apiKey === undefined) return failure(400, 'body must be JSON with an apiKey string of 1 to 512 characters');
    // The key id is the part before the first dot. A blocked key id is refused before its secret is checked.
    const keyId = apiKey.split('.')[0] ?? apiKey;
    const gate = this.loginLimiter.check(keyId);
    if (!gate.allowed) {
      const retryAfterSec = Math.max(1, Math.ceil(gate.retryAfterMs / 1000));
      return failure(429, 'too many failed logins', { 'retry-after': String(retryAfterSec) });
    }
    const session = this.sessions.authenticateApiKey(apiKey);
    if (session === null) {
      this.loginLimiter.recordFailure(keyId);
      return failure(401, 'invalid API key', { 'www-authenticate': 'Bearer realm="nexuspulse"' });
    }
    this.loginLimiter.clear(keyId);
    return json(201, { sessionId: session.sessionId, expiresAt: session.expiresAt }, { 'cache-control': 'no-store' });
  }

  private authorize(request: KvRequest): { readonly response: KvResponse } | { readonly client: string | undefined } {
    if (this.auth === undefined) return { client: undefined };
    const required = request.method === 'GET' ? 'reader' : 'writer';
    try {
      const principal = this.auth.authenticateHeader(request.authorization);
      if (roleSatisfies(principal.role, required)) return { client: principal.name };
      return { response: failure(403, `this request needs the ${required} role`) };
    } catch {
      return {
        response: failure(401, 'missing or invalid bearer token', { 'www-authenticate': 'Bearer realm="nexuspulse"' }),
      };
    }
  }

  /** Takes one token from the client's bucket. Returns a 429 when the bucket is empty. */
  private rateLimit(client: string): KvResponse | undefined {
    if (this.rateLimiter === undefined) return undefined;
    const result = this.rateLimiter.consume(client, 1);
    if (result.allowed) return undefined;
    const retryAfterSec = Math.max(1, Math.ceil(result.resetInMs / 1000));
    return failure(429, 'rate limit exceeded', { 'retry-after': String(retryAfterSec) });
  }

  private async route(request: KvRequest): Promise<KvResponse> {
    if ((request.url.split('?')[0] ?? '') === SESSION_LOGIN_PATH) return this.login(request);
    const auth = this.authorize(request);
    if ('response' in auth) return auth.response;
    if (auth.client !== undefined) {
      const limited = this.rateLimit(auth.client);
      if (limited !== undefined) return limited;
    }

    const path = request.url.split('?')[0] ?? '';
    if (path === AUDIT_PATH || path === AUDIT_VERIFY_PATH) {
      if (request.method !== 'GET') return failure(405, 'method not allowed', { allow: 'GET' });
      return this.audited(path, request.url);
    }
    if (path === STATUS_PATH) {
      if (request.method !== 'GET') return failure(405, 'method not allowed', { allow: 'GET' });
      return json(200, nodeStatus(this.node, this.store, this.durable));
    }
    if (!path.startsWith(KV_PATH_PREFIX)) return failure(404, 'not found');

    const key = keyFromPath(path);
    if (key === undefined) return failure(400, `key must be 1 to ${MAX_KEY_LENGTH} characters after URL decoding`);

    const actor = 'client' in auth && auth.client !== undefined ? auth.client : 'unauthenticated';

    switch (request.method) {
      case 'GET':
        return this.read(key);
      case 'PUT': {
        const parsed = parsePutBody(request.body);
        if (!parsed.ok) return failure(400, parsed.reason);
        // submit() proposes before its first await, so the proposal is made within this call.
        const response = await this.submit(path, () => this.node.proposeCommand('SET', { key, value: parsed.value }));
        this.recordWrite(actor, 'SET', key, response, { valueBytes: Buffer.byteLength(JSON.stringify(parsed.value)) });
        return response;
      }
      case 'DELETE': {
        const response = await this.submit(path, () => this.node.proposeCommand('DELETE', { key }));
        this.recordWrite(actor, 'DELETE', key, response, {});
        return response;
      }
      default:
        return failure(405, 'method not allowed', { allow: 'GET, PUT, DELETE' });
    }
  }

  /** Appends a committed write to the audit ledger. Only a 200 response counts as committed. */
  private recordWrite(
    actor: string,
    action: 'SET' | 'DELETE',
    key: string,
    response: KvResponse,
    extra: Record<string, number>,
  ): void {
    if (this.audit === undefined || response.status !== 200) return;
    const body = response.body as { index?: number; term?: number };
    if (typeof body.index !== 'number' || typeof body.term !== 'number') return;
    this.audit.append(actor, action, `kv:${key}`, { index: body.index, term: body.term, ...extra });
  }

  /** Answers the audit endpoints. Read-only, and only when a ledger is configured. */
  private audited(path: string, url: string): KvResponse {
    if (this.audit === undefined) return failure(404, 'not found');
    if (path === AUDIT_VERIFY_PATH) {
      return json(200, this.audit.verifyIntegrity());
    }
    return this.listAudit(url);
  }

  private listAudit(url: string): KvResponse {
    const audit = this.audit;
    if (audit === undefined) return failure(404, 'not found');
    const params = new URLSearchParams(url.split('?')[1] ?? '');

    const filter: { actorId?: string; action?: string; startTime?: number; endTime?: number } = {};
    const actorId = params.get('actor');
    if (actorId !== null) filter.actorId = actorId;
    const action = params.get('action');
    if (action !== null) filter.action = action;
    for (const [name, target] of [['since', 'startTime'], ['until', 'endTime']] as const) {
      const raw = params.get(name);
      if (raw === null) continue;
      const value = Number(raw);
      if (!Number.isFinite(value)) return failure(400, `${name} must be a number of epoch milliseconds`);
      filter[target] = value;
    }

    const limitRaw = params.get('limit');
    const limit = limitRaw === null ? DEFAULT_AUDIT_LIMIT : Number(limitRaw);
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_AUDIT_LIMIT) {
      return failure(400, `limit must be an integer from 1 to ${MAX_AUDIT_LIMIT}`);
    }

    const matches = audit.query(filter);
    return json(200, { total: matches.length, records: matches.slice(0, limit) });
  }

  private read(key: string): KvResponse {
    this.applier.applyCommitted();
    if (!this.store.has(key)) {
      return failure(404, 'key not found');
    }
    return json(200, { key, value: this.store.get(key), appliedIndex: this.store.lastApplied });
  }

  private async submit(path: string, propose: () => LogEntry): Promise<KvResponse> {
    if (this.node.state !== 'LEADER') return this.redirect(path);

    let entry: LogEntry;
    try {
      entry = propose();
    } catch {
      return failure(503, 'leadership changed; retry', { 'retry-after': '1' });
    }
    this.persist?.();
    return this.awaitCommit(entry, path);
  }

  private redirect(path: string): KvResponse {
    const leader = this.node.leaderId;
    if (leader === null) return failure(503, 'no leader is elected yet', { 'retry-after': '1' });

    const address = this.peerHttp.get(leader);
    if (address === undefined) return failure(503, 'the leader has no configured HTTP address', { 'retry-after': '1' });

    return {
      status: 307,
      headers: {
        'content-type': 'application/json',
        location: `http://${hostForUrl(address.host)}:${address.httpPort}${path}`,
      },
      body: { error: 'not the leader', leader },
    };
  }

  private async awaitCommit(entry: LogEntry, path: string): Promise<KvResponse> {
    const key = keyFromPath(path) ?? '';
    const deadline = Date.now() + this.commitTimeoutMs;
    for (;;) {
      this.applier.applyCommitted();
      if (this.store.lastApplied >= entry.index) break;
      if (Date.now() >= deadline) {
        return failure(504, 'not committed before the timeout; the outcome is unknown');
      }
      await sleep(POLL_INTERVAL_MS);
    }

    const stored = this.node.entryAt(entry.index);
    if (stored === undefined || stored.term !== entry.term) {
      return failure(409, 'a later leader replaced this write before it committed; it was not applied; retry');
    }
    return json(200, { key, index: entry.index, term: entry.term });
  }
}

function send(res: http.ServerResponse, response: KvResponse): void {
  const payload = JSON.stringify(response.body);
  res.writeHead(response.status, { ...response.headers, 'content-length': Buffer.byteLength(payload) });
  res.end(payload);
}

/** Wraps a {@link KvApi} in a Node HTTP server. Bodies over MAX_KV_BODY_BYTES get a 413. */
export function createKvServer(api: KvApi): http.Server {
  return http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    let size = 0;
    let tooLarge = false;

    req.on('data', (chunk: Buffer) => {
      if (tooLarge) return;
      size += chunk.length;
      if (size > MAX_KV_BODY_BYTES) {
        tooLarge = true;
        chunks.length = 0;
        return;
      }
      chunks.push(chunk);
    });

    req.on('end', () => {
      if (tooLarge) {
        send(res, failure(413, `body is larger than ${MAX_KV_BODY_BYTES} bytes`, { connection: 'close' }));
        return;
      }
      const body = chunks.length === 0 ? undefined : Buffer.concat(chunks).toString('utf8');
      api
        .handle({
          method: req.method ?? 'GET',
          url: req.url ?? '/',
          body,
          ...(typeof req.headers.authorization === 'string' ? { authorization: req.headers.authorization } : {}),
        })
        .then((response) => send(res, response))
        .catch(() => send(res, failure(500, 'internal error')));
    });

    req.on('error', () => res.destroy());
  });
}
