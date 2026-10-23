/**
 * Client for the NexusPulse key-value HTTP API.
 *
 * The client talks to a cluster through one or more member addresses. It handles the behaviour the
 * server documents:
 *
 * - Writes go to the leader. A follower answers 307 with the leader's address. The client repeats the
 *   same request there, up to a fixed number of hops, and remembers the leader for later requests.
 * - 503 (no leader yet, or leadership changed) and 429 (rate limited) mean the request was not
 *   applied, so they are retried with backoff. A 429 honours the server's `retry-after`.
 * - 409 means a later leader replaced the write before it committed, so it was not applied. It is
 *   retried.
 * - 504 means the outcome of a write is unknown. Writes are never retried after a 504, because a
 *   retry could apply the change twice. The error says so.
 * - A write whose outcome is ambiguous (a timeout or a dropped connection after sending) is never
 *   retried and never sent to another member. The earlier attempt may still commit, and a retry could
 *   overwrite a newer write from someone else. The error says the outcome is unknown.
 * - Reads (GET) are retried on 503, 429, timeouts and network failures, and may try another member.
 *
 * Every request has a timeout. Authentication uses a bearer token when one is configured.
 */

export type KvErrorCode =
  | 'BAD_REQUEST'
  | 'UNAUTHORIZED'
  | 'FORBIDDEN'
  | 'RATE_LIMITED'
  | 'UNAVAILABLE'
  | 'CONFLICT'
  | 'COMMIT_TIMEOUT'
  | 'TIMEOUT'
  | 'NETWORK'
  | 'REDIRECT_LOOP'
  | 'NO_ENDPOINTS'
  | 'UNEXPECTED';

/** An error from the key-value API or the network. `retryable` says whether a retry is safe. */
export class KvClientError extends Error {
  readonly code: KvErrorCode;
  readonly status: number | undefined;
  readonly retryable: boolean;
  readonly retryAfterMs: number | undefined;
  /** True only when the connection was refused, so the request certainly did not reach the member. */
  readonly notSent: boolean;

  constructor(
    code: KvErrorCode,
    message: string,
    details: { status?: number; retryable?: boolean; retryAfterMs?: number; notSent?: boolean } = {},
  ) {
    super(message);
    this.name = 'KvClientError';
    this.code = code;
    this.status = details.status;
    this.retryable = details.retryable ?? false;
    this.retryAfterMs = details.retryAfterMs;
    this.notSent = details.notSent ?? false;
  }
}

/** Result of a committed write. */
export interface WriteResult {
  readonly key: string;
  readonly index: number;
  readonly term: number;
}

/** A value read from the cluster, with the index the answering member had applied. */
export interface ReadResult<T> {
  readonly key: string;
  readonly value: T;
  readonly appliedIndex: number;
}

/** The subset of the Fetch API the client uses. Injected so tests and hosts can supply their own. */
export type FetchLike = (
  url: string,
  init: {
    readonly method: string;
    readonly headers: Record<string, string>;
    readonly body?: string;
    readonly redirect: 'manual';
    readonly signal: AbortSignal;
  },
) => Promise<{
  readonly status: number;
  readonly headers: { get(name: string): string | null };
  text(): Promise<string>;
}>;

export interface KvClientOptions {
  /** Base addresses of cluster members, for example `http://10.0.0.1:8080`. At least one. */
  readonly endpoints: readonly string[];
  /** Bearer token. Sent on every request when set. */
  readonly token?: string;
  /** Per-request timeout in milliseconds. Default 2 000. */
  readonly timeoutMs?: number;
  /** Retries after the first attempt for retryable failures. Default 3. */
  readonly maxRetries?: number;
  /** First backoff delay in milliseconds. Doubles on each retry. Default 100. */
  readonly backoffMs?: number;
  /** Most redirects followed for one request. Default 3. */
  readonly maxRedirects?: number;
  /** Fetch implementation. Defaults to the global `fetch`. */
  readonly fetch?: FetchLike;
  /** Sleep used between retries. Defaults to a timer. Injected so tests run fast. */
  readonly sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_TIMEOUT_MS = 2_000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BACKOFF_MS = 100;
const DEFAULT_MAX_REDIRECTS = 3;
const MAX_RETRY_AFTER_MS = 60_000;

const defaultSleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

function assertNonNegativeInteger(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 0) throw new RangeError(`${name} must be a non-negative integer`);
}

/** Parses `retry-after` given in seconds. Returns undefined for anything else. */
export function parseRetryAfterMs(header: string | null): number | undefined {
  if (header === null) return undefined;
  const seconds = Number(header.trim());
  if (!Number.isFinite(seconds) || seconds < 0) return undefined;
  return Math.min(Math.round(seconds * 1000), MAX_RETRY_AFTER_MS);
}

/** True when a fetch failure is a refused connection: the request never reached the member. */
export function isConnectionRefused(cause: unknown): boolean {
  let current: unknown = cause;
  for (let depth = 0; depth < 3 && typeof current === 'object' && current !== null; depth++) {
    if ((current as { code?: unknown }).code === 'ECONNREFUSED') return true;
    current = (current as { cause?: unknown }).cause;
  }
  return false;
}

/** Builds the URL path for a key, encoding each path segment so that `/` and spaces survive. */
export function kvPath(key: string): string {
  if (key.length === 0) throw new RangeError('key must not be empty');
  return `/v1/kv/${encodeURIComponent(key)}`;
}

/** Client for the key-value API. Safe to share between callers. */
export class KvClient {
  private readonly endpoints: readonly string[];
  private readonly token: string | undefined;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private readonly backoffMs: number;
  private readonly maxRedirects: number;
  private readonly fetchImpl: FetchLike;
  private readonly sleep: (ms: number) => Promise<void>;
  /** The member that answered last with a successful write, tried first next time. */
  private preferred = 0;

  constructor(options: KvClientOptions) {
    if (options.endpoints.length === 0) {
      throw new KvClientError('NO_ENDPOINTS', 'at least one endpoint is required');
    }
    this.endpoints = options.endpoints.map((endpoint) => endpoint.replace(/\/+$/, ''));
    this.token = options.token;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this.maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
    this.maxRedirects = options.maxRedirects ?? DEFAULT_MAX_REDIRECTS;
    assertNonNegativeInteger('timeoutMs', this.timeoutMs);
    assertNonNegativeInteger('maxRetries', this.maxRetries);
    assertNonNegativeInteger('backoffMs', this.backoffMs);
    assertNonNegativeInteger('maxRedirects', this.maxRedirects);
    this.fetchImpl = options.fetch ?? (globalThis.fetch as unknown as FetchLike);
    this.sleep = options.sleep ?? defaultSleep;
  }

  /**
   * Reads a key. Resolves with undefined when the key does not exist.
   *
   * @throws {KvClientError} For auth failures, unavailable members, timeouts or network errors.
   */
  async get<T = unknown>(key: string): Promise<ReadResult<T> | undefined> {
    const response = await this.request('GET', kvPath(key), undefined, { write: false });
    if (response.status === 404) return undefined;
    const body = this.parseBody(response.body);
    return { key, value: body['value'] as T, appliedIndex: Number(body['appliedIndex']) };
  }

  /** Stores a value. Resolves once the write is committed on the leader. */
  async put(key: string, value: unknown): Promise<WriteResult> {
    const response = await this.request('PUT', kvPath(key), JSON.stringify({ value }), { write: true });
    return this.writeResult(key, response.body);
  }

  /** Deletes a key. Deleting a missing key succeeds. */
  async delete(key: string): Promise<WriteResult> {
    const response = await this.request('DELETE', kvPath(key), undefined, { write: true });
    return this.writeResult(key, response.body);
  }

  /** Member the client currently sends writes to, as a base address. */
  get currentEndpoint(): string {
    return this.endpoints[this.preferred] ?? '';
  }

  private writeResult(key: string, raw: string): WriteResult {
    const body = this.parseBody(raw);
    return { key, index: Number(body['index']), term: Number(body['term']) };
  }

  private parseBody(raw: string): Record<string, unknown> {
    if (raw.length === 0) return {};
    try {
      const parsed = JSON.parse(raw) as unknown;
      return typeof parsed === 'object' && parsed !== null ? (parsed as Record<string, unknown>) : {};
    } catch {
      throw new KvClientError('UNEXPECTED', 'the server sent a body that is not JSON');
    }
  }

  /**
   * Sends a request, retrying retryable failures with exponential backoff. Redirects are followed
   * manually so that the leader address can be remembered.
   */
  private async request(
    method: string,
    path: string,
    body: string | undefined,
    options: { write: boolean },
  ): Promise<{ status: number; body: string }> {
    let attempt = 0;
    for (;;) {
      try {
        const response = await this.attempt(method, path, body, options.write);
        if (response.status >= 200 && response.status < 300) return response;
        // A missing key is an answer to a read, not a failure.
        if (response.status === 404 && method === 'GET') return response;
        throw this.errorFor(response.status, response.body, response.retryAfterMs, options.write);
      } catch (error) {
        const failure = error instanceof KvClientError ? error : this.networkError(error);
        const ambiguous = failure.code === 'TIMEOUT' || (failure.code === 'NETWORK' && !failure.notSent);
        if (options.write && ambiguous) {
          throw new KvClientError(failure.code, `${failure.message}. The outcome is unknown; check the key before retrying a write`, {
            retryable: false,
          });
        }
        const canRetry = failure.retryable && attempt < this.maxRetries;
        if (!canRetry) throw failure;
        const wait = failure.retryAfterMs ?? this.backoffMs * 2 ** attempt;
        attempt++;
        await this.sleep(wait);
      }
    }
  }

  /** One attempt, following redirects. Tries the preferred member first; falls back to the others on network failure. */
  private async attempt(
    method: string,
    path: string,
    body: string | undefined,
    write: boolean,
  ): Promise<{ status: number; body: string; retryAfterMs?: number }> {
    let lastFailure: KvClientError | undefined;
    const order = this.endpoints.map((_, i) => (this.preferred + i) % this.endpoints.length);
    for (const index of order) {
      const base = this.endpoints[index] ?? '';
      try {
        const result = await this.followRedirects(method, `${base}${path}`, body);
        if (write && result.status >= 200 && result.status < 300) this.preferred = index;
        return result;
      } catch (error) {
        const failure = error instanceof KvClientError ? error : this.networkError(error);
        // A write that may have reached a member must not be sent anywhere else. A refused connection
        // never reached one, so the next member is safe to try.
        if ((write && !failure.notSent) || !failure.retryable) throw failure;
        lastFailure = failure;
      }
    }
    throw lastFailure ?? this.networkError(undefined);
  }

  private async followRedirects(
    method: string,
    url: string,
    body: string | undefined,
  ): Promise<{ status: number; body: string; retryAfterMs?: number }> {
    let target = url;
    for (let hop = 0; ; hop++) {
      const response = await this.send(method, target, body);
      if (response.status !== 307) return response;
      if (hop >= this.maxRedirects) {
        throw new KvClientError('REDIRECT_LOOP', `more than ${this.maxRedirects} redirects`, { retryable: false });
      }
      const location = response.location;
      if (location === undefined) {
        throw new KvClientError('UNEXPECTED', 'redirect without a location', { status: 307 });
      }
      target = location;
    }
  }

  private async send(
    method: string,
    url: string,
    body: string | undefined,
  ): Promise<{ status: number; body: string; location?: string; retryAfterMs?: number }> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    const headers: Record<string, string> = {};
    if (body !== undefined) headers['content-type'] = 'application/json';
    if (this.token !== undefined) headers['authorization'] = `Bearer ${this.token}`;
    try {
      const response = await this.fetchImpl(url, {
        method,
        headers,
        ...(body === undefined ? {} : { body }),
        redirect: 'manual',
        signal: controller.signal,
      });
      const location = response.headers.get('location') ?? undefined;
      const retryAfterMs = parseRetryAfterMs(response.headers.get('retry-after'));
      return {
        status: response.status,
        body: await response.text(),
        ...(location === undefined ? {} : { location }),
        ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      };
    } catch (error) {
      if (controller.signal.aborted) {
        throw new KvClientError('TIMEOUT', `no answer from ${url} within ${this.timeoutMs} ms`, { retryable: true });
      }
      throw this.networkError(error);
    } finally {
      clearTimeout(timer);
    }
  }

  private networkError(cause: unknown): KvClientError {
    const reason = cause instanceof Error ? cause.message : 'unknown error';
    return new KvClientError('NETWORK', `cannot reach the cluster: ${reason}`, {
      retryable: true,
      notSent: isConnectionRefused(cause),
    });
  }

  /** Maps a non-success status to an error, and says whether a retry is safe for this request. */
  private errorFor(status: number, body: string, retryAfterMs: number | undefined, write: boolean): KvClientError {
    const message = this.messageFrom(body, status);
    switch (status) {
      case 400:
        return new KvClientError('BAD_REQUEST', message, { status, retryable: false });
      case 401:
        return new KvClientError('UNAUTHORIZED', message, { status, retryable: false });
      case 403:
        return new KvClientError('FORBIDDEN', message, { status, retryable: false });
      case 409:
        return new KvClientError('CONFLICT', message, { status, retryable: true });
      case 429:
        return new KvClientError('RATE_LIMITED', message, { status, retryable: true, retryAfterMs });
      case 503:
        return new KvClientError('UNAVAILABLE', message, { status, retryable: true, retryAfterMs });
      case 504:
        return new KvClientError(
          'COMMIT_TIMEOUT',
          `${message}. The outcome is unknown; check the key before retrying a write`,
          { status, retryable: !write },
        );
      default:
        return new KvClientError('UNEXPECTED', message, { status, retryable: false });
    }
  }

  private messageFrom(body: string, status: number): string {
    try {
      const parsed = JSON.parse(body) as { error?: unknown };
      if (typeof parsed.error === 'string') return parsed.error;
    } catch {
      // Not JSON; fall through to the generic message.
    }
    return `request failed with status ${status}`;
  }
}
