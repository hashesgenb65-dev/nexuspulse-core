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
 * The API has no authentication. Bind it to a trusted network only.
 */

import * as http from 'node:http';
import type { LogEntry, RaftConsensusNode } from './consensus.js';
import type { KeyValueStore } from './state_machine.js';
import { MAX_KEY_LENGTH } from './state_machine.js';
import { nodeStatus, STATUS_PATH } from './status.js';

export const KV_PATH_PREFIX = '/v1/kv/';
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
}

export interface KvRequest {
  readonly method: string;
  /** Path and optional query, as sent on the request line. */
  readonly url: string;
  /** Raw body text, if any. */
  readonly body?: string;
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

  constructor(options: KvApiOptions) {
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

  private async route(request: KvRequest): Promise<KvResponse> {
    const path = request.url.split('?')[0] ?? '';
    if (path === STATUS_PATH) {
      if (request.method !== 'GET') return failure(405, 'method not allowed', { allow: 'GET' });
      return json(200, nodeStatus(this.node, this.store, this.durable));
    }
    if (!path.startsWith(KV_PATH_PREFIX)) return failure(404, 'not found');

    const key = keyFromPath(path);
    if (key === undefined) return failure(400, `key must be 1 to ${MAX_KEY_LENGTH} characters after URL decoding`);

    switch (request.method) {
      case 'GET':
        return this.read(key);
      case 'PUT': {
        const parsed = parsePutBody(request.body);
        if (!parsed.ok) return failure(400, parsed.reason);
        // submit() proposes before its first await, so the proposal is made within this call.
        return this.submit(path, () => this.node.proposeCommand('SET', { key, value: parsed.value }));
      }
      case 'DELETE':
        return this.submit(path, () => this.node.proposeCommand('DELETE', { key }));
      default:
        return failure(405, 'method not allowed', { allow: 'GET, PUT, DELETE' });
    }
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
        .handle({ method: req.method ?? 'GET', url: req.url ?? '/', body })
        .then((response) => send(res, response))
        .catch(() => send(res, failure(500, 'internal error')));
    });

    req.on('error', () => res.destroy());
  });
}
