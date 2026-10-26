/**
 * The key-value client, run against a real key-value API on a loopback socket. Failure cases wrap
 * the real fetch so that a specific status, redirect or network error can be produced on demand.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import { TokenAuth, type TokenSpec } from '../../src/api/auth';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi, createKvServer } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';
import {
  KvClient,
  KvClientError,
  kvPath,
  parseRetryAfterMs,
  type FetchLike,
} from '../../src/client/kv_client';

const WRITER = 'writer-secret-0123456789abcdefgh';
const READER = 'reader-secret-0123456789abcdefgh';
const specs: TokenSpec[] = [
  { name: 'edge-01', role: 'writer', secret: WRITER },
  { name: 'dash', role: 'reader', secret: READER },
];

const closers: Array<() => Promise<void>> = [];

afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
});

/** A real fetch with the subset of the API the client uses. */
const realFetch: FetchLike = async (url, init) => {
  const response = await fetch(url, init as RequestInit);
  return { status: response.status, headers: response.headers, text: () => response.text() };
};

/** Starts a real key-value server with a solo leader. Returns its base address. */
async function startLeader(auth?: TokenAuth): Promise<string> {
  const node = new RaftConsensusNode({ nodeId: 'solo', clusterPeers: [], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
  node.start();
  node.startElection();
  const store = new KeyValueStore();
  const api = new KvApi({
    node,
    store,
    applier: new CommitApplier(node, store),
    peerHttp: new Map(),
    commitTimeoutMs: 500,
    ...(auth === undefined ? {} : { auth }),
  });
  const server = createKvServer(api);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  closers.push(() => new Promise<void>((resolve) => server.close(() => resolve())));
  const { port } = server.address() as AddressInfo;
  return `http://127.0.0.1:${port}`;
}

/** An address with nothing listening on it: a port that was free a moment ago, now closed. */
async function closedBase(): Promise<string> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  const { port } = server.address() as AddressInfo;
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return `http://127.0.0.1:${port}`;
}

/** A member that always answers 307, like a follower does. `locate` builds the target from this member's base address and the request path. */
async function startFollower(locate: (base: string, path: string) => string): Promise<string> {
  let base = '';
  const server = http.createServer((req, res) => {
    res.writeHead(307, { location: locate(base, req.url ?? '/'), 'content-type': 'application/json' });
    res.end('{"error":"not the leader"}');
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  closers.push(() => new Promise<void>((resolve) => server.close(() => resolve())));
  const { port } = server.address() as AddressInfo;
  base = `http://127.0.0.1:${port}`;
  return base;
}

/** A fetch that answers from `script` for the first calls, then uses the real fetch. */
function scripted(
  script: Array<((url: string) => Awaited<ReturnType<FetchLike>>) | 'network' | 'hang'>,
  calls: string[],
): FetchLike {
  let index = 0;
  return async (url, init) => {
    calls.push(url);
    const step = script[index++];
    if (step === undefined) return realFetch(url, init);
    const result = typeof step === 'string' ? step : step(url);
    if (result === 'network') throw new TypeError('fetch failed');
    if (result === 'hang') {
      return new Promise((_resolve, reject) => {
        init.signal.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError')));
      });
    }
    return result;
  };
}

const status = (code: number, headers: Record<string, string> = {}, body = '{"error":"x"}') => (): Awaited<ReturnType<FetchLike>> => ({
  status: code,
  headers: { get: (name: string) => headers[name.toLowerCase()] ?? null },
  text: async () => body,
});

describe('keys and retry-after', () => {
  it('encodes each key as one path segment', () => {
    expect(kvPath('a/b c')).toBe('/v1/kv/a%2Fb%20c');
  });

  it('rejects an empty key', () => {
    expect(() => kvPath('')).toThrow(RangeError);
  });

  it('parses retry-after in seconds and caps it', () => {
    expect(parseRetryAfterMs('2')).toBe(2_000);
    expect(parseRetryAfterMs('0')).toBe(0);
    expect(parseRetryAfterMs('abc')).toBeUndefined();
    expect(parseRetryAfterMs('-1')).toBeUndefined();
    expect(parseRetryAfterMs(null)).toBeUndefined();
    expect(parseRetryAfterMs('100000')).toBe(60_000);
  });
});

describe('round trips against a real cluster', () => {
  it('writes, reads back, and deletes a value', async () => {
    const base = await startLeader();
    const client = new KvClient({ endpoints: [base], fetch: realFetch });

    const written = await client.put('greeting', 'hello');
    expect(written).toMatchObject({ key: 'greeting', index: 1 });

    const read = await client.get<string>('greeting');
    expect(read?.value).toBe('hello');

    await client.delete('greeting');
    expect(await client.get('greeting')).toBeUndefined();
  });

  it('round-trips keys that contain a slash and a space', async () => {
    const base = await startLeader();
    const client = new KvClient({ endpoints: [base], fetch: realFetch });
    await client.put('sensors/room 1', { t: 21 });
    expect((await client.get<{ t: number }>('sensors/room 1'))?.value).toEqual({ t: 21 });
  });

  it('returns undefined for a missing key without retrying', async () => {
    const base = await startLeader();
    const calls: string[] = [];
    const client = new KvClient({ endpoints: [base], fetch: scripted([], calls) });
    expect(await client.get('nothing')).toBeUndefined();
    expect(calls).toHaveLength(1);
  });

  it('strips trailing slashes from endpoints', async () => {
    const base = await startLeader();
    const client = new KvClient({ endpoints: [`${base}///`], fetch: realFetch });
    await client.put('a', 1);
    expect(client.currentEndpoint).toBe(base);
  });
});

describe('authentication', () => {
  it('sends the bearer token and succeeds with a writer token', async () => {
    const base = await startLeader(new TokenAuth(specs));
    const client = new KvClient({ endpoints: [base], token: WRITER, fetch: realFetch });
    await expect(client.put('a', 1)).resolves.toMatchObject({ index: 1 });
  });

  it('reports 401 without retrying', async () => {
    const base = await startLeader(new TokenAuth(specs));
    const calls: string[] = [];
    const client = new KvClient({ endpoints: [base], fetch: scripted([], calls), sleep: async () => undefined });
    await expect(client.get('a')).rejects.toMatchObject({ code: 'UNAUTHORIZED', status: 401, retryable: false });
    expect(calls).toHaveLength(1);
  });

  it('reports 403 for a write made with a reader token, without retrying', async () => {
    const base = await startLeader(new TokenAuth(specs));
    const calls: string[] = [];
    const client = new KvClient({ endpoints: [base], token: READER, fetch: scripted([], calls) });
    await expect(client.put('a', 1)).rejects.toMatchObject({ code: 'FORBIDDEN', status: 403 });
    expect(calls).toHaveLength(1);
  });
});

describe('retries', () => {
  it('retries a read on 503 with exponential backoff', async () => {
    const base = await startLeader();
    const calls: string[] = [];
    const waits: number[] = [];
    const client = new KvClient({
      endpoints: [base],
      backoffMs: 100,
      fetch: scripted([status(503), status(503)], calls),
      sleep: async (ms) => {
        waits.push(ms);
      },
    });
    await client.put('a', 1);
    expect(calls).toHaveLength(3);
    expect(waits).toEqual([100, 200]);
  });

  it('retries a write on 503, because it was not applied', async () => {
    const base = await startLeader();
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: [base],
      fetch: scripted([status(503)], calls),
      sleep: async () => undefined,
    });
    await expect(client.put('a', 1)).resolves.toMatchObject({ index: 1 });
    expect(calls).toHaveLength(2);
  });

  it('honours retry-after on 429', async () => {
    const base = await startLeader();
    const waits: number[] = [];
    const client = new KvClient({
      endpoints: [base],
      fetch: scripted([status(429, { 'retry-after': '3' })], []),
      sleep: async (ms) => {
        waits.push(ms);
      },
    });
    await client.put('a', 1);
    expect(waits).toEqual([3_000]);
  });

  it('gives up after maxRetries and reports the last error', async () => {
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: ['http://127.0.0.1:9'],
      maxRetries: 2,
      fetch: scripted([status(503), status(503), status(503), status(503)], calls),
      sleep: async () => undefined,
    });
    await expect(client.get('a')).rejects.toMatchObject({ code: 'UNAVAILABLE', status: 503 });
    expect(calls).toHaveLength(3);
  });

  it('does not retry a 400', async () => {
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: ['http://127.0.0.1:9'],
      fetch: scripted([status(400, {}, '{"error":"bad key"}')], calls),
      sleep: async () => undefined,
    });
    await expect(client.get('a')).rejects.toMatchObject({ code: 'BAD_REQUEST', message: 'bad key' });
    expect(calls).toHaveLength(1);
  });

  it('retries a conflict, because the write was not applied', async () => {
    const base = await startLeader();
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: [base],
      fetch: scripted([status(409)], calls),
      sleep: async () => undefined,
    });
    await client.put('a', 1);
    expect(calls).toHaveLength(2);
  });
});

describe('write outcomes that are unknown', () => {
  it('does not retry a 504 on a write', async () => {
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: ['http://127.0.0.1:9'],
      fetch: scripted([status(504, {}, '{"error":"not committed before the timeout"}')], calls),
      sleep: async () => undefined,
    });
    await expect(client.put('a', 1)).rejects.toMatchObject({ code: 'COMMIT_TIMEOUT', retryable: false });
    expect(calls).toHaveLength(1);
  });

  it('says the outcome is unknown on a write timeout, and does not retry', async () => {
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: ['http://127.0.0.1:9'],
      timeoutMs: 20,
      fetch: scripted(['hang' as const], calls),
      sleep: async () => undefined,
    });
    const error = await client.put('a', 1).catch((err: unknown) => err);
    expect(error).toBeInstanceOf(KvClientError);
    expect((error as KvClientError).message).toMatch(/outcome is unknown/);
    expect(calls).toHaveLength(1);
  });

  it('does not retry a write after a dropped connection', async () => {
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: ['http://127.0.0.1:9'],
      fetch: scripted(['network' as const], calls),
      sleep: async () => undefined,
    });
    await expect(client.delete('a')).rejects.toMatchObject({ code: 'NETWORK' });
    expect(calls).toHaveLength(1);
  });

  it('retries a read after a timeout', async () => {
    const base = await startLeader();
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: [base],
      timeoutMs: 20,
      fetch: scripted(['hang' as const], calls),
      sleep: async () => undefined,
    });
    expect(await client.get('missing')).toBeUndefined();
    expect(calls).toHaveLength(2);
  });
});

describe('failover between members', () => {
  it('reads from the second member when the first cannot be reached', async () => {
    const base = await startLeader();
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: ['http://127.0.0.1:9', base],
      fetch: scripted(['network' as const], calls),
      sleep: async () => undefined,
    });
    expect(await client.get('missing')).toBeUndefined();
    expect(calls).toHaveLength(2);
  });

  it('does not send a write to a second member after the first may have received it', async () => {
    const base = await startLeader();
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: [base, 'http://127.0.0.1:9'],
      fetch: scripted(['network' as const], calls),
      sleep: async () => undefined,
    });
    await expect(client.put('a', 1)).rejects.toMatchObject({ code: 'NETWORK' });
    expect(calls).toEqual([`${base}/v1/kv/a`]);
  });

  it('remembers the member that accepted a write and tries it first next time', async () => {
    const base = await startLeader();
    const dead = await closedBase();
    const client = new KvClient({
      endpoints: [dead, base],
      fetch: realFetch,
      sleep: async () => undefined,
    });
    await client.put('a', 1);
    expect(client.currentEndpoint).toBe(base);
    // Next time the remembered member is tried first, so no refused connection is made.
    expect((await client.get<number>('a'))?.value).toBe(1);
  });
});

describe('refused connections', () => {
  it('are reported as not sent, so a write may go to the next member', async () => {
    const dead = await closedBase();
    const client = new KvClient({ endpoints: [dead], fetch: realFetch, maxRetries: 0, sleep: async () => undefined });
    const error = await client.put('a', 1).catch((err: unknown) => err);
    expect(error).toBeInstanceOf(KvClientError);
    expect((error as KvClientError).notSent).toBe(true);
    expect((error as KvClientError).message).not.toMatch(/outcome is unknown/);
  });
});

describe('redirects to the leader', () => {
  it('follows a 307 to the leader and succeeds', async () => {
    const leader = await startLeader();
    const follower = await startFollower((_base, path) => `${leader}${path}`);
    const client = new KvClient({ endpoints: [follower], fetch: realFetch });
    await expect(client.put('a', 'via-redirect')).resolves.toMatchObject({ index: 1 });
    expect((await client.get<string>('a'))?.value).toBe('via-redirect');
  });

  it('gives up after too many redirects', async () => {
    const follower = await startFollower((base, path) => `${base}${path}`);
    const client = new KvClient({ endpoints: [follower], maxRedirects: 2, fetch: realFetch });
    await expect(client.put('a', 1)).rejects.toMatchObject({ code: 'REDIRECT_LOOP' });
  });

  it('reports a 307 without a location as unexpected', async () => {
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: ['http://127.0.0.1:9'],
      fetch: scripted([status(307, {}, '{}')], calls),
      sleep: async () => undefined,
    });
    await expect(client.put('a', 1)).rejects.toMatchObject({ code: 'UNEXPECTED' });
  });
});

describe('option and response validation', () => {
  it('requires at least one endpoint', () => {
    expect(() => new KvClient({ endpoints: [] })).toThrowError(KvClientError);
  });

  it('rejects negative numeric options', () => {
    expect(() => new KvClient({ endpoints: ['http://a'], timeoutMs: -1 })).toThrow(RangeError);
    expect(() => new KvClient({ endpoints: ['http://a'], maxRetries: 1.5 })).toThrow(RangeError);
  });

  it('reports a body that is not JSON as unexpected', async () => {
    const calls: string[] = [];
    const client = new KvClient({
      endpoints: ['http://127.0.0.1:9'],
      fetch: scripted([status(200, {}, 'not json')], calls),
    });
    await expect(client.get('a')).rejects.toMatchObject({ code: 'UNEXPECTED' });
  });
});
