/**
 * Rate limiting on the key-value API, keyed by token name. Requests over the limit get a 429 with
 * `retry-after`. Requests that fail authentication do not use up a token.
 */

import { describe, it, expect } from 'vitest';
import { TokenAuth, type TokenSpec } from '../../src/api/auth';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KvApi } from '../../src/mesh/kv_api';
import { CommitApplier, KeyValueStore } from '../../src/mesh/state_machine';
import { DistributedRateLimiterEngine } from '../../src/ratelimit/limiter';

const ALPHA = 'alpha-secret-0123456789abcdefgh';
const BETA = 'beta-secret-0123456789abcdefghi';
const specs: TokenSpec[] = [
  { name: 'alpha', role: 'reader', secret: ALPHA },
  { name: 'beta', role: 'reader', secret: BETA },
];

function apiWith(options: { auth?: TokenAuth; rateLimiter?: DistributedRateLimiterEngine }): KvApi {
  const node = new RaftConsensusNode({
    nodeId: 'node-1',
    clusterPeers: ['node-1'],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
  const store = new KeyValueStore();
  return new KvApi({
    node,
    store,
    applier: new CommitApplier(node, store),
    peerHttp: new Map(),
    ...(options.auth === undefined ? {} : { auth: options.auth }),
    ...(options.rateLimiter === undefined ? {} : { rateLimiter: options.rateLimiter }),
  });
}

function clock(): { now: () => number; advance: (ms: number) => void } {
  let t = 1_000_000;
  return { now: () => t, advance: (ms) => { t += ms; } };
}

const get = (authorization: string | undefined): { method: string; url: string; authorization?: string } => ({
  method: 'GET',
  url: '/v1/status',
  ...(authorization === undefined ? {} : { authorization }),
});

describe('key-value API rate limiting', () => {
  it('answers 429 with retry-after once a token has used up its bucket', async () => {
    const time = clock();
    const limiter = new DistributedRateLimiterEngine({ tierName: 'T', capacity: 2, refillRatePerSec: 1 }, { now: time.now });
    const api = apiWith({ auth: new TokenAuth(specs), rateLimiter: limiter });

    expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(200);
    expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(200);

    const limited = await api.handle(get(`Bearer ${ALPHA}`));
    expect(limited.status).toBe(429);
    expect(limited.headers['retry-after']).toBe('1');
    expect(limited.body).toEqual({ error: 'rate limit exceeded' });
  });

  it('limits each token separately', async () => {
    const time = clock();
    const limiter = new DistributedRateLimiterEngine({ tierName: 'T', capacity: 1, refillRatePerSec: 1 }, { now: time.now });
    const api = apiWith({ auth: new TokenAuth(specs), rateLimiter: limiter });

    expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(200);
    expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(429);
    expect((await api.handle(get(`Bearer ${BETA}`))).status).toBe(200);
  });

  it('lets a token through again after it refills', async () => {
    const time = clock();
    const limiter = new DistributedRateLimiterEngine({ tierName: 'T', capacity: 1, refillRatePerSec: 1 }, { now: time.now });
    const api = apiWith({ auth: new TokenAuth(specs), rateLimiter: limiter });

    expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(200);
    expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(429);
    time.advance(1_000);
    expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(200);
  });

  it('does not use tokens for requests that fail authentication', async () => {
    const time = clock();
    const limiter = new DistributedRateLimiterEngine({ tierName: 'T', capacity: 1, refillRatePerSec: 1 }, { now: time.now });
    const api = apiWith({ auth: new TokenAuth(specs), rateLimiter: limiter });

    for (let i = 0; i < 5; i++) {
      expect((await api.handle(get(undefined))).status).toBe(401);
    }
    expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(200);
  });

  it('does not limit requests when there is no token store', async () => {
    const time = clock();
    const limiter = new DistributedRateLimiterEngine({ tierName: 'T', capacity: 1, refillRatePerSec: 1 }, { now: time.now });
    const api = apiWith({ rateLimiter: limiter });

    for (let i = 0; i < 5; i++) {
      expect((await api.handle(get(undefined))).status).toBe(200);
    }
  });

  it('does not limit when no rate limiter is configured', async () => {
    const api = apiWith({ auth: new TokenAuth(specs) });
    for (let i = 0; i < 20; i++) {
      expect((await api.handle(get(`Bearer ${ALPHA}`))).status).toBe(200);
    }
  });
});
