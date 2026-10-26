/**
 * Rate limiting on the TCP ingest, keyed by the token that opened each connection. Real loopback
 * sockets carry the frames. A fake clock keeps refill deterministic.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import { once } from 'node:events';
import { TokenAuth, type TokenSpec } from '../../src/api/auth';
import { createDefaultEngine, type TelemetryEvent } from '../../src/core/engine';
import { TcpIngestServer, encodeAuthRecord, encodeTelemetryRecord } from '../../src/ingest/tcp_ingest';
import { createClientTransport, FrameFlags, FrameType, type FrameTransport } from '../../src/grpc/transport';
import { DistributedRateLimiterEngine } from '../../src/ratelimit/limiter';

const NOW = 1_700_000_000_000;
const ALPHA = 'alpha-writer-0123456789abcdefgh';
const BETA = 'beta-writer-0123456789abcdefghi';
const specs: TokenSpec[] = [
  { name: 'alpha', role: 'writer', secret: ALPHA },
  { name: 'beta', role: 'writer', secret: BETA },
];

const cleanups: Array<() => void | Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function event(id: string): TelemetryEvent {
  return { id, source: 'edge-01', metric: 'cpu', value: 10, timestamp: NOW - 1_000 };
}

function limiter(capacity: number): DistributedRateLimiterEngine {
  return new DistributedRateLimiterEngine(
    { tierName: 'T', capacity, refillRatePerSec: 0.000001 },
    { now: () => NOW },
  );
}

async function start(options: { auth?: boolean; rateLimiter?: DistributedRateLimiterEngine }): Promise<{
  port: number;
  server: TcpIngestServer;
}> {
  const engine = createDefaultEngine({ clock: () => NOW });
  cleanups.push(() => {
    if (engine.metrics().state !== 'stopped') engine.stop();
  });
  const server = new TcpIngestServer({
    engine,
    ...(options.auth === false ? {} : { auth: new TokenAuth(specs) }),
    ...(options.rateLimiter === undefined ? {} : { rateLimiter: options.rateLimiter }),
  });
  cleanups.push(() => server.close());
  const port = await server.listen(0, '127.0.0.1');
  return { port, server };
}

async function connect(port: number, token: string | undefined): Promise<FrameTransport> {
  const socket = net.connect({ host: '127.0.0.1', port });
  cleanups.push(() => {
    socket.destroy();
  });
  await once(socket, 'connect');
  const transport = createClientTransport(socket, { keepAliveIntervalMs: 0 });
  if (token !== undefined) sendData(transport, encodeAuthRecord(token));
  return transport;
}

function sendData(transport: FrameTransport, payload: Buffer): void {
  const streamId = transport.openStream();
  transport.sendFrame(FrameType.DATA, FrameFlags.END_STREAM, streamId, payload);
}

async function waitFor(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

describe('TCP ingest rate limiting', () => {
  it('accepts events up to the token limit and drops the rest', async () => {
    const { port, server } = await start({ rateLimiter: limiter(2) });
    const transport = await connect(port, ALPHA);
    for (let i = 0; i < 5; i++) sendData(transport, encodeTelemetryRecord(event(`evt-${i}`)));

    await waitFor(() => server.stats().eventsAccepted + server.stats().eventsRateLimited === 5);
    expect(server.stats().eventsAccepted).toBe(2);
    expect(server.stats().eventsRateLimited).toBe(3);
  });

  it('limits each token separately', async () => {
    const { port, server } = await start({ rateLimiter: limiter(1) });
    const alpha = await connect(port, ALPHA);
    const beta = await connect(port, BETA);

    sendData(alpha, encodeTelemetryRecord(event('a-1')));
    sendData(alpha, encodeTelemetryRecord(event('a-2')));
    sendData(beta, encodeTelemetryRecord(event('b-1')));

    await waitFor(() => server.stats().eventsAccepted + server.stats().eventsRateLimited === 3);
    expect(server.stats().eventsAccepted).toBe(2);
    expect(server.stats().eventsRateLimited).toBe(1);
  });

  it('shares one token across connections, so a second connection cannot bypass the limit', async () => {
    const { port, server } = await start({ rateLimiter: limiter(2) });
    const first = await connect(port, ALPHA);
    const second = await connect(port, ALPHA);

    sendData(first, encodeTelemetryRecord(event('f-1')));
    sendData(first, encodeTelemetryRecord(event('f-2')));
    sendData(second, encodeTelemetryRecord(event('s-1')));

    await waitFor(() => server.stats().eventsAccepted + server.stats().eventsRateLimited === 3);
    expect(server.stats().eventsAccepted).toBe(2);
    expect(server.stats().eventsRateLimited).toBe(1);
  });

  it('does not limit anything when no rate limiter is configured', async () => {
    const { port, server } = await start({});
    const transport = await connect(port, ALPHA);
    for (let i = 0; i < 10; i++) sendData(transport, encodeTelemetryRecord(event(`evt-${i}`)));

    await waitFor(() => server.stats().eventsAccepted === 10);
    expect(server.stats().eventsRateLimited).toBe(0);
  });

  it('does not limit connections when there is no token store', async () => {
    const { port, server } = await start({ auth: false, rateLimiter: limiter(1) });
    const transport = await connect(port, undefined);
    for (let i = 0; i < 4; i++) sendData(transport, encodeTelemetryRecord(event(`evt-${i}`)));

    await waitFor(() => server.stats().eventsAccepted === 4);
    expect(server.stats().eventsRateLimited).toBe(0);
  });
});
