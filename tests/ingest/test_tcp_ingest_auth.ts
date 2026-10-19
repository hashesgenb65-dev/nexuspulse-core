/**
 * Token checks on the TCP ingest: a connection must open with a writer token before any event is
 * read. Over real loopback sockets, with a real client transport and a real StreamEngine.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import { once } from 'node:events';
import { TokenAuth, type TokenSpec } from '../../src/api/auth';
import { createDefaultEngine, type StreamEngine, type TelemetryEvent } from '../../src/core/engine';
import {
  TcpIngestServer,
  encodeAuthRecord,
  encodeTelemetryRecord,
  decodeAuthRecord,
  AUTH_FIELD,
} from '../../src/ingest/tcp_ingest';
import { BinaryCodec, createClientTransport, FrameFlags, FrameType, type FrameTransport } from '../../src/grpc/transport';

const NOW = 1_700_000_000_000;
const clock = (): number => NOW;
const READER = 'reader-secret-0123456789abcd';
const WRITER = 'writer-secret-0123456789abcd';

const specs: TokenSpec[] = [
  { name: 'dash', role: 'reader', secret: READER },
  { name: 'edge', role: 'writer', secret: WRITER },
];

const cleanups: Array<() => void | Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function event(id: string): TelemetryEvent {
  return { id, source: 'edge-01', metric: 'cpu', value: 10, timestamp: NOW - 1_000 };
}

async function startIngest(auth?: TokenAuth): Promise<{ port: number; engine: StreamEngine; server: TcpIngestServer }> {
  const engine = createDefaultEngine({ clock });
  cleanups.push(() => {
    if (engine.metrics().state !== 'stopped') engine.stop();
  });
  const server = new TcpIngestServer({ engine, ...(auth === undefined ? {} : { auth }) });
  cleanups.push(() => server.close());
  const port = await server.listen(0, '127.0.0.1');
  return { port, engine, server };
}

async function connect(port: number): Promise<{ socket: net.Socket; transport: FrameTransport }> {
  const socket = net.connect({ host: '127.0.0.1', port });
  cleanups.push(() => {
    socket.destroy();
  });
  await once(socket, 'connect');
  return { socket, transport: createClientTransport(socket, { keepAliveIntervalMs: 0 }) };
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

/** Waits a short time so that any event that was going to be accepted has been. */
const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 150));

describe('auth record encoding', () => {
  it('round-trips a token through the auth record', () => {
    expect(decodeAuthRecord(encodeAuthRecord(WRITER))).toBe(WRITER);
  });

  it('does not read a telemetry record as a token', () => {
    const telemetry = encodeTelemetryRecord(event('evt-1'));
    expect(decodeAuthRecord(telemetry)).toBeUndefined();
  });

  it('returns undefined for bytes that are not a record at all', () => {
    expect(decodeAuthRecord(Buffer.from([0xff, 0xff, 0xff]))).toBeUndefined();
    expect(new BinaryCodec().decode(encodeAuthRecord('x'))[AUTH_FIELD]).toBe('x');
  });
});

describe('TCP ingest with a token store', () => {
  it('accepts events from a connection that opens with a writer token', async () => {
    const { port, server } = await startIngest(new TokenAuth(specs));
    const { transport } = await connect(port);

    sendData(transport, encodeAuthRecord(WRITER));
    sendData(transport, encodeTelemetryRecord(event('evt-1')));
    sendData(transport, encodeTelemetryRecord(event('evt-2')));

    await waitFor(() => server.stats().eventsAccepted === 2);
    expect(server.stats().authFailures).toBe(0);
  });

  it('closes a connection that opens with a reader token, before any event is taken', async () => {
    const { port, server } = await startIngest(new TokenAuth(specs));
    const { socket, transport } = await connect(port);
    const closed = once(socket, 'close');

    sendData(transport, encodeAuthRecord(READER));
    sendData(transport, encodeTelemetryRecord(event('evt-1')));
    await closed;

    expect(server.stats().authFailures).toBe(1);
    expect(server.stats().eventsAccepted).toBe(0);
  });

  it('closes a connection that opens with an unknown token', async () => {
    const { port, server } = await startIngest(new TokenAuth(specs));
    const { socket, transport } = await connect(port);
    const closed = once(socket, 'close');

    sendData(transport, encodeAuthRecord('not-a-real-token-0123456789abcdef'));
    await closed;

    expect(server.stats().authFailures).toBe(1);
  });

  it('closes a connection whose first frame is an event rather than a token', async () => {
    const { port, server } = await startIngest(new TokenAuth(specs));
    const { socket, transport } = await connect(port);
    const closed = once(socket, 'close');

    sendData(transport, encodeTelemetryRecord(event('evt-1')));
    await closed;
    await settle();

    expect(server.stats().authFailures).toBe(1);
    expect(server.stats().eventsAccepted).toBe(0);
  });

  it('counts each refused connection and keeps accepting valid ones', async () => {
    const { port, server } = await startIngest(new TokenAuth(specs));

    const bad = await connect(port);
    const badClosed = once(bad.socket, 'close');
    sendData(bad.transport, encodeAuthRecord(READER));
    await badClosed;

    const good = await connect(port);
    sendData(good.transport, encodeAuthRecord(WRITER));
    sendData(good.transport, encodeTelemetryRecord(event('evt-1')));

    await waitFor(() => server.stats().eventsAccepted === 1);
    expect(server.stats().authFailures).toBe(1);
  });
});

describe('TCP ingest without a token store', () => {
  it('accepts events from a connection that sends no token, as before', async () => {
    const { port, server } = await startIngest();
    const { transport } = await connect(port);

    sendData(transport, encodeTelemetryRecord(event('evt-1')));

    await waitFor(() => server.stats().eventsAccepted === 1);
    expect(server.stats().authFailures).toBe(0);
  });
});
