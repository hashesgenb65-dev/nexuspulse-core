/**
 * @file tests/ingest/test_tcp_ingest.ts
 * @description End-to-end tests for /src/ingest/tcp_ingest.ts over real TCP
 * sockets on the loopback interface, with a real client transport and a real
 * StreamEngine. These are the first tests that exercise the transport over an
 * actual socket.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import { once } from 'node:events';
import { BackpressureError, createDefaultEngine, type StreamEngine, type TelemetryEvent } from '../../src/core/engine.js';
import {
  TcpIngestServer,
  encodeTelemetryRecord,
  decodeTelemetryRecord,
  EVENT_FIELD,
  DEFAULT_MAX_CONNECTIONS,
  DEFAULT_IDLE_TIMEOUT_MS,
} from '../../src/ingest/tcp_ingest.js';
import {
  BinaryCodec,
  createClientTransport,
  FrameFlags,
  FrameType,
  type FrameTransport,
} from '../../src/grpc/transport.js';

const NOW = 1_700_000_000_000;
const clock = (): number => NOW;

const servers: TcpIngestServer[] = [];
const sockets: net.Socket[] = [];
const engines: StreamEngine[] = [];

function event(id: string, value = 10, source = 'edge-01', metric = 'cpu'): TelemetryEvent {
  return { id, source, metric, value, timestamp: NOW - 1_000 };
}

function makeServer(
  options: { engineConfig?: Parameters<typeof createDefaultEngine>[0]; maxConnections?: number; idleTimeoutMs?: number } = {},
): { server: TcpIngestServer; engine: StreamEngine } {
  const engine = createDefaultEngine({ clock, ...(options.engineConfig ?? {}) });
  engines.push(engine);
  const server = new TcpIngestServer({
    engine,
    ...(options.maxConnections === undefined ? {} : { maxConnections: options.maxConnections }),
    ...(options.idleTimeoutMs === undefined ? {} : { idleTimeoutMs: options.idleTimeoutMs }),
  });
  servers.push(server);
  return { server, engine };
}

interface Client {
  readonly socket: net.Socket;
  readonly transport: FrameTransport;
}

/** Opens a raw TCP connection and wraps it in a client transport with keep-alive off. */
async function connect(port: number): Promise<Client> {
  const socket = net.connect({ host: '127.0.0.1', port });
  sockets.push(socket);
  await once(socket, 'connect');
  const transport = createClientTransport(socket, { keepAliveIntervalMs: 0 });
  return { socket, transport };
}

/** Sends one event as its own stream, the way a real client would. */
function sendEvent(transport: FrameTransport, ev: TelemetryEvent): void {
  const streamId = transport.openStream();
  transport.sendFrame(FrameType.DATA, FrameFlags.END_STREAM, streamId, encodeTelemetryRecord(ev));
}

/** Polls until a condition holds, failing after the timeout. */
async function waitFor(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

afterEach(async () => {
  for (const socket of sockets.splice(0)) socket.destroy();
  for (const server of servers.splice(0)) await server.close();
  for (const engine of engines.splice(0)) {
    if (engine.metrics().state !== 'stopped') engine.stop();
  }
});

// ---------------------------------------------------------------------------
// Wire encoding
// ---------------------------------------------------------------------------

describe('telemetry record encoding', () => {
  it('round-trips an event through the record format', () => {
    const ev = event('evt-1', 12.5);
    const raw = decodeTelemetryRecord(encodeTelemetryRecord(ev));
    expect(raw).toEqual({ id: 'evt-1', source: 'edge-01', metric: 'cpu', value: 12.5, timestamp: NOW - 1_000 });
  });

  it('encodes the same event to the same bytes', () => {
    expect(encodeTelemetryRecord(event('same'))).toEqual(encodeTelemetryRecord(event('same')));
  });

  it('preserves negative and fractional values', () => {
    const raw = decodeTelemetryRecord(encodeTelemetryRecord(event('neg', -0.25)));
    expect(raw['value']).toBe(-0.25);
  });

  it('uses the documented field IDs', () => {
    expect(EVENT_FIELD).toEqual({ ID: 1, SOURCE: 2, METRIC: 3, VALUE: 4, TIMESTAMP: 5 });
  });

  it('leaves a field absent from the decoded object when the record does not carry it', () => {
    const payload = new BinaryCodec().encode({ [EVENT_FIELD.ID]: 'only-id' });
    expect(decodeTelemetryRecord(payload)).toEqual({ id: 'only-id' });
  });
});

// ---------------------------------------------------------------------------
// Construction and options
// ---------------------------------------------------------------------------

describe('construction and options', () => {
  it('exposes defaults', () => {
    expect(DEFAULT_MAX_CONNECTIONS).toBe(64);
    expect(DEFAULT_IDLE_TIMEOUT_MS).toBe(60_000);
  });

  it.each([0, -1, 1.5, 10_001])('rejects maxConnections %s', (maxConnections) => {
    const engine = createDefaultEngine({ clock });
    engines.push(engine);
    expect(() => new TcpIngestServer({ engine, maxConnections })).toThrow(RangeError);
  });

  it.each([0, 1.5, 86_400_001])('rejects idleTimeoutMs %s', (idleTimeoutMs) => {
    const engine = createDefaultEngine({ clock });
    engines.push(engine);
    expect(() => new TcpIngestServer({ engine, idleTimeoutMs })).toThrow(RangeError);
  });

  it('has no port before listening, and a port once bound', async () => {
    const { server } = makeServer();
    expect(server.port).toBeUndefined();
    const port = await server.listen(0);
    expect(server.port).toBe(port);
    expect(port).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// Ingest over TCP
// ---------------------------------------------------------------------------

describe('ingest over TCP', () => {
  it('delivers valid events from a real client to the engine', async () => {
    const { server, engine } = makeServer();
    const port = await server.listen(0);
    const client = await connect(port);

    for (let i = 0; i < 3; i++) sendEvent(client.transport, event(`e${i}`, 10 + i));

    await waitFor(() => engine.metrics().accepted === 3);
    expect(server.stats()).toMatchObject({ framesReceived: 3, eventsAccepted: 3, eventsRejected: 0 });
  });

  it('counts an invalid event as rejected and keeps the connection usable', async () => {
    const { server, engine } = makeServer();
    const port = await server.listen(0);
    const client = await connect(port);

    sendEvent(client.transport, { ...event('bad'), value: 'not-a-number' } as unknown as TelemetryEvent);
    sendEvent(client.transport, event('good'));

    await waitFor(() => server.stats().eventsAccepted === 1);
    expect(server.stats()).toMatchObject({ eventsRejected: 1, connectionsOpen: 1 });
    expect(engine.metrics().rejectedInvalid).toBe(1);
  });

  it('counts a malformed payload and keeps the connection open', async () => {
    const { server } = makeServer();
    const port = await server.listen(0);
    const client = await connect(port);

    const streamId = client.transport.openStream();
    client.transport.sendFrame(FrameType.DATA, FrameFlags.END_STREAM, streamId, Buffer.from([0xff, 0xff, 0xff]));
    sendEvent(client.transport, event('after-garbage'));

    await waitFor(() => server.stats().eventsAccepted === 1);
    expect(server.stats().malformedFrames).toBe(1);
    expect(server.stats().connectionsOpen).toBe(1);
  });

  it('ignores frames that are not DATA', async () => {
    const { server } = makeServer();
    const port = await server.listen(0);
    const client = await connect(port);

    const streamId = client.transport.openStream();
    client.transport.sendFrame(FrameType.HEADERS, 0, streamId, Buffer.alloc(0));
    sendEvent(client.transport, event('only-data-counts'));

    await waitFor(() => server.stats().eventsAccepted === 1);
    expect(server.stats().framesReceived).toBe(1);
  });

  it('reports queue-full drops under the reject policy without closing the connection', async () => {
    const { server, engine } = makeServer({ engineConfig: { queueCapacity: 1, backpressure: 'reject' } });
    const port = await server.listen(0);
    const client = await connect(port);

    sendEvent(client.transport, event('q1'));
    sendEvent(client.transport, event('q2'));

    await waitFor(() => server.stats().framesReceived === 2);
    expect(server.stats()).toMatchObject({ eventsAccepted: 1, eventsDroppedQueueFull: 1, connectionsOpen: 1 });
    expect(engine.metrics().droppedQueueFull).toBe(1);
  });

  it('handles several concurrent clients', async () => {
    const { server, engine } = makeServer();
    const port = await server.listen(0);
    const clients = await Promise.all([connect(port), connect(port), connect(port)]);

    for (const [index, client] of clients.entries()) {
      for (let i = 0; i < 20; i++) sendEvent(client.transport, event(`c${index}-${i}`, i, `edge-${index}`));
    }

    await waitFor(() => engine.metrics().accepted === 60);
    expect(server.stats()).toMatchObject({ connectionsAccepted: 3, connectionsOpen: 3, eventsAccepted: 60 });
  });

  it('accepts a larger burst within one connection', async () => {
    const { server, engine } = makeServer({ engineConfig: { queueCapacity: 1_000 } });
    const port = await server.listen(0);
    const client = await connect(port);

    for (let i = 0; i < 300; i++) sendEvent(client.transport, event(`b${i}`, i % 100));

    await waitFor(() => engine.metrics().accepted === 300);
    expect(server.stats().eventsAccepted).toBe(300);
  });
});

// ---------------------------------------------------------------------------
// Connection limits and idle timeouts
// ---------------------------------------------------------------------------

describe('connection limits', () => {
  it('closes connections beyond maxConnections at once', async () => {
    const { server } = makeServer({ maxConnections: 2 });
    const port = await server.listen(0);
    await connect(port);
    await connect(port);
    await waitFor(() => server.stats().connectionsOpen === 2);

    const third = await connect(port);
    await once(third.socket, 'close');

    expect(server.stats()).toMatchObject({ connectionsAccepted: 2, connectionsRejected: 1, connectionsOpen: 2 });
  });

  it('frees a slot when a client disconnects', async () => {
    const { server } = makeServer({ maxConnections: 1 });
    const port = await server.listen(0);
    const first = await connect(port);
    await waitFor(() => server.stats().connectionsOpen === 1);

    first.socket.destroy();
    await waitFor(() => server.stats().connectionsOpen === 0);

    await connect(port);
    await waitFor(() => server.stats().connectionsOpen === 1);
    expect(server.stats().connectionsRejected).toBe(0);
  });
});

describe('idle timeout', () => {
  it('closes a connection that sends nothing for idleTimeoutMs', async () => {
    const { server } = makeServer({ idleTimeoutMs: 100 });
    const port = await server.listen(0);
    const client = await connect(port);

    await once(client.socket, 'close');
    await waitFor(() => server.stats().connectionsOpen === 0);
    expect(server.stats().idleClosed).toBe(1);
  });

  it('a connection that keeps sending stays open', async () => {
    const { server, engine } = makeServer({ idleTimeoutMs: 400 });
    const port = await server.listen(0);
    const client = await connect(port);

    for (let i = 0; i < 6; i++) {
      sendEvent(client.transport, event(`keep-${i}`));
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    expect(server.stats()).toMatchObject({ idleClosed: 0, connectionsOpen: 1 });
    expect(engine.metrics().accepted).toBe(6);
  });
});

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

describe('close', () => {
  it('closes the listener and every open connection', async () => {
    const { server } = makeServer();
    const port = await server.listen(0);
    const client = await connect(port);
    await waitFor(() => server.stats().connectionsOpen === 1);

    await server.close();

    await once(client.socket, 'close');
    expect(server.port).toBeUndefined();
    expect(server.stats().connectionsOpen).toBe(0);
  });

  it('is safe to call twice, and safe before listening', async () => {
    const { server } = makeServer();
    await expect(server.close()).resolves.toBeUndefined();
    await server.listen(0);
    await server.close();
    await expect(server.close()).resolves.toBeUndefined();
  });

  it('stops accepting new connections after close', async () => {
    const { server } = makeServer();
    const port = await server.listen(0);
    await server.close();

    const attempt = net.connect({ host: '127.0.0.1', port });
    sockets.push(attempt);
    const [error] = (await once(attempt, 'error')) as [NodeJS.ErrnoException];
    expect(error.code).toBe('ECONNREFUSED');
  });

  it('a listen on a port already in use rejects', async () => {
    const first = makeServer();
    const port = await first.server.listen(0);
    const second = makeServer();
    await expect(second.server.listen(port)).rejects.toMatchObject({ code: 'EADDRINUSE' });
  });
});

// ---------------------------------------------------------------------------
// submit wrapper
// ---------------------------------------------------------------------------

describe('submit wrapper', () => {
  function serverWithSubmit(
    submit: (raw: unknown) => { accepted: boolean },
  ): { server: TcpIngestServer } {
    const engine = createDefaultEngine({ clock });
    engines.push(engine);
    const server = new TcpIngestServer({ engine, submit: submit as never });
    servers.push(server);
    return { server };
  }

  it('receives every decoded record, and the engine validates it', async () => {
    const seen: unknown[] = [];
    const engine = createDefaultEngine({ clock });
    engines.push(engine);
    const server = new TcpIngestServer({
      engine,
      submit: (raw) => {
        seen.push(raw);
        return engine.submit(raw);
      },
    });
    servers.push(server);
    const port = await server.listen(0);
    const client = await connect(port);

    sendEvent(client.transport, event('w1', 42, 'edge-7', 'mem'));

    await waitFor(() => server.stats().eventsAccepted === 1);
    expect(seen).toEqual([{ id: 'w1', source: 'edge-7', metric: 'mem', value: 42, timestamp: NOW - 1_000 }]);
  });

  it('a rejected result is counted as rejected', async () => {
    const { server } = serverWithSubmit(() => ({ accepted: false }));
    const port = await server.listen(0);
    const client = await connect(port);

    sendEvent(client.transport, event('r1'));

    await waitFor(() => server.stats().eventsRejected === 1);
    expect(server.stats().eventsAccepted).toBe(0);
  });

  it('a throwing wrapper is counted as an internal error and does not break ingest', async () => {
    const { server } = serverWithSubmit(() => {
      throw new Error('wrapper broke');
    });
    const port = await server.listen(0);
    const client = await connect(port);

    sendEvent(client.transport, event('a'));
    sendEvent(client.transport, event('b'));

    await waitFor(() => server.stats().internalErrors === 2);
    expect(server.stats()).toMatchObject({ connectionsOpen: 1, eventsAccepted: 0 });
  });

  it('a BackpressureError from the wrapper counts as a queue-full drop', async () => {
    const { server } = serverWithSubmit(() => {
      throw new BackpressureError('full', { capacity: 1 });
    });
    const port = await server.listen(0);
    const client = await connect(port);

    sendEvent(client.transport, event('full'));

    await waitFor(() => server.stats().eventsDroppedQueueFull === 1);
    expect(server.stats().internalErrors).toBe(0);
  });
});
