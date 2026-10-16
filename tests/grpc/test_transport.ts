/**
 * @file tests/grpc/test_transport.ts
 * @description Exhaustive Vitest suite for /src/grpc/transport.ts.
 *
 * ## Test strategy
 *
 * Tests are grouped into:
 * 1. **Frame builder/parser** — deterministic round-trips, boundary values,
 *    error cases.
 * 2. **FrameParser (streaming)** — chunked delivery, multi-frame batches,
 *    partial header, bad magic.
 * 3. **BinaryCodec** — every FieldType, round-trips, truncation, bad types.
 * 4. **FrameTransport** — using an in-process PassThrough pair as the duplex
 *    stream; tests cover: attach, sendFrame, flow control, stream lifecycle,
 *    PING/PONG with fixed clock, GOAWAY, error propagation, metrics.
 */

import { Duplex } from 'node:stream';
import { describe, it, expect } from 'vitest';
import {
  FRAME_MAGIC,
  PROTOCOL_VERSION,
  HEADER_BYTES,
  MAX_PAYLOAD_BYTES,
  DEFAULT_WINDOW_SIZE,
  FrameType,
  FrameFlags,
  FieldType,
  TransportError,
  TransportClosedError,
  FrameProtocolError,
  FlowControlError,
  CodecError,
  buildFrame,
  parseFrame,
  FrameParser,
  BinaryCodec,
  FrameTransport,
  createClientTransport,
  createServerTransport,
  resolveTransportConfig,
  DEFAULT_TRANSPORT_CONFIG,
  type Frame,
  type CodableRecord,
} from '../../src/grpc/transport.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Creates a single-ended sink stream for unit tests. Writes are discarded
 * (never echoed back to the readable side), and the test injects inbound
 * bytes with `sink.push(...)`. A plain PassThrough must NOT be used here:
 * once the transport attaches a 'data' listener, a PassThrough re-emits what
 * was written to it, which makes the transport read its own outbound frames.
 */
function makeSink(): Duplex {
  return new Duplex({
    read(): void { /* inbound bytes are injected via push() */ },
    write(_chunk, _enc, cb): void { cb(); },
  });
}

/**
 * Creates a cross-wired duplex pair simulating a socket pair.
 * Bytes written to `clientStream` are readable from `serverStream` and vice
 * versa. This is NOT a pipe loop: each direction is consumed exactly once,
 * so no data echoes back to its sender.
 */
function makePair(): { clientStream: Duplex; serverStream: Duplex } {
  const clientStream: Duplex = new Duplex({
    read(): void { /* data is pushed by the peer's write() */ },
    write(chunk: Buffer, _enc, cb): void {
      serverStream.push(chunk);
      cb();
    },
    final(cb): void {
      serverStream.push(null);
      cb();
    },
  });
  const serverStream: Duplex = new Duplex({
    read(): void { /* data is pushed by the peer's write() */ },
    write(chunk: Buffer, _enc, cb): void {
      clientStream.push(chunk);
      cb();
    },
    final(cb): void {
      clientStream.push(null);
      cb();
    },
  });
  return { clientStream, serverStream };
}

const FIXED_NOW = 1_760_000_000_000;
const fixedClock = (): number => FIXED_NOW;

// ---------------------------------------------------------------------------
// buildFrame / parseFrame
// ---------------------------------------------------------------------------

describe('buildFrame', () => {
  it('produces a buffer of HEADER_BYTES + payload length', () => {
    const payload = Buffer.from('hello');
    const buf = buildFrame(FrameType.DATA, FrameFlags.NONE, 1, payload);
    expect(buf.length).toBe(HEADER_BYTES + payload.length);
  });

  it('encodes magic correctly', () => {
    const buf = buildFrame(FrameType.DATA, FrameFlags.NONE, 1, Buffer.alloc(0));
    expect(buf.readUInt32BE(0)).toBe(FRAME_MAGIC);
  });

  it('encodes version correctly', () => {
    const buf = buildFrame(FrameType.DATA, FrameFlags.NONE, 1, Buffer.alloc(0));
    expect(buf.readUInt8(4)).toBe(PROTOCOL_VERSION);
  });

  it('encodes frame type', () => {
    const buf = buildFrame(FrameType.PING, FrameFlags.NONE, 0, Buffer.alloc(0));
    expect(buf.readUInt8(5)).toBe(FrameType.PING);
  });

  it('encodes flags', () => {
    const buf = buildFrame(FrameType.DATA, FrameFlags.END_STREAM, 3, Buffer.alloc(0));
    expect(buf.readUInt16BE(6)).toBe(FrameFlags.END_STREAM);
  });

  it('encodes stream ID', () => {
    const buf = buildFrame(FrameType.DATA, FrameFlags.NONE, 0xdeadbeef, Buffer.alloc(0));
    expect(buf.readUInt32BE(8)).toBe(0xdeadbeef);
  });

  it('encodes payload length', () => {
    const payload = Buffer.alloc(100, 0xab);
    const buf = buildFrame(FrameType.DATA, FrameFlags.NONE, 1, payload);
    expect(buf.readUInt32BE(12)).toBe(100);
  });

  it('payload bytes are present', () => {
    const payload = Buffer.from([1, 2, 3]);
    const buf = buildFrame(FrameType.DATA, FrameFlags.NONE, 1, payload);
    expect(buf.subarray(HEADER_BYTES).equals(payload)).toBe(true);
  });

  it('accepts empty payload', () => {
    expect(() =>
      buildFrame(FrameType.PING, FrameFlags.NONE, 0, Buffer.alloc(0)),
    ).not.toThrow();
  });

  it('throws FrameProtocolError for oversized payload', () => {
    const big = Buffer.alloc(MAX_PAYLOAD_BYTES + 1);
    expect(() => buildFrame(FrameType.DATA, FrameFlags.NONE, 1, big)).toThrow(
      FrameProtocolError,
    );
  });
});

describe('parseFrame', () => {
  it('round-trips DATA frame', () => {
    const payload = Buffer.from('round-trip');
    const buf = buildFrame(FrameType.DATA, FrameFlags.END_STREAM, 7, payload);
    const frame = parseFrame(buf);
    expect(frame.magic).toBe(FRAME_MAGIC);
    expect(frame.type).toBe(FrameType.DATA);
    expect(frame.flags).toBe(FrameFlags.END_STREAM);
    expect(frame.streamId).toBe(7);
    expect(frame.payload.equals(payload)).toBe(true);
  });

  it('throws on buffer shorter than HEADER_BYTES', () => {
    expect(() => parseFrame(Buffer.alloc(4))).toThrow(FrameProtocolError);
  });

  it('throws on bad magic', () => {
    const buf = buildFrame(FrameType.DATA, 0, 1, Buffer.alloc(0));
    buf.writeUInt32BE(0xdeadbeef, 0); // corrupt magic
    expect(() => parseFrame(buf)).toThrow(FrameProtocolError);
  });

  it('throws on unsupported version', () => {
    const buf = buildFrame(FrameType.DATA, 0, 1, Buffer.alloc(0));
    buf.writeUInt8(0xff, 4); // bad version
    expect(() => parseFrame(buf)).toThrow(FrameProtocolError);
  });

  it('throws when buffer shorter than header + payload', () => {
    const buf = buildFrame(FrameType.DATA, 0, 1, Buffer.alloc(10));
    // Truncate to less than full frame
    expect(() => parseFrame(buf.subarray(0, HEADER_BYTES + 5))).toThrow(
      FrameProtocolError,
    );
  });

  it('throws when declared payload length exceeds MAX', () => {
    const buf = Buffer.allocUnsafe(HEADER_BYTES);
    buf.writeUInt32BE(FRAME_MAGIC, 0);
    buf.writeUInt8(PROTOCOL_VERSION, 4);
    buf.writeUInt8(FrameType.DATA, 5);
    buf.writeUInt16BE(0, 6);
    buf.writeUInt32BE(1, 8);
    buf.writeUInt32BE(MAX_PAYLOAD_BYTES + 1, 12); // oversized
    expect(() => parseFrame(buf)).toThrow(FrameProtocolError);
  });
});

// ---------------------------------------------------------------------------
// FrameParser (streaming)
// ---------------------------------------------------------------------------

describe('FrameParser', () => {
  it('parses a single complete frame', () => {
    const parser = new FrameParser();
    const buf = buildFrame(FrameType.DATA, 0, 1, Buffer.from('one'));
    const frames = parser.push(buf);
    expect(frames).toHaveLength(1);
    expect(frames[0]?.type).toBe(FrameType.DATA);
  });

  it('parses multiple frames in one chunk', () => {
    const parser = new FrameParser();
    const f1 = buildFrame(FrameType.DATA, 0, 1, Buffer.from('a'));
    const f2 = buildFrame(FrameType.PING, 0, 0, Buffer.alloc(0));
    const combined = Buffer.concat([f1, f2]);
    const frames = parser.push(combined);
    expect(frames).toHaveLength(2);
    expect(frames[0]?.type).toBe(FrameType.DATA);
    expect(frames[1]?.type).toBe(FrameType.PING);
  });

  it('reassembles frame split across two chunks', () => {
    const parser = new FrameParser();
    const full = buildFrame(FrameType.DATA, 0, 1, Buffer.from('split-me'));
    const mid = Math.floor(full.length / 2);
    const frames1 = parser.push(full.subarray(0, mid));
    expect(frames1).toHaveLength(0);
    const frames2 = parser.push(full.subarray(mid));
    expect(frames2).toHaveLength(1);
    expect(frames2[0]?.payload.toString()).toBe('split-me');
  });

  it('handles header delivered one byte at a time', () => {
    const parser = new FrameParser();
    const full = buildFrame(FrameType.PING, 0, 0, Buffer.alloc(0));
    for (let i = 0; i < full.length - 1; i++) {
      expect(parser.push(full.subarray(i, i + 1))).toHaveLength(0);
    }
    const last = parser.push(full.subarray(full.length - 1));
    expect(last).toHaveLength(1);
  });

  it('bufferedBytes reflects leftover data', () => {
    const parser = new FrameParser();
    const full = buildFrame(FrameType.DATA, 0, 1, Buffer.from('trailing'));
    parser.push(full.subarray(0, HEADER_BYTES - 1));
    expect(parser.bufferedBytes).toBe(HEADER_BYTES - 1);
  });

  it('reset() clears buffer', () => {
    const parser = new FrameParser();
    parser.push(Buffer.from([0x4e, 0x50]));
    expect(parser.bufferedBytes).toBe(2);
    parser.reset();
    expect(parser.bufferedBytes).toBe(0);
  });

  it('throws FrameProtocolError on bad magic mid-stream', () => {
    const parser = new FrameParser();
    const bad = Buffer.allocUnsafe(HEADER_BYTES);
    bad.writeUInt32BE(0xdeadbeef, 0);
    bad.writeUInt8(PROTOCOL_VERSION, 4);
    bad.writeUInt8(FrameType.DATA, 5);
    bad.writeUInt16BE(0, 6);
    bad.writeUInt32BE(1, 8);
    bad.writeUInt32BE(0, 12);
    expect(() => parser.push(bad)).toThrow(FrameProtocolError);
  });

  it('parses 100 frames delivered as one giant chunk', () => {
    const parser = new FrameParser();
    const frames = Array.from({ length: 100 }, (_, i) =>
      buildFrame(FrameType.DATA, 0, i + 1, Buffer.from(`frame-${i}`)),
    );
    const blob = Buffer.concat(frames);
    const parsed = parser.push(blob);
    expect(parsed).toHaveLength(100);
    parsed.forEach((f, i) => {
      expect(f.streamId).toBe(i + 1);
      expect(f.payload.toString()).toBe(`frame-${i}`);
    });
  });
});

// ---------------------------------------------------------------------------
// BinaryCodec
// ---------------------------------------------------------------------------

describe('BinaryCodec', () => {
  const codec = new BinaryCodec();

  it('encodes and decodes null', () => {
    const rec: CodableRecord = { 0: null };
    const decoded = codec.decode(codec.encode(rec));
    expect(decoded[0]).toBeNull();
  });

  it('encodes and decodes boolean true', () => {
    const decoded = codec.decode(codec.encode({ 1: true }));
    expect(decoded[1]).toBe(true);
  });

  it('encodes and decodes boolean false', () => {
    const decoded = codec.decode(codec.encode({ 1: false }));
    expect(decoded[1]).toBe(false);
  });

  it('encodes and decodes uint32 zero', () => {
    const decoded = codec.decode(codec.encode({ 2: 0 }));
    expect(decoded[2]).toBe(0);
  });

  it('encodes and decodes uint32 max', () => {
    const decoded = codec.decode(codec.encode({ 2: 0xffff_ffff }));
    expect(decoded[2]).toBe(0xffff_ffff);
  });

  it('encodes integers ≤ 0xffffffff as UINT32 not FLOAT64', () => {
    const buf = codec.encode({ 5: 42 });
    expect(buf.readUInt8(1)).toBe(FieldType.UINT32);
  });

  it('encodes non-integer numbers as FLOAT64', () => {
    const buf = codec.encode({ 5: 3.14 });
    expect(buf.readUInt8(1)).toBe(FieldType.FLOAT64);
  });

  it('encodes and decodes float64', () => {
    const decoded = codec.decode(codec.encode({ 3: Math.PI }));
    expect(decoded[3]).toBeCloseTo(Math.PI, 10);
  });

  it('encodes and decodes bigint (uint64)', () => {
    const big = 0xdead_beef_cafe_baben; // 16 hex digits = 64-bit value
    const decoded = codec.decode(codec.encode({ 4: big }));
    expect(decoded[4]).toBe(big);
  });

  it('encodes and decodes Buffer bytes', () => {
    const bytes = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
    const decoded = codec.decode(codec.encode({ 5: bytes }));
    expect(Buffer.isBuffer(decoded[5])).toBe(true);
    expect((decoded[5] as Buffer).equals(bytes)).toBe(true);
  });

  it('encodes and decodes UTF-8 string', () => {
    const decoded = codec.decode(codec.encode({ 6: 'héllo wörld 🌍' }));
    expect(decoded[6]).toBe('héllo wörld 🌍');
  });

  it('encodes empty string', () => {
    const decoded = codec.decode(codec.encode({ 7: '' }));
    expect(decoded[7]).toBe('');
  });

  it('encodes empty Buffer', () => {
    const decoded = codec.decode(codec.encode({ 8: Buffer.alloc(0) }));
    expect(Buffer.isBuffer(decoded[8])).toBe(true);
    expect((decoded[8] as Buffer).length).toBe(0);
  });

  it('skips undefined fields', () => {
    const rec: CodableRecord = { 0: 1, 1: undefined, 2: 2 };
    const buf = codec.encode(rec);
    const decoded = codec.decode(buf);
    expect(decoded[0]).toBe(1);
    expect(decoded[1]).toBeUndefined();
    expect(decoded[2]).toBe(2);
  });

  it('round-trips multiple fields of mixed types', () => {
    const original: CodableRecord = {
      0: null,
      1: true,
      2: 42,
      3: 3.14,
      4: 0xffff_ffff_ffff_ffffn,
      5: Buffer.from('binary'),
      6: 'utf8-string',
    };
    const decoded = codec.decode(codec.encode(original));
    expect(decoded[0]).toBeNull();
    expect(decoded[1]).toBe(true);
    expect(decoded[2]).toBe(42);
    expect(decoded[3]).toBeCloseTo(3.14, 10);
    expect(decoded[4]).toBe(0xffff_ffff_ffff_ffffn);
    expect((decoded[5] as Buffer).equals(Buffer.from('binary'))).toBe(true);
    expect(decoded[6]).toBe('utf8-string');
  });

  it('encodes empty record to empty buffer', () => {
    expect(codec.encode({}).length).toBe(0);
  });

  it('decodes empty buffer to empty record', () => {
    expect(Object.keys(codec.decode(Buffer.alloc(0)))).toHaveLength(0);
  });

  it('throws CodecError on truncated header', () => {
    const buf = Buffer.alloc(3); // need 6 for a full field header
    expect(() => codec.decode(buf)).toThrow(CodecError);
  });

  it('throws CodecError on truncated value', () => {
    const buf = Buffer.allocUnsafe(10);
    buf.writeUInt8(0, 0);               // fieldId
    buf.writeUInt8(FieldType.STRING, 1); // type
    buf.writeUInt32BE(100, 2);           // claims 100 bytes but only 4 remain
    buf.writeUInt32BE(0, 6);
    expect(() => codec.decode(buf)).toThrow(CodecError);
  });

  it('throws CodecError for out-of-range field ID', () => {
    // Force non-numeric key
    const bad = { 'not-a-number': 42 } as unknown as CodableRecord;
    expect(() => codec.encode(bad)).toThrow(CodecError);
  });
});

// ---------------------------------------------------------------------------
// resolveTransportConfig
// ---------------------------------------------------------------------------

describe('resolveTransportConfig', () => {
  it('applies defaults when no overrides', () => {
    const cfg = resolveTransportConfig();
    expect(cfg.isClient).toBe(DEFAULT_TRANSPORT_CONFIG.isClient);
    expect(cfg.keepAliveIntervalMs).toBe(DEFAULT_TRANSPORT_CONFIG.keepAliveIntervalMs);
    expect(cfg.initialWindowSize).toBe(DEFAULT_TRANSPORT_CONFIG.initialWindowSize);
  });

  it('merges overrides', () => {
    const cfg = resolveTransportConfig({ isClient: false, keepAliveIntervalMs: 0 });
    expect(cfg.isClient).toBe(false);
    expect(cfg.keepAliveIntervalMs).toBe(0);
    expect(cfg.initialWindowSize).toBe(DEFAULT_TRANSPORT_CONFIG.initialWindowSize);
  });
});

// ---------------------------------------------------------------------------
// FrameTransport — construction / attach
// ---------------------------------------------------------------------------

describe('FrameTransport construction', () => {
  it('starts closed = false', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    expect(t.isClosed).toBe(false);
  });

  it('throws if attached twice', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    const s = makeSink();
    t.attach(s);
    expect(() => t.attach(makeSink())).toThrow(TransportError);
    t.close();
  });

  it('throws on attach after close', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    const s = makeSink();
    t.attach(s);
    t.close();
    expect(() => t.attach(makeSink())).toThrow(TransportClosedError);
  });

  it('client transport allocates odd stream IDs', () => {
    const t = new FrameTransport({ isClient: true, keepAliveIntervalMs: 0 });
    const s = makeSink();
    t.attach(s);
    expect(t.openStream() % 2).toBe(1);
    t.close();
  });

  it('server transport allocates even stream IDs', () => {
    const t = new FrameTransport({ isClient: false, keepAliveIntervalMs: 0 });
    const s = makeSink();
    t.attach(s);
    expect(t.openStream() % 2).toBe(0);
    t.close();
  });
});

// ---------------------------------------------------------------------------
// FrameTransport — sendFrame / receive
// ---------------------------------------------------------------------------

describe('FrameTransport sendFrame and receive', () => {
  it('sends a DATA frame receivable by peer', async () => {
    const { clientStream, serverStream } = makePair();
    const client = new FrameTransport({ isClient: true, keepAliveIntervalMs: 0 });
    const server = new FrameTransport({ isClient: false, keepAliveIntervalMs: 0 });
    client.attach(clientStream);
    server.attach(serverStream);

    const received: Frame[] = [];
    server.on('frame', (f) => received.push(f));

    const sid = client.openStream();
    const payload = Buffer.from('telemetry-data');
    client.sendFrame(FrameType.DATA, FrameFlags.NONE, sid, payload);

    // Allow event loop to flush
    await new Promise((r) => setTimeout(r, 10));

    expect(received.length).toBeGreaterThanOrEqual(1);
    const dataFrame = received.find((f) => f.type === FrameType.DATA);
    expect(dataFrame).toBeDefined();
    expect(dataFrame!.payload.equals(payload)).toBe(true);

    client.close();
    server.close();
  });

  it('throws TransportClosedError when sending after close', async () => {
    const s = makeSink();
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(s);
    t.close();
    expect(() => t.sendFrame(FrameType.DATA, 0, 1, Buffer.alloc(0))).toThrow(
      TransportClosedError,
    );
  });

  it('throws FlowControlError when send window exhausted', () => {
    const s = makeSink();
    const t = new FrameTransport({
      isClient: true,
      keepAliveIntervalMs: 0,
      initialWindowSize: 10,
    });
    t.attach(s);
    const sid = t.openStream();
    expect(() =>
      t.sendFrame(FrameType.DATA, 0, sid, Buffer.alloc(100)),
    ).toThrow(FlowControlError);
    t.close();
  });

  it('deducts payload from send window', () => {
    const s = makeSink();
    const t = new FrameTransport({
      isClient: true,
      keepAliveIntervalMs: 0,
      initialWindowSize: 1000,
    });
    t.attach(s);
    const sid = t.openStream();
    t.sendFrame(FrameType.DATA, 0, sid, Buffer.alloc(300));
    expect(t.getStreamState(sid)!.sendWindow).toBe(700);
    t.close();
  });

  it('updateWindow grants the peer quota without changing our own send window', () => {
    const s = makeSink();
    const t = new FrameTransport({
      isClient: true,
      keepAliveIntervalMs: 0,
      initialWindowSize: 100,
    });
    t.attach(s);
    const sid = t.openStream();
    t.sendFrame(FrameType.DATA, 0, sid, Buffer.alloc(100));
    expect(t.getStreamState(sid)!.sendWindow).toBe(0);
    t.updateWindow(sid, 500);
    expect(t.getStreamState(sid)!.sendWindow).toBe(0);
    t.close();
  });

  it('a peer WINDOW_UPD increases our own send window', async () => {
    const s = makeSink();
    const t = new FrameTransport({
      isClient: true,
      keepAliveIntervalMs: 0,
      initialWindowSize: 100,
    });
    t.attach(s);
    const sid = t.openStream();
    t.sendFrame(FrameType.DATA, 0, sid, Buffer.alloc(100));
    expect(t.getStreamState(sid)!.sendWindow).toBe(0);

    const grant = Buffer.allocUnsafe(4);
    grant.writeUInt32BE(500, 0);
    s.push(buildFrame(FrameType.WINDOW_UPD, FrameFlags.NONE, sid, grant));
    await new Promise((r) => setTimeout(r, 10));

    expect(t.getStreamState(sid)!.sendWindow).toBe(500);
    t.close();
  });

  it('END_STREAM flag marks stream as half-closed', () => {
    const s = makeSink();
    const t = new FrameTransport({ isClient: true, keepAliveIntervalMs: 0 });
    t.attach(s);
    const sid = t.openStream();
    t.sendFrame(FrameType.DATA, FrameFlags.END_STREAM, sid, Buffer.alloc(0));
    expect(t.getStreamState(sid)!.halfClosed).toBe(true);
    t.close();
  });
});

// ---------------------------------------------------------------------------
// FrameTransport — stream management
// ---------------------------------------------------------------------------

describe('stream management', () => {
  it('openStream returns increasing stream IDs', () => {
    const s = makeSink();
    const t = new FrameTransport({ isClient: true, keepAliveIntervalMs: 0 });
    t.attach(s);
    const ids = [t.openStream(), t.openStream(), t.openStream()];
    expect(ids[0]).toBe(1);
    expect(ids[1]).toBe(3);
    expect(ids[2]).toBe(5);
    t.close();
  });

  it('getStreamState returns undefined for unknown stream', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(makeSink());
    expect(t.getStreamState(99)).toBeUndefined();
    t.close();
  });

  it('resetStream sends RST_STREAM and marks stream reset', () => {
    const s = makeSink();
    const t = new FrameTransport({ isClient: true, keepAliveIntervalMs: 0 });
    t.attach(s);
    const sid = t.openStream();
    t.resetStream(sid, 0x08 /* CANCEL */);
    expect(t.getStreamState(sid)!.reset).toBe(true);
    t.close();
  });

  it('acceptStream registers a remotely-initiated stream', () => {
    const t = new FrameTransport({ isClient: false, keepAliveIntervalMs: 0 });
    t.attach(makeSink());
    t.acceptStream(1);
    expect(t.getStreamState(1)).toBeDefined();
    expect(t.getStreamState(1)!.sendWindow).toBe(DEFAULT_WINDOW_SIZE);
    t.close();
  });

  it('metrics tracks stream count', () => {
    const t = new FrameTransport({ isClient: true, keepAliveIntervalMs: 0 });
    t.attach(makeSink());
    t.openStream();
    t.openStream();
    expect(t.metrics().streamCount).toBe(2);
    t.close();
  });
});

// ---------------------------------------------------------------------------
// FrameTransport — PING/PONG with fixed clock
// ---------------------------------------------------------------------------

describe('PING / PONG', () => {
  it('sendPing returns the clock timestamp', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0, clock: fixedClock });
    t.attach(makeSink());
    const ts = t.sendPing();
    expect(ts).toBe(FIXED_NOW);
    t.close();
  });

  it('increments pingsSent', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0, clock: fixedClock });
    t.attach(makeSink());
    t.sendPing();
    t.sendPing();
    expect(t.metrics().pingsSent).toBe(2);
    t.close();
  });

  it('pong event emitted and RTT calculated from clock delta', async () => {
    const sentAt = FIXED_NOW;
    const receivedAt = FIXED_NOW + 25;

    // Clock returns sentAt on the first call (sendPing) and receivedAt on the
    // second call (handlePong), so the RTT is exactly 25 ms.
    let callCount = 0;
    const steppedClock = (): number => (callCount++ === 0 ? sentAt : receivedAt);

    const s = makeSink();
    const t = new FrameTransport({ keepAliveIntervalMs: 0, clock: steppedClock });
    t.attach(s);

    const pongs: Array<{ sid: number; rtt: number }> = [];
    t.on('pong', (sid: number, rtt: number) => pongs.push({ sid, rtt }));

    t.sendPing();
    s.push(buildFrame(FrameType.PONG, FrameFlags.ACK, 0, Buffer.alloc(8)));

    await new Promise((r) => setTimeout(r, 10));
    expect(t.metrics().pongsReceived).toBe(1);
    expect(t.lastPongRttMs).toBe(receivedAt - sentAt);
    expect(pongs).toHaveLength(1);
    expect(pongs[0]?.rtt).toBe(25);

    t.close();
  });

  it('throws TransportClosedError when sending ping after close', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(makeSink());
    t.close();
    expect(() => t.sendPing()).toThrow(TransportClosedError);
  });
});

// ---------------------------------------------------------------------------
// FrameTransport — GOAWAY
// ---------------------------------------------------------------------------

describe('GOAWAY', () => {
  it('close() marks transport closed', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(makeSink());
    t.close();
    expect(t.isClosed).toBe(true);
  });

  it('emits close event on close()', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(makeSink());
    let fired = false;
    t.on('close', () => { fired = true; });
    t.close();
    expect(fired).toBe(true);
  });

  it('emits goaway event when GOAWAY received from peer', async () => {
    const s = makeSink();
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(s);
    let gotGoAway = false;
    t.on('goaway', () => { gotGoAway = true; });

    const payload = Buffer.allocUnsafe(8);
    payload.writeUInt32BE(5, 0);  // lastStreamId
    payload.writeUInt32BE(0, 4);  // errorCode
    const frame = buildFrame(FrameType.GOAWAY, 0, 0, payload);
    s.push(frame);

    await new Promise((r) => setTimeout(r, 10));
    expect(gotGoAway).toBe(true);
    expect(t.isClosed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// FrameTransport — error propagation
// ---------------------------------------------------------------------------

describe('error propagation', () => {
  it('emits error event on protocol violation', async () => {
    const s = makeSink();
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(s);
    const errors: Error[] = [];
    t.on('error', (e) => errors.push(e));

    // Push garbage bytes that fail magic check
    const bad = Buffer.allocUnsafe(HEADER_BYTES);
    bad.writeUInt32BE(0xdeadbeef, 0); // bad magic
    bad.fill(0, 4);
    bad.writeUInt32BE(0, 12); // zero payload
    s.push(bad);

    await new Promise((r) => setTimeout(r, 10));
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]).toBeInstanceOf(FrameProtocolError);
    expect(t.isClosed).toBe(true);
  });

  it('metrics.errors increments on protocol error', async () => {
    const s = makeSink();
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(s);
    t.on('error', () => {});

    const bad = Buffer.alloc(HEADER_BYTES); // all zeros → bad magic
    s.push(bad);
    await new Promise((r) => setTimeout(r, 10));
    expect(t.metrics().errors).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// FrameTransport — metrics accumulation
// ---------------------------------------------------------------------------

describe('metrics accumulation', () => {
  it('tracks frames sent and bytes sent', async () => {
    const s = makeSink();
    const t = new FrameTransport({ isClient: true, keepAliveIntervalMs: 0 });
    t.attach(s);
    const sid = t.openStream();
    const payload = Buffer.alloc(50);
    t.sendFrame(FrameType.DATA, 0, sid, payload);
    t.sendFrame(FrameType.DATA, 0, sid, payload);
    const m = t.metrics();
    expect(m.framesSent).toBe(2);
    expect(m.bytesSent).toBe(2 * (HEADER_BYTES + 50));
    t.close();
  });

  it('tracks frames received and bytes received', async () => {
    const s = makeSink();
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(s);

    const frame = buildFrame(FrameType.DATA, 0, 1, Buffer.from('abc'));
    s.push(frame);
    await new Promise((r) => setTimeout(r, 10));

    const m = t.metrics();
    expect(m.framesReceived).toBe(1);
    expect(m.bytesReceived).toBe(frame.length);
    t.close();
  });
});

// ---------------------------------------------------------------------------
// createClientTransport / createServerTransport factories
// ---------------------------------------------------------------------------

describe('convenience factories', () => {
  it('createClientTransport returns a transport with isClient=true', async () => {
    const s = makeSink();
    const t = createClientTransport(s, { keepAliveIntervalMs: 0 });
    expect(t.openStream() % 2).toBe(1); // odd
    t.close();
  });

  it('createServerTransport returns a transport with isClient=false', async () => {
    const s = makeSink();
    const t = createServerTransport(s, { keepAliveIntervalMs: 0 });
    expect(t.openStream() % 2).toBe(0); // even
    t.close();
  });
});

// ---------------------------------------------------------------------------
// updateWindow validation
// ---------------------------------------------------------------------------

describe('updateWindow', () => {
  it('throws FrameProtocolError for zero increment', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(makeSink());
    expect(() => t.updateWindow(1, 0)).toThrow(FrameProtocolError);
    t.close();
  });

  it('throws FrameProtocolError for negative increment', () => {
    const t = new FrameTransport({ keepAliveIntervalMs: 0 });
    t.attach(makeSink());
    expect(() => t.updateWindow(1, -100)).toThrow(FrameProtocolError);
    t.close();
  });
});
