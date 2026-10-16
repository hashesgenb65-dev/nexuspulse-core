/**
 * @module transport
 * @description Custom binary frame-multiplexing transport layer for NexusPulse.
 *
 * ## Architecture overview
 *
 * This module implements a lightweight, high-performance binary multiplexing
 * protocol over a raw duplex stream (TCP socket, Unix pipe, or any
 * `stream.Duplex`).  It is deliberately independent of gRPC's HTTP/2 framing
 * so that the core telemetry pipeline can run over bare TCP with minimal
 * overhead.
 *
 * ### Frame wire format
 *
 * Every message is prefixed with a fixed 16-byte header:
 *
 * ```
 * Offset  Len  Field
 * ──────  ───  ─────────────────────────────────────────────────────────
 *      0    4  magic          0xNP_MAGIC (0x4e50_5800) — "NPX\0"
 *      4    1  version        Protocol version (currently 0x01)
 *      5    1  type           FrameType enum
 *      6    2  flags          Bitmask of FrameFlags
 *      8    4  streamId       Multiplexing stream identifier (uint32 BE)
 *     12    4  payloadLen     Payload length in bytes (uint32 BE, ≤ MAX_PAYLOAD)
 * ```
 *
 * The header is followed immediately by `payloadLen` bytes of payload.
 * Total frame size = HEADER_BYTES + payloadLen.
 *
 * ### Multiplexing
 *
 * Each logical RPC call is identified by a `streamId`.  The initiating side
 * allocates odd stream IDs (client-initiated) and the responding side uses
 * even IDs (server-initiated), mirroring HTTP/2 semantics.  Stream IDs are
 * 32-bit unsigned integers; overflow wraps to 1 (skipping 0).
 *
 * ### Frame types
 *
 * | Value | Name        | Description                                  |
 * |-------|-------------|----------------------------------------------|
 * | 0x00  | DATA        | Application payload                          |
 * | 0x01  | HEADERS     | Method name + metadata (msgpack-like)        |
 * | 0x02  | RST_STREAM  | Abruptly close a stream (carries error code) |
 * | 0x03  | SETTINGS    | Connection-level configuration               |
 * | 0x04  | PING        | Keep-alive probe (echoed as PONG)            |
 * | 0x05  | PONG        | Keep-alive reply                             |
 * | 0x06  | GOAWAY      | Graceful shutdown notification               |
 * | 0x07  | WINDOW_UPD  | Flow-control window increment                |
 *
 * ### Codec
 *
 * The module includes a `BinaryCodec` that serialises/deserialises plain
 * JavaScript objects to/from a compact Tag-Length-Value (TLV) binary format.
 * Each field is encoded as:
 *
 * ```
 * fieldId (uint8) | type (uint8) | length (uint32 BE) | value
 * ```
 *
 * Supported value types: `uint32`, `uint64` (as two uint32s), `float64`,
 * `bool`, `bytes`, `string` (UTF-8), `null`.
 *
 * ### Keep-alive
 *
 * The `FrameTransport` sends a PING frame every `keepAliveIntervalMs`
 * milliseconds and expects a PONG reply within `keepAliveTimeoutMs`.  If the
 * timeout fires without a PONG, the connection is torn down with a
 * `TransportTimeoutError`.
 *
 * ### Flow control
 *
 * Each stream has a send window (`initialWindowSize` bytes, default 64 KiB).
 * The sender blocks if the window is exhausted and resumes on WINDOW_UPD.
 * Connection-level flow control is not implemented (all budgets are per-stream).
 *
 * @packageDocumentation
 */

import { EventEmitter } from 'node:events';
import { Duplex } from 'node:stream';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Magic number at offset 0 of every frame header: "NPX\0". */
export const FRAME_MAGIC = 0x4e505800 as const;

/** Current protocol version embedded in every frame. */
export const PROTOCOL_VERSION = 0x01 as const;

/** Fixed header size in bytes. */
export const HEADER_BYTES = 16 as const;

/** Maximum allowed payload size (4 MiB). Frames exceeding this are rejected. */
export const MAX_PAYLOAD_BYTES = 4 * 1024 * 1024;

/** Default flow-control window per stream (64 KiB). */
export const DEFAULT_WINDOW_SIZE = 65536 as const;

/** Default keep-alive probe interval in milliseconds. */
export const DEFAULT_KEEPALIVE_INTERVAL_MS = 30_000 as const;

/** Default keep-alive timeout in milliseconds. */
export const DEFAULT_KEEPALIVE_TIMEOUT_MS = 10_000 as const;

/** Maximum stream ID before wrap-around. */
export const MAX_STREAM_ID = 0xffff_ffff as const;

// ---------------------------------------------------------------------------
// Enumerations
// ---------------------------------------------------------------------------

/** Frame type byte values. */
export const enum FrameType {
  DATA       = 0x00,
  HEADERS    = 0x01,
  RST_STREAM = 0x02,
  SETTINGS   = 0x03,
  PING       = 0x04,
  PONG       = 0x05,
  GOAWAY     = 0x06,
  WINDOW_UPD = 0x07,
}

/** Frame flag bit positions. */
export const enum FrameFlags {
  NONE        = 0x0000,
  END_STREAM  = 0x0001, // DATA: last frame for this stream
  COMPRESSED  = 0x0002, // DATA: payload is compressed (future use)
  ACK         = 0x0004, // PING: this PING is a PONG
  PADDED      = 0x0008, // reserved
  PRIORITY    = 0x0010, // reserved
}

/** TLV field value types for BinaryCodec. */
export const enum FieldType {
  NULL    = 0x00,
  BOOL    = 0x01,
  UINT32  = 0x02,
  UINT64  = 0x03, // encoded as two uint32 BE (hi, lo)
  FLOAT64 = 0x04,
  BYTES   = 0x05,
  STRING  = 0x06,
}

// ---------------------------------------------------------------------------
// Error hierarchy
// ---------------------------------------------------------------------------

/** Root error class for all transport failures. */
export class TransportError extends Error {
  public readonly code: string;
  public readonly details: Readonly<Record<string, unknown>>;
  constructor(
    message: string,
    code: string,
    details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = 'TransportError';
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

/** The transport connection was closed unexpectedly or gracefully. */
export class TransportClosedError extends TransportError {
  constructor(reason?: string) {
    super(reason ?? 'Transport closed', 'TRANSPORT_CLOSED');
    this.name = 'TransportClosedError';
  }
}

/** A keep-alive PONG was not received within the timeout window. */
export class TransportTimeoutError extends TransportError {
  constructor() {
    super('Keep-alive timeout: no PONG received', 'KEEPALIVE_TIMEOUT');
    this.name = 'TransportTimeoutError';
  }
}

/** A received frame failed protocol-level validation. */
export class FrameProtocolError extends TransportError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'FRAME_PROTOCOL_ERROR', details);
    this.name = 'FrameProtocolError';
  }
}

/** The flow-control send window is exhausted. */
export class FlowControlError extends TransportError {
  constructor(streamId: number) {
    super(
      `Stream ${streamId}: send window exhausted`,
      'FLOW_CONTROL_EXHAUSTED',
      { streamId },
    );
    this.name = 'FlowControlError';
  }
}

/** A codec serialisation or deserialisation failure. */
export class CodecError extends TransportError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'CODEC_ERROR', details);
    this.name = 'CodecError';
  }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/** Fully decoded frame, ready for application consumption. */
export interface Frame {
  readonly magic: typeof FRAME_MAGIC;
  readonly version: number;
  readonly type: FrameType;
  readonly flags: number;
  readonly streamId: number;
  readonly payload: Buffer;
}

/** Snapshot of per-stream state. */
export interface StreamState {
  readonly streamId: number;
  readonly sendWindow: number;
  readonly recvBytes: number;
  readonly sentBytes: number;
  readonly halfClosed: boolean; // END_STREAM sent
  readonly reset: boolean;      // RST_STREAM received
}

/** Transport-level metrics snapshot. */
export interface TransportMetrics {
  readonly framesReceived: number;
  readonly framesSent: number;
  readonly bytesReceived: number;
  readonly bytesSent: number;
  readonly pingsSent: number;
  readonly pongsReceived: number;
  readonly streamCount: number;
  readonly activeStreams: number;
  readonly errors: number;
}

/** Events emitted by `FrameTransport`. */
export interface TransportEvents {
  frame:   (frame: Frame) => void;
  error:   (err: Error) => void;
  close:   () => void;
  ping:    (streamId: number) => void;
  pong:    (streamId: number, rttMs: number) => void;
  goaway:  (lastStreamId: number, errorCode: number) => void;
  window:  (streamId: number, increment: number) => void;
}

/** Configuration for `FrameTransport`. */
export interface TransportConfig {
  /** If true, this side allocates odd stream IDs; otherwise even. */
  readonly isClient: boolean;
  /** Keep-alive probe interval in ms. 0 disables keep-alive. */
  readonly keepAliveIntervalMs: number;
  /** Keep-alive timeout in ms. */
  readonly keepAliveTimeoutMs: number;
  /** Initial per-stream send window in bytes. */
  readonly initialWindowSize: number;
  /** Override Date.now for deterministic tests. */
  readonly clock: () => number;
}

/** Resolved config with defaults. */
export const DEFAULT_TRANSPORT_CONFIG: Readonly<TransportConfig> = Object.freeze({
  isClient: true,
  keepAliveIntervalMs: DEFAULT_KEEPALIVE_INTERVAL_MS,
  keepAliveTimeoutMs: DEFAULT_KEEPALIVE_TIMEOUT_MS,
  initialWindowSize: DEFAULT_WINDOW_SIZE,
  clock: Date.now,
});

export function resolveTransportConfig(
  overrides?: Partial<TransportConfig>,
): TransportConfig {
  return { ...DEFAULT_TRANSPORT_CONFIG, ...overrides };
}

// ---------------------------------------------------------------------------
// Frame builder
// ---------------------------------------------------------------------------

/**
 * Builds a raw frame buffer from its components.
 *
 * @param type      - FrameType byte.
 * @param flags     - Bitmask of FrameFlags.
 * @param streamId  - 32-bit stream identifier.
 * @param payload   - Payload bytes (may be empty).
 * @throws {FrameProtocolError} if payload exceeds MAX_PAYLOAD_BYTES.
 */
export function buildFrame(
  type: FrameType,
  flags: number,
  streamId: number,
  payload: Buffer,
): Buffer {
  if (payload.length > MAX_PAYLOAD_BYTES) {
    throw new FrameProtocolError(
      `Payload length ${payload.length} exceeds maximum ${MAX_PAYLOAD_BYTES}`,
      { payloadLength: payload.length, max: MAX_PAYLOAD_BYTES },
    );
  }

  const frame = Buffer.allocUnsafe(HEADER_BYTES + payload.length);
  frame.writeUInt32BE(FRAME_MAGIC, 0);
  frame.writeUInt8(PROTOCOL_VERSION, 4);
  frame.writeUInt8(type, 5);
  frame.writeUInt16BE(flags, 6);
  frame.writeUInt32BE(streamId >>> 0, 8);
  frame.writeUInt32BE(payload.length, 12);
  payload.copy(frame, HEADER_BYTES);
  return frame;
}

/**
 * Parses a complete frame buffer (header + payload).
 *
 * @throws {FrameProtocolError} on any validation failure.
 */
export function parseFrame(buf: Buffer): Frame {
  if (buf.length < HEADER_BYTES) {
    throw new FrameProtocolError(
      `Buffer too short for header: ${buf.length} < ${HEADER_BYTES}`,
    );
  }
  const magic = buf.readUInt32BE(0);
  if (magic !== FRAME_MAGIC) {
    throw new FrameProtocolError(
      `Invalid magic: 0x${magic.toString(16)} (expected 0x${FRAME_MAGIC.toString(16)})`,
      { magic },
    );
  }
  const version = buf.readUInt8(4);
  if (version !== PROTOCOL_VERSION) {
    throw new FrameProtocolError(
      `Unsupported protocol version: ${version}`,
      { version },
    );
  }
  const type = buf.readUInt8(5) as FrameType;
  const flags = buf.readUInt16BE(6);
  const streamId = buf.readUInt32BE(8);
  const payloadLen = buf.readUInt32BE(12);
  if (payloadLen > MAX_PAYLOAD_BYTES) {
    throw new FrameProtocolError(
      `Declared payload length ${payloadLen} exceeds maximum ${MAX_PAYLOAD_BYTES}`,
      { payloadLen },
    );
  }
  if (buf.length < HEADER_BYTES + payloadLen) {
    throw new FrameProtocolError(
      `Buffer too short for payload: have ${buf.length - HEADER_BYTES}, need ${payloadLen}`,
    );
  }
  const payload = buf.subarray(HEADER_BYTES, HEADER_BYTES + payloadLen);
  return {
    magic: FRAME_MAGIC,
    version,
    type,
    flags,
    streamId,
    payload,
  };
}

// ---------------------------------------------------------------------------
// Length-prefixed frame parser (streaming)
// ---------------------------------------------------------------------------

/**
 * Stateful streaming frame parser.
 *
 * Accumulates raw bytes from a TCP stream and emits complete frames as soon as
 * they are available.  Uses a private ring-like Buffer that grows on demand and
 * is consumed (slice-on-consume) rather than copied on every read.
 *
 * ### Usage
 * ```ts
 * const parser = new FrameParser();
 * socket.on('data', (chunk) => {
 *   for (const frame of parser.push(chunk)) {
 *     handle(frame);
 *   }
 * });
 * ```
 */
export class FrameParser {
  #buf: Buffer = Buffer.allocUnsafe(0);

  /**
   * Appends `chunk` to the internal buffer and returns an array of all
   * complete frames that can be parsed from the accumulated bytes.
   *
   * @throws {FrameProtocolError} on hard protocol violations (bad magic, etc.).
   */
  push(chunk: Buffer): Frame[] {
    this.#buf = Buffer.concat([this.#buf, chunk]);
    const frames: Frame[] = [];

    while (this.#buf.length >= HEADER_BYTES) {
      // Peek at declared payload length without consuming header yet
      const magic = this.#buf.readUInt32BE(0);
      if (magic !== FRAME_MAGIC) {
        throw new FrameProtocolError(
          `Stream sync lost: bad magic 0x${magic.toString(16)}`,
          { magic },
        );
      }
      const payloadLen = this.#buf.readUInt32BE(12);
      if (payloadLen > MAX_PAYLOAD_BYTES) {
        throw new FrameProtocolError(
          `Declared payload length ${payloadLen} exceeds maximum`,
          { payloadLen },
        );
      }
      const totalLen = HEADER_BYTES + payloadLen;
      if (this.#buf.length < totalLen) break; // need more bytes

      const frameSlice = this.#buf.subarray(0, totalLen);
      frames.push(parseFrame(frameSlice));
      this.#buf = this.#buf.subarray(totalLen); // consume
    }

    return frames;
  }

  /** Remaining unparsed bytes in the internal buffer. */
  get bufferedBytes(): number {
    return this.#buf.length;
  }

  /** Clears the internal buffer. */
  reset(): void {
    this.#buf = Buffer.allocUnsafe(0);
  }
}

// ---------------------------------------------------------------------------
// BinaryCodec: TLV serialisation
// ---------------------------------------------------------------------------

/** A record that can be encoded by BinaryCodec.  Values must be encodable. */
export type CodableRecord = Record<
  number,
  null | boolean | number | bigint | Buffer | string | undefined
>;

/**
 * Compact Tag-Length-Value codec for application messages.
 *
 * Field encoding:
 * ```
 * fieldId  uint8   (0–255)
 * type     uint8   FieldType enum
 * length   uint32 BE
 * value    <length bytes>
 * ```
 *
 * `undefined` fields are skipped during encoding (sparse representation).
 *
 * ### Limitations
 * - Field IDs must be 0–255.
 * - Nested records are not supported; flatten before encoding.
 * - uint64 values are encoded as a bigint or two uint32s (hi:uint32, lo:uint32).
 */
export class BinaryCodec {
  /**
   * Encodes a record into a TLV buffer.
   *
   * @throws {CodecError} for unsupported value types or out-of-range field IDs.
   */
  encode(record: CodableRecord): Buffer {
    const parts: Buffer[] = [];

    for (const [rawId, value] of Object.entries(record)) {
      if (value === undefined) continue;

      const fieldId = parseInt(rawId, 10);
      if (!Number.isInteger(fieldId) || fieldId < 0 || fieldId > 255) {
        throw new CodecError(`Field ID must be 0–255, got ${rawId}`);
      }

      const encoded = this.#encodeValue(value);
      const header = Buffer.allocUnsafe(6); // fieldId(1) + type(1) + length(4)
      header.writeUInt8(fieldId, 0);
      header.writeUInt8(encoded.type, 1);
      header.writeUInt32BE(encoded.data.length, 2);
      parts.push(header, encoded.data);
    }

    return Buffer.concat(parts);
  }

  /**
   * Decodes a TLV buffer back into a plain record.
   *
   * @throws {CodecError} on truncated or malformed input.
   */
  decode(buf: Buffer): CodableRecord {
    const record: CodableRecord = {};
    let offset = 0;

    while (offset < buf.length) {
      if (offset + 6 > buf.length) {
        throw new CodecError(
          `Truncated TLV field header at offset ${offset}`,
          { offset, bufLen: buf.length },
        );
      }
      const fieldId = buf.readUInt8(offset);
      const type = buf.readUInt8(offset + 1) as FieldType;
      const length = buf.readUInt32BE(offset + 2);
      offset += 6;

      if (offset + length > buf.length) {
        throw new CodecError(
          `Truncated TLV field value: need ${length} bytes at offset ${offset}, have ${buf.length - offset}`,
          { fieldId, type, length, offset, bufLen: buf.length },
        );
      }

      const valueSlice = buf.subarray(offset, offset + length);
      record[fieldId] = this.#decodeValue(type, valueSlice, fieldId);
      offset += length;
    }

    return record;
  }

  // -------------------------------------------------------------------------
  // Private encoding helpers
  // -------------------------------------------------------------------------

  #encodeValue(
    value: Exclude<CodableRecord[number], undefined>,
  ): { type: FieldType; data: Buffer } {
    if (value === null) {
      return { type: FieldType.NULL, data: Buffer.allocUnsafe(0) };
    }
    if (typeof value === 'boolean') {
      const data = Buffer.allocUnsafe(1);
      data.writeUInt8(value ? 1 : 0, 0);
      return { type: FieldType.BOOL, data };
    }
    if (typeof value === 'bigint') {
      // Encode as uint64: high 32 bits, low 32 bits
      const hi = Number((value >> 32n) & 0xffff_ffffn);
      const lo = Number(value & 0xffff_ffffn);
      const data = Buffer.allocUnsafe(8);
      data.writeUInt32BE(hi, 0);
      data.writeUInt32BE(lo, 4);
      return { type: FieldType.UINT64, data };
    }
    if (typeof value === 'number') {
      if (Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff) {
        const data = Buffer.allocUnsafe(4);
        data.writeUInt32BE(value >>> 0, 0);
        return { type: FieldType.UINT32, data };
      }
      const data = Buffer.allocUnsafe(8);
      data.writeDoubleBE(value, 0);
      return { type: FieldType.FLOAT64, data };
    }
    if (Buffer.isBuffer(value)) {
      return { type: FieldType.BYTES, data: value };
    }
    if (typeof value === 'string') {
      return { type: FieldType.STRING, data: Buffer.from(value, 'utf8') };
    }
    throw new CodecError(
      `Unsupported value type: ${typeof value}`,
      { valueType: typeof value },
    );
  }

  #decodeValue(
    type: FieldType,
    buf: Buffer,
    fieldId: number,
  ): CodableRecord[number] {
    switch (type) {
      case FieldType.NULL:
        return null;
      case FieldType.BOOL:
        if (buf.length !== 1)
          throw new CodecError(`BOOL field ${fieldId} must be 1 byte`, { fieldId });
        return buf.readUInt8(0) !== 0;
      case FieldType.UINT32:
        if (buf.length !== 4)
          throw new CodecError(`UINT32 field ${fieldId} must be 4 bytes`, { fieldId });
        return buf.readUInt32BE(0);
      case FieldType.UINT64: {
        if (buf.length !== 8)
          throw new CodecError(`UINT64 field ${fieldId} must be 8 bytes`, { fieldId });
        const hi = BigInt(buf.readUInt32BE(0));
        const lo = BigInt(buf.readUInt32BE(4));
        return (hi << 32n) | lo;
      }
      case FieldType.FLOAT64:
        if (buf.length !== 8)
          throw new CodecError(`FLOAT64 field ${fieldId} must be 8 bytes`, { fieldId });
        return buf.readDoubleBE(0);
      case FieldType.BYTES:
        return Buffer.from(buf); // copy for safety
      case FieldType.STRING:
        return buf.toString('utf8');
      default:
        throw new CodecError(
          `Unknown field type: 0x${(type as number).toString(16)}`,
          { type },
        );
    }
  }
}

// ---------------------------------------------------------------------------
// Stream state tracker
// ---------------------------------------------------------------------------

class StreamRecord {
  readonly streamId: number;
  sendWindow: number;
  recvBytes = 0;
  sentBytes = 0;
  halfClosed = false;
  reset = false;

  constructor(streamId: number, initialWindow: number) {
    this.streamId = streamId;
    this.sendWindow = initialWindow;
  }

  snapshot(): StreamState {
    return {
      streamId: this.streamId,
      sendWindow: this.sendWindow,
      recvBytes: this.recvBytes,
      sentBytes: this.sentBytes,
      halfClosed: this.halfClosed,
      reset: this.reset,
    };
  }
}

// ---------------------------------------------------------------------------
// FrameTransport
// ---------------------------------------------------------------------------

/**
 * Multiplexing binary frame transport.
 *
 * Wraps any `Duplex` stream and provides:
 * - Length-prefixed frame parsing via `FrameParser`.
 * - Stream multiplexing with per-stream flow control.
 * - Keep-alive PING/PONG with RTT measurement.
 * - Graceful shutdown via GOAWAY.
 * - Typed event emission (`frame`, `error`, `close`, `ping`, `pong`,
 *   `goaway`, `window`).
 *
 * ### Lifecycle
 * ```
 * CREATED → [attach()] → OPEN ─→ [close() / peer GOAWAY] → CLOSING → CLOSED
 *                                └→ [error]               → CLOSED
 * ```
 *
 * After `close()` or a fatal error, no more frames are sent or emitted.
 */
export class FrameTransport extends EventEmitter {
  readonly #cfg: TransportConfig;
  readonly #parser = new FrameParser();

  #stream: Duplex | null = null;
  #closed = false;
  #nextStreamId: number;

  // Keep-alive
  #pingTimer: ReturnType<typeof setInterval> | null = null;
  #pongTimer: ReturnType<typeof setTimeout> | null = null;
  #lastPingSentAt = 0;
  #lastPongRttMs = 0;

  // Per-stream state
  readonly #streams = new Map<number, StreamRecord>();

  // Metrics
  #framesReceived = 0;
  #framesSent = 0;
  #bytesReceived = 0;
  #bytesSent = 0;
  #pingsSent = 0;
  #pongsReceived = 0;
  #errors = 0;

  constructor(config?: Partial<TransportConfig>) {
    super();
    this.#cfg = resolveTransportConfig(config);
    // Clients use odd stream IDs starting at 1; servers use even starting at 2
    this.#nextStreamId = this.#cfg.isClient ? 1 : 2;
  }

  // -------------------------------------------------------------------------
  // Public: attach
  // -------------------------------------------------------------------------

  /**
   * Binds this transport to a duplex stream and begins reading frames.
   * May only be called once.
   *
   * @throws {TransportError} if the transport is already attached or closed.
   */
  attach(stream: Duplex): void {
    if (this.#closed) {
      throw new TransportClosedError('Cannot attach a closed transport');
    }
    if (this.#stream !== null) {
      throw new TransportError('Transport is already attached to a stream', 'ALREADY_ATTACHED');
    }
    this.#stream = stream;

    stream.on('data', (chunk: Buffer) => this.#onData(chunk));
    stream.on('error', (err: Error) => this.#onStreamError(err));
    stream.on('close', () => this.#onStreamClose());
    stream.on('end', () => this.#onStreamClose());

    if (this.#cfg.keepAliveIntervalMs > 0) {
      this.#startKeepAlive();
    }
  }

  // -------------------------------------------------------------------------
  // Public: sendFrame
  // -------------------------------------------------------------------------

  /**
   * Serialises and writes a frame to the underlying stream.
   *
   * @returns `true` if the write was buffered successfully; `false` if the
   *          underlying stream's write returned false (backpressure).
   * @throws {TransportClosedError} if the transport is closed.
   * @throws {FlowControlError}     if the stream's send window is exhausted.
   */
  sendFrame(
    type: FrameType,
    flags: number,
    streamId: number,
    payload: Buffer = Buffer.allocUnsafe(0),
  ): boolean {
    if (this.#closed || this.#stream === null) {
      throw new TransportClosedError();
    }

    // Flow-control check for DATA frames on tracked streams
    if (type === FrameType.DATA && streamId !== 0) {
      const rec = this.#streams.get(streamId);
      if (rec !== undefined) {
        if (payload.length > rec.sendWindow) {
          throw new FlowControlError(streamId);
        }
        rec.sendWindow -= payload.length;
        rec.sentBytes += payload.length;
      }
    }

    const frame = buildFrame(type, flags, streamId, payload);
    const ok = this.#stream.write(frame);
    this.#framesSent++;
    this.#bytesSent += frame.length;

    // Track END_STREAM
    if ((flags & FrameFlags.END_STREAM) !== 0) {
      const rec = this.#streams.get(streamId);
      if (rec !== undefined) rec.halfClosed = true;
    }

    return ok;
  }

  // -------------------------------------------------------------------------
  // Public: stream management
  // -------------------------------------------------------------------------

  /**
   * Allocates a new stream ID and registers its state.
   * Must be called by the initiating side before sending HEADERS.
   */
  openStream(): number {
    if (this.#closed) throw new TransportClosedError();
    const id = this.#nextStreamId;
    this.#nextStreamId += 2;
    if (this.#nextStreamId > MAX_STREAM_ID) {
      // Wrap around, skip 0
      this.#nextStreamId = this.#cfg.isClient ? 1 : 2;
    }
    this.#streams.set(id, new StreamRecord(id, this.#cfg.initialWindowSize));
    return id;
  }

  /**
   * Registers a remotely-initiated stream ID.
   * Called when HEADERS are received for an unknown stream ID.
   */
  acceptStream(streamId: number): void {
    if (!this.#streams.has(streamId)) {
      this.#streams.set(
        streamId,
        new StreamRecord(streamId, this.#cfg.initialWindowSize),
      );
    }
  }

  /**
   * Sends RST_STREAM and marks the stream as reset.
   *
   * @param streamId  - Stream to reset.
   * @param errorCode - Application error code (uint32, 4-byte payload).
   */
  resetStream(streamId: number, errorCode: number): void {
    const rec = this.#streams.get(streamId);
    if (rec !== undefined) rec.reset = true;
    const payload = Buffer.allocUnsafe(4);
    payload.writeUInt32BE(errorCode >>> 0, 0);
    this.sendFrame(FrameType.RST_STREAM, FrameFlags.NONE, streamId, payload);
  }

  /**
   * Sends a WINDOW_UPD frame, granting the REMOTE side additional send quota
   * on `streamId`. This does not change our own send window: our quota is
   * only raised by WINDOW_UPD frames received from the peer (see
   * `#handleWindowUpdate`).
   *
   * @param streamId  - Target stream.
   * @param increment - Window increment in bytes (must be > 0).
   */
  updateWindow(streamId: number, increment: number): void {
    if (increment <= 0) {
      throw new FrameProtocolError(
        `Window increment must be positive, got ${increment}`,
      );
    }
    const payload = Buffer.allocUnsafe(4);
    payload.writeUInt32BE(increment >>> 0, 0);
    this.sendFrame(FrameType.WINDOW_UPD, FrameFlags.NONE, streamId, payload);
  }

  /** Returns the current state of a stream, or `undefined` if unknown. */
  getStreamState(streamId: number): StreamState | undefined {
    return this.#streams.get(streamId)?.snapshot();
  }

  // -------------------------------------------------------------------------
  // Public: close / goaway
  // -------------------------------------------------------------------------

  /**
   * Sends GOAWAY with `lastStreamId = 0` and closes the underlying stream.
   *
   * @param errorCode - Application-level error code (default 0 = no error).
   */
  close(errorCode = 0): void {
    if (this.#closed) return;
    this.#setClosed();
    this.#sendGoAway(0, errorCode);
    this.#stream?.destroy();
  }

  // -------------------------------------------------------------------------
  // Public: keep-alive / ping
  // -------------------------------------------------------------------------

  /**
   * Sends a PING frame on stream 0.
   * The remote side must reply with a PONG (same payload, ACK flag set).
   *
   * @returns The timestamp (from `cfg.clock`) at which the ping was sent.
   */
  sendPing(): number {
    if (this.#closed || this.#stream === null) throw new TransportClosedError();
    this.#lastPingSentAt = this.#cfg.clock();
    const payload = Buffer.allocUnsafe(8);
    payload.writeBigUInt64BE(BigInt(this.#lastPingSentAt), 0);
    this.sendFrame(FrameType.PING, FrameFlags.NONE, 0, payload);
    this.#pingsSent++;
    return this.#lastPingSentAt;
  }

  /** Most recently measured keep-alive RTT in milliseconds. */
  get lastPongRttMs(): number {
    return this.#lastPongRttMs;
  }

  // -------------------------------------------------------------------------
  // Public: metrics
  // -------------------------------------------------------------------------

  metrics(): TransportMetrics {
    let activeStreams = 0;
    for (const rec of this.#streams.values()) {
      if (!rec.halfClosed && !rec.reset) activeStreams++;
    }
    return {
      framesReceived: this.#framesReceived,
      framesSent: this.#framesSent,
      bytesReceived: this.#bytesReceived,
      bytesSent: this.#bytesSent,
      pingsSent: this.#pingsSent,
      pongsReceived: this.#pongsReceived,
      streamCount: this.#streams.size,
      activeStreams,
      errors: this.#errors,
    };
  }

  /** Whether the transport is in a closed state. */
  get isClosed(): boolean {
    return this.#closed;
  }

  // -------------------------------------------------------------------------
  // Private: data handler
  // -------------------------------------------------------------------------

  #onData(chunk: Buffer): void {
    this.#bytesReceived += chunk.length;
    let frames: Frame[];
    try {
      frames = this.#parser.push(chunk);
    } catch (err) {
      this.#errors++;
      this.emit('error', err);
      this.#setClosed();
      this.#stream?.destroy();
      return;
    }

    for (const frame of frames) {
      this.#framesReceived++;
      this.#dispatchFrame(frame);
    }
  }

  #dispatchFrame(frame: Frame): void {
    switch (frame.type) {
      case FrameType.PING:
        this.#handlePing(frame);
        break;
      case FrameType.PONG:
        this.#handlePong(frame);
        break;
      case FrameType.GOAWAY:
        this.#handleGoAway(frame);
        break;
      case FrameType.WINDOW_UPD:
        this.#handleWindowUpdate(frame);
        break;
      case FrameType.RST_STREAM:
        this.#handleRstStream(frame);
        break;
      case FrameType.HEADERS:
        this.acceptStream(frame.streamId);
        this.emit('frame', frame);
        break;
      default:
        // Update recv bytes on tracked streams for DATA frames
        if (frame.type === FrameType.DATA) {
          const rec = this.#streams.get(frame.streamId);
          if (rec !== undefined) rec.recvBytes += frame.payload.length;
        }
        this.emit('frame', frame);
    }
  }

  // -------------------------------------------------------------------------
  // Private: frame type handlers
  // -------------------------------------------------------------------------

  #handlePing(frame: Frame): void {
    // Echo back as PONG
    this.emit('ping', frame.streamId);
    if (!this.#closed && this.#stream !== null) {
      this.sendFrame(FrameType.PONG, FrameFlags.ACK, frame.streamId, Buffer.from(frame.payload));
    }
  }

  #handlePong(frame: Frame): void {
    // Cancel the keep-alive timeout timer
    if (this.#pongTimer !== null) {
      clearTimeout(this.#pongTimer);
      this.#pongTimer = null;
    }
    const now = this.#cfg.clock();
    this.#lastPongRttMs = now - this.#lastPingSentAt;
    this.#pongsReceived++;
    this.emit('pong', frame.streamId, this.#lastPongRttMs);
  }

  #handleGoAway(frame: Frame): void {
    const lastStreamId = frame.payload.length >= 4
      ? frame.payload.readUInt32BE(0)
      : 0;
    const errorCode = frame.payload.length >= 8
      ? frame.payload.readUInt32BE(4)
      : 0;
    this.emit('goaway', lastStreamId, errorCode);
    this.#setClosed();
    this.#stream?.destroy();
  }

  #handleWindowUpdate(frame: Frame): void {
    if (frame.payload.length < 4) {
      this.#errors++;
      this.emit('error', new FrameProtocolError('WINDOW_UPD payload must be ≥ 4 bytes'));
      return;
    }
    const increment = frame.payload.readUInt32BE(0);
    const rec = this.#streams.get(frame.streamId);
    if (rec !== undefined) {
      rec.sendWindow += increment;
    }
    this.emit('window', frame.streamId, increment);
  }

  #handleRstStream(frame: Frame): void {
    const rec = this.#streams.get(frame.streamId);
    if (rec !== undefined) rec.reset = true;
    this.emit('frame', frame);
  }

  // -------------------------------------------------------------------------
  // Private: keep-alive
  // -------------------------------------------------------------------------

  #startKeepAlive(): void {
    this.#pingTimer = setInterval(() => {
      if (this.#closed) return;
      try {
        this.sendPing();
      } catch {
        return;
      }
      // Arm timeout for PONG
      this.#pongTimer = setTimeout(() => {
        this.#errors++;
        const err = new TransportTimeoutError();
        this.emit('error', err);
        this.#setClosed();
        this.#stream?.destroy();
      }, this.#cfg.keepAliveTimeoutMs);
    }, this.#cfg.keepAliveIntervalMs);

    // Don't let the keep-alive timer prevent process exit
    if (typeof this.#pingTimer.unref === 'function') {
      this.#pingTimer.unref();
    }
  }

  // -------------------------------------------------------------------------
  // Private: helpers
  // -------------------------------------------------------------------------

  #setClosed(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#pingTimer !== null) {
      clearInterval(this.#pingTimer);
      this.#pingTimer = null;
    }
    if (this.#pongTimer !== null) {
      clearTimeout(this.#pongTimer);
      this.#pongTimer = null;
    }
    this.emit('close');
  }

  #onStreamError(err: Error): void {
    this.#errors++;
    this.emit('error', err);
    this.#setClosed();
  }

  #onStreamClose(): void {
    this.#setClosed();
  }

  #sendGoAway(lastStreamId: number, errorCode: number): void {
    if (this.#stream === null || this.#stream.destroyed) return;
    try {
      const payload = Buffer.allocUnsafe(8);
      payload.writeUInt32BE(lastStreamId >>> 0, 0);
      payload.writeUInt32BE(errorCode >>> 0, 4);
      const frame = buildFrame(FrameType.GOAWAY, FrameFlags.NONE, 0, payload);
      this.#stream.write(frame);
    } catch {
      // best-effort
    }
  }
}

// ---------------------------------------------------------------------------
// Convenience factory
// ---------------------------------------------------------------------------

/**
 * Creates a client-side `FrameTransport` bound to the given duplex stream.
 */
export function createClientTransport(
  stream: Duplex,
  config?: Partial<TransportConfig>,
): FrameTransport {
  const t = new FrameTransport({ ...config, isClient: true });
  t.attach(stream);
  return t;
}

/**
 * Creates a server-side `FrameTransport` bound to the given duplex stream.
 */
export function createServerTransport(
  stream: Duplex,
  config?: Partial<TransportConfig>,
): FrameTransport {
  const t = new FrameTransport({ ...config, isClient: false });
  t.attach(stream);
  return t;
}
