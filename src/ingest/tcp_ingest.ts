/**
 * NexusPulse TCP ingest: accepts framed telemetry over TCP and feeds it to the
 * stream engine.
 *
 * Wire format. Each telemetry event is one DATA frame whose payload is a
 * BinaryCodec record with these field IDs (see {@link EVENT_FIELD}):
 *   1 id (string), 2 source (string), 3 metric (string),
 *   4 value (number), 5 timestamp (number, epoch ms)
 *
 * Behaviour.
 * - A malformed payload is counted and skipped. The connection stays open.
 * - A record that fails engine validation is counted as rejected. The connection stays open.
 * - Frames other than DATA are ignored by the ingest layer. The transport still handles
 *   PING, PONG, GOAWAY and window updates itself.
 * - Connections beyond `maxConnections` are closed at once.
 * - A connection with no socket activity for `idleTimeoutMs` is closed.
 *
 * Authentication. When a {@link TokenAuth} is configured, the first DATA frame on every
 * connection must be an auth record: field {@link AUTH_FIELD} holds a writer token. Any other
 * first frame, or a token that is unknown or has the reader role, closes the connection before
 * any event is read. Without a token store the ingest is open, as before.
 *
 * TLS is not handled here. Run this behind TLS or a mutually authenticated tunnel before
 * exposing it beyond a trusted network: tokens are sent in clear over plain TCP.
 */

import * as net from 'node:net';
import * as tls from 'node:tls';
import { roleSatisfies, type TokenAuth } from '../api/auth.js';
import { BackpressureError, type StreamEngine, type SubmitResult, type TelemetryEvent } from '../core/engine.js';
import { tlsServerOptions, type TlsCredentials } from './tls_credentials.js';
import {
  BinaryCodec,
  createServerTransport,
  FrameType,
  type CodableRecord,
  type Frame,
  type FrameTransport,
  type TransportConfig,
} from '../grpc/transport.js';

// ---------------------------------------------------------------------------
// Wire encoding
// ---------------------------------------------------------------------------

/** Field IDs used in the event record. */
export const EVENT_FIELD = Object.freeze({ ID: 1, SOURCE: 2, METRIC: 3, VALUE: 4, TIMESTAMP: 5 });

/** Encodes one telemetry event as a record payload. */
export function encodeTelemetryRecord(event: TelemetryEvent): Buffer {
  const codec = new BinaryCodec();
  return codec.encode({
    [EVENT_FIELD.ID]: event.id,
    [EVENT_FIELD.SOURCE]: event.source,
    [EVENT_FIELD.METRIC]: event.metric,
    [EVENT_FIELD.VALUE]: event.value,
    [EVENT_FIELD.TIMESTAMP]: event.timestamp,
  });
}

/**
 * Decodes a record payload into the raw object the engine validates. Missing
 * fields stay absent, so the engine reports them as invalid.
 *
 * @throws {CodecError} If the payload is not a well-formed record.
 */
export function decodeTelemetryRecord(payload: Buffer): Record<string, unknown> {
  const codec = new BinaryCodec();
  const record: CodableRecord = codec.decode(payload);
  const raw: Record<string, unknown> = {};
  const map: [keyof typeof EVENT_FIELD, string][] = [
    ['ID', 'id'],
    ['SOURCE', 'source'],
    ['METRIC', 'metric'],
    ['VALUE', 'value'],
    ['TIMESTAMP', 'timestamp'],
  ];
  for (const [field, name] of map) {
    const value = record[EVENT_FIELD[field]];
    if (value !== undefined) raw[name] = value;
  }
  return raw;
}

/** Field ID that carries the bearer token in the first frame of a connection. */
export const AUTH_FIELD = 100;

/** Encodes the auth record that opens an authenticated connection. */
export function encodeAuthRecord(secret: string): Buffer {
  return new BinaryCodec().encode({ [AUTH_FIELD]: secret });
}

/** Returns the token in an auth record, or undefined if the payload is not one. Never throws. */
export function decodeAuthRecord(payload: Buffer): string | undefined {
  try {
    const value = new BinaryCodec().decode(payload)[AUTH_FIELD];
    return typeof value === 'string' ? value : undefined;
  } catch {
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

export interface TcpIngestOptions {
  readonly engine: StreamEngine;
  /**
   * Token store. When set, each connection must open with a writer token (see the module
   * comment). When omitted, the ingest accepts connections without credentials.
   */
  readonly auth?: TokenAuth;
  /** Maximum simultaneous connections. Default 64. */
  readonly maxConnections?: number;
  /** Close a connection after this many milliseconds without socket activity. Default 60 000. */
  readonly idleTimeoutMs?: number;
  /** Overrides for the transport. Keep-alive probes default to off on the server side. */
  readonly transport?: Partial<TransportConfig>;
  /**
   * Submits one raw event. Defaults to `engine.submit`. The host passes a
   * wrapper here so that rules see events that arrive over TCP.
   */
  readonly submit?: (raw: unknown) => SubmitResult;
  /**
   * TLS credentials. When set, the port speaks TLS (TLS 1.2 or later) and the clear-text protocol
   * is only available inside the TLS session. Load them with `loadTlsCredentials`.
   */
  readonly tls?: TlsCredentials;
}

export interface IngestStats {
  readonly connectionsAccepted: number;
  readonly connectionsRejected: number;
  readonly connectionsOpen: number;
  readonly connectionErrors: number;
  readonly idleClosed: number;
  readonly framesReceived: number;
  readonly malformedFrames: number;
  readonly eventsAccepted: number;
  readonly eventsRejected: number;
  readonly eventsDroppedQueueFull: number;
  /** Connections closed because their opening frame was not a valid writer token. */
  readonly authFailures: number;
  /** Unexpected engine failures. These are counted and never propagated. */
  readonly internalErrors: number;
}

export const DEFAULT_MAX_CONNECTIONS = 64;
export const DEFAULT_IDLE_TIMEOUT_MS = 60_000;
const MAX_CONNECTIONS_LIMIT = 10_000;
const MAX_IDLE_TIMEOUT_MS = 86_400_000;

function assertIntegerIn(name: string, value: number, min: number, max: number): void {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} must be an integer from ${min} to ${max}`);
  }
}

/** Accepts telemetry over TCP and forwards valid events to a {@link StreamEngine}. */
export class TcpIngestServer {
  private readonly engine: StreamEngine;
  private readonly maxConnections: number;
  private readonly idleTimeoutMs: number;
  private readonly transportConfig: Partial<TransportConfig>;
  private readonly submit: (raw: unknown) => SubmitResult;
  private readonly auth: TokenAuth | undefined;
  private readonly sockets = new Set<net.Socket>();
  private readonly server: net.Server;
  private boundPort: number | undefined;
  private readonly counters = {
    connectionsAccepted: 0,
    connectionsRejected: 0,
    connectionErrors: 0,
    idleClosed: 0,
    framesReceived: 0,
    malformedFrames: 0,
    eventsAccepted: 0,
    eventsRejected: 0,
    eventsDroppedQueueFull: 0,
    authFailures: 0,
    internalErrors: 0,
  };

  constructor(options: TcpIngestOptions) {
    this.engine = options.engine;
    this.auth = options.auth;
    this.maxConnections = options.maxConnections ?? DEFAULT_MAX_CONNECTIONS;
    this.idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS;
    assertIntegerIn('maxConnections', this.maxConnections, 1, MAX_CONNECTIONS_LIMIT);
    assertIntegerIn('idleTimeoutMs', this.idleTimeoutMs, 1, MAX_IDLE_TIMEOUT_MS);
    this.transportConfig = { keepAliveIntervalMs: 0, ...(options.transport ?? {}) };
    const engine = this.engine;
    this.submit = options.submit ?? ((raw: unknown) => engine.submit(raw));
    const onConnection = (socket: net.Socket): void => this.onConnection(socket);
    this.server =
      options.tls === undefined
        ? net.createServer(onConnection)
        : tls.createServer(tlsServerOptions(options.tls), onConnection);
  }

  /**
   * Starts listening. Port 0 picks a free port.
   *
   * @returns The bound port.
   */
  listen(port: number, host: string = '127.0.0.1'): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      const onError = (err: Error): void => reject(err);
      this.server.once('error', onError);
      this.server.listen(port, host, () => {
        this.server.off('error', onError);
        const address = this.server.address();
        if (address === null || typeof address === 'string') {
          reject(new Error('server did not bind to a TCP port'));
          return;
        }
        this.boundPort = address.port;
        resolve(address.port);
      });
    });
  }

  /** The bound port, or undefined before listen() resolves or after close(). */
  get port(): number | undefined {
    return this.server.listening ? this.boundPort : undefined;
  }

  /**
   * Stops accepting connections and closes the open ones. Resolves once the
   * listening socket is closed. Safe to call more than once.
   */
  close(): Promise<void> {
    return new Promise<void>((resolve) => {
      if (!this.server.listening) {
        resolve();
        return;
      }
      this.server.close(() => resolve());
      for (const socket of this.sockets) socket.destroy();
    });
  }

  stats(): IngestStats {
    return Object.freeze({
      ...this.counters,
      connectionsOpen: this.sockets.size,
    });
  }

  // -- internals -------------------------------------------------------------

  private onConnection(socket: net.Socket): void {
    if (this.sockets.size >= this.maxConnections) {
      this.counters.connectionsRejected++;
      socket.destroy();
      return;
    }
    this.counters.connectionsAccepted++;
    this.sockets.add(socket);

    socket.setTimeout(this.idleTimeoutMs, () => {
      this.counters.idleClosed++;
      socket.destroy();
    });
    socket.on('error', () => {
      this.counters.connectionErrors++;
      socket.destroy();
    });
    socket.on('close', () => {
      this.sockets.delete(socket);
    });

    const transport = createServerTransport(socket, this.transportConfig);
    this.bindTransport(transport, socket);
  }

  private bindTransport(transport: FrameTransport, socket: net.Socket): void {
    // Without a token store every connection is trusted from the start.
    let authenticated = this.auth === undefined;
    // A refused connection may still have frames queued behind the bad one. They are dropped, and it is counted once.
    let refused = false;
    transport.on('frame', (frame: Frame) => {
      if (refused || frame.type !== FrameType.DATA) return;
      if (!authenticated) {
        if (!this.admit(frame.payload)) {
          refused = true;
          this.counters.authFailures++;
          socket.destroy();
          return;
        }
        authenticated = true;
        return;
      }
      this.onFrame(frame);
    });
    transport.on('error', () => {
      this.counters.connectionErrors++;
      socket.destroy();
    });
  }

  /** True when the opening frame carries a token with the writer role. */
  private admit(payload: Buffer): boolean {
    const secret = decodeAuthRecord(payload);
    if (secret === undefined || this.auth === undefined) return false;
    const principal = this.auth.verifySecret(secret);
    return principal !== undefined && roleSatisfies(principal.role, 'writer');
  }

  private onFrame(frame: Frame): void {
    this.counters.framesReceived++;

    let raw: Record<string, unknown>;
    try {
      raw = decodeTelemetryRecord(frame.payload);
    } catch {
      this.counters.malformedFrames++;
      return;
    }

    try {
      const result = this.submit(raw);
      if (result.accepted) {
        this.counters.eventsAccepted++;
      } else {
        this.counters.eventsRejected++;
      }
    } catch (err) {
      // Never rethrow here: this runs inside the transport's event emitter.
      if (err instanceof BackpressureError) {
        this.counters.eventsDroppedQueueFull++;
      } else {
        this.counters.internalErrors++;
      }
    }
  }
}
