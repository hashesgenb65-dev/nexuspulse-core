/**
 * TCP transport for the consensus driver: the network half of {@link PeerTransport}.
 *
 * Wire format. Each message is one frame: a 4-byte big-endian length followed by that many
 * bytes of UTF-8 JSON. A request is `{ type: 'request', id, method, body }`. A reply is
 * `{ type: 'reply', id, ok: true, body }` or `{ type: 'reply', id, ok: false, error }`. The id
 * ties each reply to its request, so replies can arrive in any order on one connection.
 *
 * Snapshots. An InstallSnapshot is split into chunks of at most 512 KiB, sent as one
 * `installSnapshotChunk` request each. The follower replies with the offset it wants next, so a
 * dropped or repeated chunk is resent from the right place.
 *
 * Server behaviour. A request whose body fails validation gets an error reply, and the
 * connection stays open. A frame that cannot be parsed, or that announces a length above the
 * limit, closes the connection, because the stream can no longer be trusted.
 *
 * Client behaviour. Each peer gets one lazily opened connection, which is replaced after it
 * fails. Every request has a timeout. A late reply after its timeout is discarded.
 *
 * TLS and peer authentication are not handled here. Use a private network or a tunnel until
 * they are added.
 */

import * as net from 'node:net';
import * as tls from 'node:tls';
import { tlsServerOptions, type TlsCredentials, MIN_TLS_VERSION } from '../ingest/tls_credentials.js';
import type {
  AppendReply,
  AppendRequest,
  InstallSnapshotReply,
  InstallSnapshotRequest,
  LogEntry,
  VoteRequest,
  VoteResponse,
} from './consensus.js';
import type { PeerTransport, Scheduler, TimerId } from './driver.js';
import { SnapshotAssembler, encodeSnapshot, sliceChunk, type SnapshotChunk } from './snapshot_chunks.js';
import { authenticate, PeerAuthError, refuseFrame } from './peer_auth.js';

export const DEFAULT_MAX_FRAME_BYTES = 1 << 20;
export const DEFAULT_REQUEST_TIMEOUT_MS = 1_000;
export const DEFAULT_CONNECT_TIMEOUT_MS = 1_000;

export type TransportErrorCode =
  | 'FRAME_TOO_LARGE'
  | 'MALFORMED_FRAME'
  | 'TIMEOUT'
  | 'UNREACHABLE'
  | 'CONNECTION_LOST'
  | 'REMOTE_ERROR'
  | 'BAD_REPLY'
  | 'UNKNOWN_PEER'
  | 'CLOSED'
  | 'AUTH_FAILED';

/** A failure in the transport. `code` says what kind of failure it was. */
export class TransportError extends Error {
  public readonly code: TransportErrorCode;

  constructor(code: TransportErrorCode, message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'TransportError';
    this.code = code;
  }
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

/** Encodes one value as a length-prefixed JSON frame. */
export function encodeFrame(value: unknown, maxFrameBytes: number = DEFAULT_MAX_FRAME_BYTES): Buffer {
  const body = Buffer.from(JSON.stringify(value), 'utf8');
  if (body.length > maxFrameBytes) {
    throw new TransportError('FRAME_TOO_LARGE', `frame of ${body.length} bytes exceeds the limit of ${maxFrameBytes}`);
  }
  const frame = Buffer.alloc(4 + body.length);
  frame.writeUInt32BE(body.length, 0);
  body.copy(frame, 4);
  return frame;
}

function parseBody(body: Buffer): unknown {
  try {
    return JSON.parse(body.toString('utf8')) as unknown;
  } catch {
    throw new TransportError('MALFORMED_FRAME', 'frame body is not valid JSON');
  }
}

/** Splits a byte stream into frames. Partial frames are held until the rest arrives. */
export class FrameDecoder {
  private pending: Buffer = Buffer.alloc(0);
  private readonly maxFrameBytes: number;

  constructor(maxFrameBytes: number = DEFAULT_MAX_FRAME_BYTES) {
    this.maxFrameBytes = maxFrameBytes;
  }

  /**
   * Adds bytes from the stream and returns every complete value they finish.
   *
   * @throws {TransportError} FRAME_TOO_LARGE when a header announces a frame above the limit,
   *   and MALFORMED_FRAME when a body is not JSON. After either error the stream is unusable.
   */
  push(chunk: Buffer): unknown[] {
    this.pending = this.pending.length === 0 ? chunk : Buffer.concat([this.pending, chunk]);
    const messages: unknown[] = [];

    while (this.pending.length >= 4) {
      const length = this.pending.readUInt32BE(0);
      if (length > this.maxFrameBytes) {
        throw new TransportError('FRAME_TOO_LARGE', `frame of ${length} bytes exceeds the limit of ${this.maxFrameBytes}`);
      }
      if (this.pending.length < 4 + length) break;

      const body = this.pending.subarray(4, 4 + length);
      this.pending = this.pending.subarray(4 + length);
      messages.push(parseBody(body));
    }
    return messages;
  }
}

// ---------------------------------------------------------------------------
// Validation of wire values
// ---------------------------------------------------------------------------

type Method = 'requestVote' | 'appendEntries' | 'installSnapshotChunk';

/** One chunk of an InstallSnapshot, sent in its own RPC. The envelope carries the leader's term. */
interface InstallSnapshotChunkRequest {
  readonly term: number;
  readonly leaderId: string;
  readonly chunk: SnapshotChunk;
}

/** The follower's answer to one chunk. `nextOffset` is the byte the leader sends next. */
interface InstallSnapshotChunkReply extends InstallSnapshotReply {
  readonly nextOffset: number;
}

/** A chunk stream that makes no progress this many times in a row is abandoned. */
const MAX_STALLED_CHUNKS = 3;

interface RequestFrame {
  readonly id: number;
  readonly method: Method;
  readonly body: unknown;
}

interface ReplyFrame {
  readonly id: number;
  readonly ok: boolean;
  readonly body?: unknown;
  readonly error?: unknown;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0;
}

function isLogEntry(value: unknown): value is LogEntry {
  return (
    isRecord(value) &&
    isNonNegativeInteger(value.index) &&
    value.index >= 1 &&
    isNonNegativeInteger(value.term) &&
    typeof value.command === 'string'
  );
}

function isVoteRequest(value: unknown): value is VoteRequest {
  return (
    isRecord(value) &&
    isNonNegativeInteger(value.term) &&
    typeof value.candidateId === 'string' &&
    isNonNegativeInteger(value.lastLogIndex) &&
    isNonNegativeInteger(value.lastLogTerm)
  );
}

function isAppendRequest(value: unknown): value is AppendRequest {
  return (
    isRecord(value) &&
    isNonNegativeInteger(value.term) &&
    typeof value.leaderId === 'string' &&
    isNonNegativeInteger(value.prevLogIndex) &&
    isNonNegativeInteger(value.prevLogTerm) &&
    isNonNegativeInteger(value.leaderCommit) &&
    Array.isArray(value.entries) &&
    value.entries.every(isLogEntry)
  );
}

function isInstallSnapshotChunkRequest(value: unknown): value is InstallSnapshotChunkRequest {
  return (
    isRecord(value) &&
    isNonNegativeInteger(value.term) &&
    typeof value.leaderId === 'string' &&
    isRecord(value.chunk) &&
    typeof value.chunk.data === 'string'
  );
}

function isInstallSnapshotChunkReply(value: unknown): value is InstallSnapshotChunkReply {
  return (
    isRecord(value) &&
    isNonNegativeInteger(value.term) &&
    typeof value.installed === 'boolean' &&
    isNonNegativeInteger(value.nextOffset)
  );
}

function isVoteResponse(value: unknown): value is VoteResponse {
  return isRecord(value) && isNonNegativeInteger(value.term) && typeof value.voteGranted === 'boolean';
}

function isAppendReply(value: unknown): value is AppendReply {
  return isRecord(value) && isNonNegativeInteger(value.term) && typeof value.success === 'boolean';
}

function parseRequest(message: unknown): RequestFrame | undefined {
  if (!isRecord(message) || message.type !== 'request' || !isNonNegativeInteger(message.id)) return undefined;
  if (message.method === 'requestVote' && isVoteRequest(message.body)) {
    return { id: message.id, method: 'requestVote', body: message.body };
  }
  if (message.method === 'appendEntries' && isAppendRequest(message.body)) {
    return { id: message.id, method: 'appendEntries', body: message.body };
  }
  if (message.method === 'installSnapshotChunk' && isInstallSnapshotChunkRequest(message.body)) {
    return { id: message.id, method: 'installSnapshotChunk', body: message.body };
  }
  return undefined;
}

function isReply(message: unknown): message is ReplyFrame {
  return (
    isRecord(message) &&
    message.type === 'reply' &&
    isNonNegativeInteger(message.id) &&
    typeof message.ok === 'boolean'
  );
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : 'unknown error';
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/** The node methods that answer peer requests. {@link ConsensusDriver} implements this. */
export interface RpcHandler {
  handleVoteRequest(request: VoteRequest): VoteResponse;
  handleAppendRequest(request: AppendRequest): AppendReply;
  /** Absent on nodes that do not take snapshots. Such a node answers an installSnapshot with an error. */
  handleSnapshotRequest?(request: InstallSnapshotRequest): InstallSnapshotReply;
}

export interface PeerServerOptions {
  handler: RpcHandler;
  maxFrameBytes?: number;
  /**
   * The cluster secret. When set, requests without a valid tag are dropped with their connection,
   * and every reply is tagged. Every member must share the same secret.
   */
  secret?: Buffer;
  /** Clock used for tags and the skew check. Defaults to Date.now. */
  now?: () => number;
  /** Receives failures on connections. `where` says which part failed. */
  onError?: (error: unknown, where: string) => void;
  /** TLS credentials. When set, peers must connect over TLS 1.2 or later. */
  tls?: TlsCredentials;
}

/** Accepts peer connections and answers their RPCs by calling the handler. */
export class PeerServer {
  private readonly server: net.Server;
  private readonly sockets = new Set<net.Socket>();
  private readonly handler: RpcHandler;
  private readonly maxFrameBytes: number;
  private readonly onError: ((error: unknown, where: string) => void) | undefined;
  private readonly secret: Buffer | undefined;
  private readonly now: () => number;
  /** Chunks of the snapshot being received. One leader sends one snapshot at a time. */
  private readonly snapshots = new SnapshotAssembler();

  constructor(options: PeerServerOptions) {
    this.handler = options.handler;
    this.maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
    this.onError = options.onError;
    this.secret = options.secret;
    this.now = options.now ?? Date.now;
    const onConnection = (socket: net.Socket): void => this.onConnection(socket);
    this.server =
      options.tls === undefined
        ? net.createServer(onConnection)
        : tls.createServer(tlsServerOptions(options.tls), onConnection);
  }

  /** Starts listening. Port 0 picks a free port. Resolves with the port in use. */
  listen(port: number, host: string = '127.0.0.1'): Promise<number> {
    return new Promise((resolve, reject) => {
      const onStartError = (error: Error): void => reject(error);
      this.server.once('error', onStartError);
      this.server.listen(port, host, () => {
        this.server.off('error', onStartError);
        const address = this.server.address();
        if (address === null || typeof address === 'string') {
          reject(new Error('server is not bound to a TCP port'));
          return;
        }
        resolve(address.port);
      });
    });
  }

  /** Closes the listener and every open connection. */
  close(): Promise<void> {
    for (const socket of this.sockets) socket.destroy();
    this.sockets.clear();
    return new Promise((resolve) => {
      if (!this.server.listening) {
        resolve();
        return;
      }
      this.server.close(() => resolve());
    });
  }

  private onConnection(socket: net.Socket): void {
    this.sockets.add(socket);
    const decoder = new FrameDecoder(this.maxFrameBytes);

    socket.on('data', (chunk: Buffer) => {
      let messages: unknown[];
      try {
        messages = decoder.push(chunk);
      } catch (error) {
        this.report(error, 'frame');
        socket.destroy();
        return;
      }
      for (const message of messages) {
        if (!this.admit(socket, message)) return;
        this.dispatch(socket, message);
      }
    });
    socket.on('error', (error) => this.report(error, 'socket'));
    socket.on('close', () => this.sockets.delete(socket));
  }

  /** Returns false, after dropping the connection, when a frame does not carry a valid tag. */
  private admit(socket: net.Socket, message: unknown): boolean {
    if (this.secret === undefined) return true;
    const refusal = isRecord(message) ? refuseFrame(this.secret, message, this.now()) : 'not an object';
    if (refusal === undefined) return true;

    this.report(new PeerAuthError(`refused a frame: ${refusal}`), 'auth');
    socket.destroy();
    return false;
  }

  private dispatch(socket: net.Socket, message: unknown): void {
    const request = parseRequest(message);
    if (request === undefined) {
      // Without a usable id there is no way to answer, so the connection is dropped.
      const id = isRecord(message) && isNonNegativeInteger(message.id) ? message.id : undefined;
      if (id === undefined) {
        socket.destroy();
        return;
      }
      this.send(socket, { type: 'reply', id, ok: false, error: 'invalid request' });
      return;
    }

    try {
      const body = this.route(request);
      this.send(socket, { type: 'reply', id: request.id, ok: true, body });
    } catch (error) {
      this.send(socket, { type: 'reply', id: request.id, ok: false, error: describe(error) });
    }
  }

  private route(request: RequestFrame): unknown {
    switch (request.method) {
      case 'requestVote':
        return this.handler.handleVoteRequest(request.body as VoteRequest);
      case 'appendEntries':
        return this.handler.handleAppendRequest(request.body as AppendRequest);
      case 'installSnapshotChunk': {
        const handle = this.handler.handleSnapshotRequest;
        if (handle === undefined) throw new Error('snapshots are not supported by this node');
        return this.receiveChunk(handle, request.body as InstallSnapshotChunkRequest);
      }
    }
  }

  /**
   * Takes one chunk. A partial snapshot is only buffered, so the node's state does not change until
   * the last chunk arrives. Then the whole snapshot goes to the handler, as before.
   */
  private receiveChunk(
    handle: NonNullable<RpcHandler['handleSnapshotRequest']>,
    body: InstallSnapshotChunkRequest,
  ): InstallSnapshotChunkReply {
    const result = this.snapshots.accept(body.chunk);
    if (result.status === 'refused') throw new Error(`snapshot chunk refused: ${result.reason}`);
    if (result.status === 'partial') return { term: body.term, installed: false, nextOffset: result.nextOffset };

    const reply = handle.call(this.handler, { term: body.term, leaderId: body.leaderId, snapshot: result.snapshot });
    return { term: reply.term, installed: reply.installed, nextOffset: body.chunk.total };
  }

  private send(socket: net.Socket, frame: object): void {
    if (socket.destroyed) return;
    const signed = this.secret === undefined ? frame : { ...frame, auth: authenticate(this.secret, frame, this.now()) };
    socket.write(encodeFrame(signed, this.maxFrameBytes));
  }

  private report(error: unknown, where: string): void {
    this.onError?.(error, where);
  }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

export interface PeerAddress {
  readonly host: string;
  readonly port: number;
}

export interface TcpPeerTransportOptions {
  /** Address of every peer, keyed by node id. */
  peers: Readonly<Record<string, PeerAddress>>;
  requestTimeoutMs?: number;
  connectTimeoutMs?: number;
  maxFrameBytes?: number;
  /** The cluster secret. When set, every request is tagged and every reply must carry a valid tag. */
  secret?: Buffer;
  /** Clock used for tags and the skew check. Defaults to Date.now. */
  now?: () => number;
  /** When set, connections use TLS and the peer's certificate must chain to `ca`. */
  tls?: PeerTlsClient;
}

/** How a peer connection trusts the other member's certificate. */
export interface PeerTlsClient {
  /** PEM certificate, or chain, that the peer's certificate must chain to. */
  readonly ca: Buffer;
  /** Name checked against the certificate. Defaults to the peer's host. */
  readonly servername?: string;
}

interface LinkSettings {
  readonly requestTimeoutMs: number;
  readonly connectTimeoutMs: number;
  readonly maxFrameBytes: number;
  readonly secret: Buffer | undefined;
  readonly now: () => number;
  readonly tls: PeerTlsClient | undefined;
}

interface Waiter {
  readonly resolve: (value: unknown) => void;
  readonly reject: (error: Error) => void;
  readonly timer: ReturnType<typeof setTimeout>;
}

/** One peer's connection: opened on first use, replaced after it fails. */
class Link {
  private readonly peerId: string;
  private readonly address: PeerAddress;
  private readonly settings: LinkSettings;
  private socket: net.Socket | undefined;
  private connecting: Promise<net.Socket> | undefined;
  private readonly waiters = new Map<number, Waiter>();
  private nextId = 1;
  private closed = false;

  constructor(peerId: string, address: PeerAddress, settings: LinkSettings) {
    this.peerId = peerId;
    this.address = address;
    this.settings = settings;
  }

  async request(method: Method, body: unknown): Promise<unknown> {
    if (this.closed) throw new TransportError('CLOSED', `transport to ${this.peerId} is closed`);
    const socket = await this.connect();
    const id = this.nextId++;
    const unsigned = { type: 'request', id, method, body };
    const signed =
      this.settings.secret === undefined
        ? unsigned
        : { ...unsigned, auth: authenticate(this.settings.secret, unsigned, this.settings.now()) };
    const frame = encodeFrame(signed, this.settings.maxFrameBytes);

    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.waiters.delete(id);
        reject(new TransportError('TIMEOUT', `no reply from ${this.peerId} within ${this.settings.requestTimeoutMs} ms`));
      }, this.settings.requestTimeoutMs);
      this.waiters.set(id, { resolve, reject, timer });
      socket.write(frame);
    });
  }

  close(): void {
    this.closed = true;
    const socket = this.socket;
    if (socket !== undefined) {
      this.fail(socket, new TransportError('CLOSED', `transport to ${this.peerId} closed`));
    }
  }

  private connect(): Promise<net.Socket> {
    if (this.socket !== undefined && !this.socket.destroyed) return Promise.resolve(this.socket);
    this.connecting ??= this.open();
    return this.connecting;
  }

  private open(): Promise<net.Socket> {
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const fail = (error: TransportError): void => {
        if (timer !== undefined) clearTimeout(timer);
        this.connecting = undefined;
        socket.destroy();
        reject(error);
      };
      const onConnectError = (error: Error): void => {
        fail(new TransportError('UNREACHABLE', `cannot reach ${this.peerId}: ${error.message}`));
      };

      const tlsClient = this.settings.tls;
      const socket: net.Socket =
        tlsClient === undefined
          ? net.connect({ host: this.address.host, port: this.address.port })
          : tls.connect({
              host: this.address.host,
              port: this.address.port,
              ca: tlsClient.ca,
              ...(tlsClient.servername === undefined ? {} : { servername: tlsClient.servername }),
              minVersion: MIN_TLS_VERSION,
              rejectUnauthorized: true,
            });
      timer = setTimeout(() => {
        fail(new TransportError('UNREACHABLE', `connecting to ${this.peerId} timed out`));
      }, this.settings.connectTimeoutMs);
      socket.once('error', onConnectError);
      socket.once(tlsClient === undefined ? 'connect' : 'secureConnect', () => {
        socket.removeListener('error', onConnectError);
        if (timer !== undefined) clearTimeout(timer);
        this.connecting = undefined;
        if (this.closed) {
          socket.destroy();
          reject(new TransportError('CLOSED', `transport to ${this.peerId} closed`));
          return;
        }
        this.attach(socket);
        resolve(socket);
      });
    });
  }

  private attach(socket: net.Socket): void {
    this.socket = socket;
    const decoder = new FrameDecoder(this.settings.maxFrameBytes);

    socket.on('data', (chunk: Buffer) => {
      let messages: unknown[];
      try {
        messages = decoder.push(chunk);
      } catch (error) {
        const cause = error instanceof TransportError ? error : new TransportError('MALFORMED_FRAME', describe(error));
        this.fail(socket, cause);
        return;
      }
      for (const message of messages) this.onMessage(socket, message);
    });
    socket.on('error', (error) => {
      this.fail(socket, new TransportError('CONNECTION_LOST', `connection to ${this.peerId} failed: ${error.message}`));
    });
    socket.on('close', () => {
      this.fail(socket, new TransportError('CONNECTION_LOST', `connection to ${this.peerId} closed`));
    });
  }

  private onMessage(socket: net.Socket, message: unknown): void {
    if (this.settings.secret !== undefined) {
      const refusal = isRecord(message) ? refuseFrame(this.settings.secret, message, this.settings.now()) : 'not an object';
      if (refusal !== undefined) {
        this.fail(socket, new TransportError('AUTH_FAILED', `${this.peerId} sent a reply that failed authentication: ${refusal}`));
        return;
      }
    }
    if (!isReply(message)) {
      this.fail(socket, new TransportError('MALFORMED_FRAME', `unexpected frame from ${this.peerId}`));
      return;
    }
    const waiter = this.waiters.get(message.id);
    if (waiter === undefined) return; // a reply that arrived after its timeout

    clearTimeout(waiter.timer);
    this.waiters.delete(message.id);
    if (message.ok) {
      waiter.resolve(message.body);
    } else {
      waiter.reject(new TransportError('REMOTE_ERROR', `${this.peerId} rejected the request: ${String(message.error)}`));
    }
  }

  /** Tears down a connection and fails every request waiting on it. */
  private fail(socket: net.Socket, error: TransportError): void {
    if (this.socket !== undefined && this.socket !== socket) {
      socket.destroy();
      return;
    }
    this.socket = undefined;
    socket.destroy();
    for (const waiter of this.waiters.values()) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    this.waiters.clear();
  }
}

/** A {@link PeerTransport} that sends RPCs over TCP. Unreachable peers reject with a TransportError. */
export class TcpPeerTransport implements PeerTransport {
  private readonly links = new Map<string, Link>();

  constructor(options: TcpPeerTransportOptions) {
    const numeric = {
      requestTimeoutMs: options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      connectTimeoutMs: options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
      maxFrameBytes: options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES,
    };
    for (const [name, value] of Object.entries(numeric)) {
      if (!Number.isInteger(value) || value <= 0) {
        throw new RangeError(`${name} must be a positive integer, got ${value}`);
      }
    }
    const settings: LinkSettings = {
      ...numeric,
      secret: options.secret,
      now: options.now ?? Date.now,
      tls: options.tls,
    };
    for (const [peerId, address] of Object.entries(options.peers)) {
      this.links.set(peerId, new Link(peerId, address, settings));
    }
  }

  requestVote(peerId: string, request: VoteRequest): Promise<VoteResponse> {
    return this.call(peerId, 'requestVote', request, isVoteResponse);
  }

  appendEntries(peerId: string, request: AppendRequest): Promise<AppendReply> {
    return this.call(peerId, 'appendEntries', request, isAppendReply);
  }

  /**
   * Sends a snapshot as a series of chunk RPCs, each resuming at the offset the peer reports. Stops
   * when the peer has the whole snapshot, or when its term no longer matches. Fails after
   * {@link MAX_STALLED_CHUNKS} chunks in a row make no progress.
   */
  async installSnapshot(peerId: string, request: InstallSnapshotRequest): Promise<InstallSnapshotReply> {
    const { snapshot } = request;
    const bytes = encodeSnapshot(snapshot);
    let offset = 0;
    let stalled = 0;
    for (;;) {
      const chunk = sliceChunk(bytes, snapshot.term, snapshot.index, offset);
      const body: InstallSnapshotChunkRequest = { term: request.term, leaderId: request.leaderId, chunk };
      const reply = await this.call(peerId, 'installSnapshotChunk', body, isInstallSnapshotChunkReply);
      if (reply.nextOffset > bytes.length) {
        throw new TransportError('BAD_REPLY', `${peerId} asked for byte ${reply.nextOffset} of a ${bytes.length}-byte snapshot`);
      }
      if (reply.nextOffset >= bytes.length || reply.term !== request.term) {
        return { term: reply.term, installed: reply.installed };
      }
      stalled = reply.nextOffset === offset ? stalled + 1 : 0;
      if (stalled >= MAX_STALLED_CHUNKS) {
        throw new TransportError('BAD_REPLY', `${peerId} stopped accepting snapshot chunks at byte ${offset}`);
      }
      offset = reply.nextOffset;
    }
  }

  /** Closes every connection. Requests still waiting are rejected with CLOSED. */
  close(): void {
    for (const link of this.links.values()) link.close();
  }

  private async call<T>(
    peerId: string,
    method: Method,
    body: unknown,
    check: (value: unknown) => value is T,
  ): Promise<T> {
    const link = this.links.get(peerId);
    if (link === undefined) throw new TransportError('UNKNOWN_PEER', `no address for peer ${peerId}`);
    const reply = await link.request(method, body);
    if (!check(reply)) throw new TransportError('BAD_REPLY', `${peerId} sent a malformed reply to ${method}`);
    return reply;
  }
}

// ---------------------------------------------------------------------------
// Real clock
// ---------------------------------------------------------------------------

/** A {@link Scheduler} backed by the real clock. */
export function systemScheduler(): Scheduler {
  const timers = new Map<TimerId, ReturnType<typeof setTimeout>>();
  let nextId = 1;
  return {
    schedule(ms: number, task: () => void): TimerId {
      const id = nextId++;
      timers.set(
        id,
        setTimeout(() => {
          timers.delete(id);
          task();
        }, ms),
      );
      return id;
    },
    cancel(id: TimerId): void {
      const timer = timers.get(id);
      if (timer !== undefined) {
        clearTimeout(timer);
        timers.delete(id);
      }
    },
  };
}
