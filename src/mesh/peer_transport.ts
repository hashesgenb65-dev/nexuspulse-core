/**
 * TCP transport for the consensus driver: the network half of {@link PeerTransport}.
 *
 * Wire format. Each message is one frame: a 4-byte big-endian length followed by that many
 * bytes of UTF-8 JSON. A request is `{ type: 'request', id, method, body }`. A reply is
 * `{ type: 'reply', id, ok: true, body }` or `{ type: 'reply', id, ok: false, error }`. The id
 * ties each reply to its request, so replies can arrive in any order on one connection.
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
  | 'CLOSED';

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

type Method = 'requestVote' | 'appendEntries' | 'installSnapshot';

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

function isInstallSnapshotRequest(value: unknown): value is InstallSnapshotRequest {
  if (!isRecord(value) || !isNonNegativeInteger(value.term) || typeof value.leaderId !== 'string') return false;
  const { snapshot } = value;
  return (
    isRecord(snapshot) &&
    isNonNegativeInteger(snapshot.index) &&
    isNonNegativeInteger(snapshot.term) &&
    'data' in snapshot
  );
}

function isInstallSnapshotReply(value: unknown): value is InstallSnapshotReply {
  return isRecord(value) && isNonNegativeInteger(value.term) && typeof value.installed === 'boolean';
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
  if (message.method === 'installSnapshot' && isInstallSnapshotRequest(message.body)) {
    return { id: message.id, method: 'installSnapshot', body: message.body };
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
  /** Receives failures on connections. `where` says which part failed. */
  onError?: (error: unknown, where: string) => void;
}

/** Accepts peer connections and answers their RPCs by calling the handler. */
export class PeerServer {
  private readonly server: net.Server;
  private readonly sockets = new Set<net.Socket>();
  private readonly handler: RpcHandler;
  private readonly maxFrameBytes: number;
  private readonly onError: ((error: unknown, where: string) => void) | undefined;

  constructor(options: PeerServerOptions) {
    this.handler = options.handler;
    this.maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
    this.onError = options.onError;
    this.server = net.createServer((socket) => this.onConnection(socket));
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
      for (const message of messages) this.dispatch(socket, message);
    });
    socket.on('error', (error) => this.report(error, 'socket'));
    socket.on('close', () => this.sockets.delete(socket));
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
      case 'installSnapshot': {
        const handle = this.handler.handleSnapshotRequest;
        if (handle === undefined) throw new Error('snapshots are not supported by this node');
        return handle.call(this.handler, request.body as InstallSnapshotRequest);
      }
    }
  }

  private send(socket: net.Socket, frame: unknown): void {
    if (socket.destroyed) return;
    socket.write(encodeFrame(frame, this.maxFrameBytes));
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
}

interface LinkSettings {
  readonly requestTimeoutMs: number;
  readonly connectTimeoutMs: number;
  readonly maxFrameBytes: number;
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
    const frame = encodeFrame({ type: 'request', id, method, body }, this.settings.maxFrameBytes);

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

      const socket = net.connect({ host: this.address.host, port: this.address.port });
      timer = setTimeout(() => {
        fail(new TransportError('UNREACHABLE', `connecting to ${this.peerId} timed out`));
      }, this.settings.connectTimeoutMs);
      socket.once('error', onConnectError);
      socket.once('connect', () => {
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
    const settings: LinkSettings = {
      requestTimeoutMs: options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      connectTimeoutMs: options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
      maxFrameBytes: options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES,
    };
    for (const [name, value] of Object.entries(settings)) {
      if (!Number.isInteger(value) || value <= 0) {
        throw new RangeError(`${name} must be a positive integer, got ${value}`);
      }
    }
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

  installSnapshot(peerId: string, request: InstallSnapshotRequest): Promise<InstallSnapshotReply> {
    return this.call(peerId, 'installSnapshot', request, isInstallSnapshotReply);
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
