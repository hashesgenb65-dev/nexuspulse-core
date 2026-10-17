/**
 * Tests for the TCP peer transport: framing, server validation, client error handling,
 * reconnection, and a three-node cluster running over real sockets and the real clock.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import { RaftConsensusNode, type AppendRequest, type LogEntry, type VoteRequest } from '../../src/mesh/consensus';
import { ConsensusDriver } from '../../src/mesh/driver';
import {
  DEFAULT_MAX_FRAME_BYTES,
  encodeFrame,
  FrameDecoder,
  PeerServer,
  systemScheduler,
  TcpPeerTransport,
  TransportError,
  type RpcHandler,
} from '../../src/mesh/peer_transport';

const LOCALHOST = '127.0.0.1';
const cleanups: Array<() => Promise<void> | void> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function track<T extends { close(): unknown }>(resource: T): T {
  cleanups.push(async () => {
    await resource.close();
  });
  return resource;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(condition: () => boolean, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await sleep(10);
  }
}

interface Recorder {
  readonly handler: RpcHandler;
  readonly votes: VoteRequest[];
  readonly appends: AppendRequest[];
  failNextVote: string | undefined;
}

/** A handler that grants every vote and accepts every append, recording what it was sent. */
function recorder(): Recorder {
  const rec: Recorder = {
    votes: [],
    appends: [],
    failNextVote: undefined,
    handler: {
      handleVoteRequest: (request) => {
        rec.votes.push(request);
        if (rec.failNextVote !== undefined) {
          const message = rec.failNextVote;
          rec.failNextVote = undefined;
          throw new Error(message);
        }
        return { term: request.term, voteGranted: true };
      },
      handleAppendRequest: (request) => {
        rec.appends.push(request);
        return { term: request.term, success: true };
      },
    },
  };
  return rec;
}

async function startServer(handler: RpcHandler, port = 0): Promise<{ server: PeerServer; port: number }> {
  const server = track(new PeerServer({ handler }));
  const bound = await server.listen(port, LOCALHOST);
  return { server, port: bound };
}

/** A raw TCP server with a custom connection handler, for simulating misbehaving peers. */
async function rawServer(onConnection: (socket: net.Socket) => void): Promise<{ port: number; close: () => Promise<void> }> {
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => undefined);
    onConnection(socket);
  });
  await new Promise<void>((resolve) => server.listen(0, LOCALHOST, () => resolve()));
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('raw server did not bind');
  return {
    port: address.port,
    close: () =>
      new Promise<void>((resolve) => {
        for (const socket of sockets) socket.destroy();
        server.close(() => resolve());
      }),
  };
}

function connectRaw(port: number): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const socket = net.connect({ host: LOCALHOST, port });
    socket.once('connect', () => resolve(socket));
    socket.once('error', reject);
    cleanups.push(() => void socket.destroy());
  });
}

/** Reads frames from a raw socket until `count` have arrived. */
function readFrames(socket: net.Socket, count: number, timeoutMs = 2_000): Promise<unknown[]> {
  return new Promise((resolve, reject) => {
    const decoder = new FrameDecoder();
    const frames: unknown[] = [];
    const timer = setTimeout(() => reject(new Error(`only ${frames.length} of ${count} frames arrived`)), timeoutMs);
    socket.on('data', (chunk: Buffer) => {
      frames.push(...decoder.push(chunk));
      if (frames.length >= count) {
        clearTimeout(timer);
        resolve(frames.slice(0, count));
      }
    });
  });
}

function voteRequest(term: number, candidateId = 'node_a'): VoteRequest {
  return { term, candidateId, lastLogIndex: 0, lastLogTerm: 0 };
}

function entry(index: number, term: number, payload: unknown = {}): LogEntry {
  return { index, term, command: 'SET', payload };
}

function expectThrownCode(fn: () => unknown, code: string): void {
  let thrown: unknown;
  try {
    fn();
  } catch (error) {
    thrown = error;
  }
  expect(thrown).toBeInstanceOf(TransportError);
  expect((thrown as TransportError).code).toBe(code);
}

async function expectCode(promise: Promise<unknown>, code: string): Promise<void> {
  const error = await promise.then(
    () => undefined,
    (reason: unknown) => reason,
  );
  expect(error).toBeInstanceOf(TransportError);
  expect((error as TransportError).code).toBe(code);
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

describe('framing', () => {
  it('reassembles frames split across chunks and reads several from one chunk', () => {
    const first = encodeFrame({ n: 1 });
    const second = encodeFrame({ n: 2, text: 'héllo' });
    const joined = Buffer.concat([first, second]);

    const decoder = new FrameDecoder();
    expect(decoder.push(joined.subarray(0, 3))).toEqual([]);
    // The first frame is still one byte short here, so nothing is complete yet.
    expect(decoder.push(joined.subarray(3, first.length - 1))).toEqual([]);
    expect(decoder.push(joined.subarray(first.length - 1))).toEqual([{ n: 1 }, { n: 2, text: 'héllo' }]);
  });

  it('refuses to encode a value above the limit', () => {
    expectThrownCode(() => encodeFrame({ text: 'x'.repeat(100) }, 50), 'FRAME_TOO_LARGE');
  });

  it('refuses a header that announces a frame above the limit', () => {
    const header = Buffer.alloc(4);
    header.writeUInt32BE(DEFAULT_MAX_FRAME_BYTES + 1, 0);
    expectThrownCode(() => new FrameDecoder().push(header), 'FRAME_TOO_LARGE');
  });

  it('refuses a body that is not JSON', () => {
    const body = Buffer.from('{not json', 'utf8');
    const header = Buffer.alloc(4);
    header.writeUInt32BE(body.length, 0);
    expectThrownCode(() => new FrameDecoder().push(Buffer.concat([header, body])), 'MALFORMED_FRAME');
  });
});

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

describe('round trips', () => {
  it('carries a vote request and returns the reply', async () => {
    const rec = recorder();
    const { port } = await startServer(rec.handler);
    const transport = track(new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port } } }));

    const reply = await transport.requestVote('b', voteRequest(2));
    expect(reply).toEqual({ term: 2, voteGranted: true });
    expect(rec.votes).toEqual([voteRequest(2)]);
  });

  it('carries entries with their payloads intact', async () => {
    const rec = recorder();
    const { port } = await startServer(rec.handler);
    const transport = track(new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port } } }));

    const request: AppendRequest = {
      term: 3,
      leaderId: 'node_a',
      prevLogIndex: 0,
      prevLogTerm: 0,
      leaderCommit: 0,
      entries: [entry(1, 3, { nested: [1, 'two', true, null] })],
    };
    const reply = await transport.appendEntries('b', request);
    expect(reply).toEqual({ term: 3, success: true });
    expect(rec.appends).toEqual([request]);
  });

  it('answers many concurrent requests on one connection', async () => {
    const rec = recorder();
    const { port } = await startServer(rec.handler);
    const transport = track(new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port } } }));

    const replies = await Promise.all(Array.from({ length: 20 }, (_, i) => transport.requestVote('b', voteRequest(i + 1))));
    expect(replies.map((r) => r.term)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
    expect(rec.votes).toHaveLength(20);
  });

  it('rejects a peer it has no address for', async () => {
    const transport = track(new TcpPeerTransport({ peers: {} }));
    await expectCode(transport.requestVote('nobody', voteRequest(1)), 'UNKNOWN_PEER');
  });

  it('rejects an invalid transport configuration', () => {
    expect(() => new TcpPeerTransport({ peers: {}, requestTimeoutMs: 0 })).toThrowError(RangeError);
  });
});

// ---------------------------------------------------------------------------
// Server validation and failures
// ---------------------------------------------------------------------------

describe('server validation', () => {
  it('reports a handler failure to the caller and keeps the connection usable', async () => {
    const rec = recorder();
    rec.failNextVote = 'boom';
    const { port } = await startServer(rec.handler);
    const transport = track(new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port } } }));

    await expect(transport.requestVote('b', voteRequest(1))).rejects.toMatchObject({ code: 'REMOTE_ERROR' });
    await expect(transport.requestVote('b', voteRequest(2))).resolves.toEqual({ term: 2, voteGranted: true });
  });

  it('answers a malformed request with an error reply and keeps the connection open', async () => {
    const { port } = await startServer(recorder().handler);
    const socket = await connectRaw(port);

    socket.write(encodeFrame({ type: 'request', id: 7, method: 'requestVote', body: { term: 'x' } }));
    socket.write(encodeFrame({ type: 'request', id: 8, method: 'requestVote', body: voteRequest(1) }));

    const [bad, good] = await readFrames(socket, 2);
    expect(bad).toEqual({ type: 'reply', id: 7, ok: false, error: 'invalid request' });
    expect(good).toEqual({ type: 'reply', id: 8, ok: true, body: { term: 1, voteGranted: true } });
  });

  it('answers an unknown method with an error reply', async () => {
    const { port } = await startServer(recorder().handler);
    const socket = await connectRaw(port);

    socket.write(encodeFrame({ type: 'request', id: 3, method: 'bogusMethod', body: {} }));
    const [reply] = await readFrames(socket, 1);
    expect(reply).toEqual({ type: 'reply', id: 3, ok: false, error: 'invalid request' });
  });

  it('closes a connection that sends a frame that is not JSON', async () => {
    const { port } = await startServer(recorder().handler);
    const socket = await connectRaw(port);
    const body = Buffer.from('{oops', 'utf8');
    const header = Buffer.alloc(4);
    header.writeUInt32BE(body.length, 0);

    const closed = new Promise<void>((resolve) => socket.once('close', () => resolve()));
    socket.write(Buffer.concat([header, body]));
    await closed;
  });

  it('closes a connection that announces an oversized frame', async () => {
    const { port } = await startServer(recorder().handler);
    const socket = await connectRaw(port);
    const header = Buffer.alloc(4);
    header.writeUInt32BE(DEFAULT_MAX_FRAME_BYTES + 1, 0);

    const closed = new Promise<void>((resolve) => socket.once('close', () => resolve()));
    socket.write(header);
    await closed;
  });
});

// ---------------------------------------------------------------------------
// Client failures
// ---------------------------------------------------------------------------

describe('client failures', () => {
  it('times out when the peer never replies', async () => {
    const silent = await rawServer(() => undefined);
    cleanups.push(silent.close);
    const transport = track(
      new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port: silent.port } }, requestTimeoutMs: 100 }),
    );
    await expectCode(transport.requestVote('b', voteRequest(1)), 'TIMEOUT');
  });

  it('reports UNREACHABLE when nothing is listening on the address', async () => {
    const probe = track(new PeerServer({ handler: recorder().handler }));
    const port = await probe.listen(0, LOCALHOST);
    await probe.close();

    const transport = track(new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port } } }));
    await expectCode(transport.requestVote('b', voteRequest(1)), 'UNREACHABLE');
  });

  it('reports CONNECTION_LOST when the peer drops the connection mid-request', async () => {
    const dropping = await rawServer((socket) => {
      socket.on('data', () => socket.destroy());
    });
    cleanups.push(dropping.close);
    const transport = track(new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port: dropping.port } } }));
    await expectCode(transport.requestVote('b', voteRequest(1)), 'CONNECTION_LOST');
  });

  it('reports BAD_REPLY when the peer answers with the wrong shape', async () => {
    const wrongShape = await rawServer((socket) => {
      const decoder = new FrameDecoder();
      socket.on('data', (chunk: Buffer) => {
        for (const message of decoder.push(chunk)) {
          const id = (message as { id: number }).id;
          socket.write(encodeFrame({ type: 'reply', id, ok: true, body: { term: 'x' } }));
        }
      });
    });
    cleanups.push(wrongShape.close);
    const transport = track(new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port: wrongShape.port } } }));
    await expectCode(transport.requestVote('b', voteRequest(1)), 'BAD_REPLY');
  });

  it('reconnects after the peer restarts on the same port', async () => {
    const first = recorder();
    const started = await startServer(first.handler);
    const transport = track(new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port: started.port } } }));
    await expect(transport.requestVote('b', voteRequest(1))).resolves.toEqual({ term: 1, voteGranted: true });

    await started.server.close();
    await sleep(50);
    await startServer(recorder().handler, started.port);

    await expect(transport.requestVote('b', voteRequest(2))).resolves.toEqual({ term: 2, voteGranted: true });
  });

  it('rejects requests made after close', async () => {
    const { port } = await startServer(recorder().handler);
    const transport = new TcpPeerTransport({ peers: { b: { host: LOCALHOST, port } } });
    await transport.requestVote('b', voteRequest(1));
    transport.close();
    await expectCode(transport.requestVote('b', voteRequest(2)), 'CLOSED');
  });
});

// ---------------------------------------------------------------------------
// Real clock
// ---------------------------------------------------------------------------

describe('systemScheduler', () => {
  it('runs a task after its delay and not after it is cancelled', async () => {
    const scheduler = systemScheduler();
    const ran: string[] = [];
    scheduler.schedule(10, () => ran.push('kept'));
    const cancelled = scheduler.schedule(10, () => ran.push('cancelled'));
    scheduler.cancel(cancelled);

    expect(ran).toEqual([]);
    await sleep(40);
    expect(ran).toEqual(['kept']);
  });
});

// ---------------------------------------------------------------------------
// Cluster over real sockets
// ---------------------------------------------------------------------------

interface LiveNode {
  readonly id: string;
  readonly node: RaftConsensusNode;
  readonly driver: ConsensusDriver;
  readonly transport: TcpPeerTransport;
  readonly server: PeerServer;
}

/** Starts a cluster on localhost with real timers. Each node runs a server and a driver. */
async function startLiveCluster(ids: string[]): Promise<Map<string, LiveNode>> {
  const drivers = new Map<string, ConsensusDriver>();
  const ports = new Map<string, number>();
  const servers = new Map<string, PeerServer>();

  for (const id of ids) {
    const server = track(
      new PeerServer({
        handler: {
          handleVoteRequest: (request) => requireDriver(drivers, id).handleVoteRequest(request),
          handleAppendRequest: (request) => requireDriver(drivers, id).handleAppendRequest(request),
        },
      }),
    );
    ports.set(id, await server.listen(0, LOCALHOST));
    servers.set(id, server);
  }

  const live = new Map<string, LiveNode>();
  for (const id of ids) {
    const peerIds = ids.filter((other) => other !== id);
    const peers: Record<string, { host: string; port: number }> = {};
    for (const other of peerIds) {
      const port = ports.get(other);
      if (port === undefined) throw new Error(`no port for ${other}`);
      peers[other] = { host: LOCALHOST, port };
    }

    const transport = track(new TcpPeerTransport({ peers, requestTimeoutMs: 500, connectTimeoutMs: 500 }));
    const node = new RaftConsensusNode({
      nodeId: id,
      clusterPeers: peerIds,
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
    });
    const driver = new ConsensusDriver({
      node,
      transport,
      scheduler: systemScheduler(),
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
    });
    drivers.set(id, driver);
    const server = servers.get(id);
    if (server === undefined) throw new Error(`no server for ${id}`);
    live.set(id, { id, node, driver, transport, server });
  }

  for (const member of live.values()) member.driver.start();
  cleanups.push(() => {
    for (const member of live.values()) member.driver.stop();
  });
  return live;
}

function requireDriver(drivers: Map<string, ConsensusDriver>, id: string): ConsensusDriver {
  const driver = drivers.get(id);
  if (driver === undefined) throw new Error(`no driver ${id}`);
  return driver;
}

function leadersOf(live: Map<string, LiveNode>): LiveNode[] {
  return [...live.values()].filter((member) => member.node.state === 'LEADER');
}

describe('three nodes over real sockets', () => {
  it('elects one leader and commits a command on every node', async () => {
    const live = await startLiveCluster(['node_1', 'node_2', 'node_3']);

    await waitFor(() => leadersOf(live).length === 1);
    const [leader] = leadersOf(live);
    if (leader === undefined) throw new Error('no leader');

    leader.node.proposeCommand('SET', { key: 'a', value: 1 });
    await waitFor(() => [...live.values()].every((member) => member.node.commitIndex === 1));

    for (const member of live.values()) {
      expect(member.node.log.map((e) => e.command)).toEqual(['SET']);
      expect(member.node.leaderId).toBe(leader.id);
    }
  });

  it('elects a new leader when the old one is stopped', async () => {
    const live = await startLiveCluster(['node_1', 'node_2', 'node_3']);
    await waitFor(() => leadersOf(live).length === 1);
    const [oldLeader] = leadersOf(live);
    if (oldLeader === undefined) throw new Error('no leader');
    const oldTerm = oldLeader.node.currentTerm;

    oldLeader.driver.stop();
    await oldLeader.server.close();
    oldLeader.transport.close();

    const survivors = new Map([...live].filter(([id]) => id !== oldLeader.id));
    await waitFor(() => leadersOf(survivors).length === 1);
    const [newLeader] = leadersOf(survivors);
    expect(newLeader?.node.currentTerm).toBeGreaterThan(oldTerm);
  });
});
