/**
 * Peer authentication over real sockets: matching secrets talk, and a wrong, missing, stale or
 * tampered tag is refused. A refused request gets no reply from the server.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import { encodeFrame } from '../../src/mesh/peer_transport';
import { MIN_SECRET_BYTES, authenticate } from '../../src/mesh/peer_auth';
import { PeerServer, TcpPeerTransport } from '../../src/mesh/peer_transport';

const HOST = '127.0.0.1';
const SECRET = Buffer.alloc(MIN_SECRET_BYTES, 1);
const OTHER = Buffer.alloc(MIN_SECRET_BYTES, 2);
const cleanups: Array<() => void | Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

const handler = {
  handleVoteRequest: () => ({ term: 1, voteGranted: true }),
  handleAppendRequest: () => ({ term: 1, success: true }),
};

async function serve(secret: Buffer | undefined): Promise<number> {
  const server = new PeerServer({ handler, secret });
  const port = await server.listen(0, HOST);
  cleanups.push(() => server.close());
  return port;
}

function client(port: number, secret: Buffer | undefined, now?: () => number): TcpPeerTransport {
  const transport = new TcpPeerTransport({
    peers: { server: { host: HOST, port } },
    requestTimeoutMs: 500,
    connectTimeoutMs: 500,
    secret,
    now,
  });
  cleanups.push(() => transport.close());
  return transport;
}

const vote = { term: 1, candidateId: 'client', lastLogIndex: 0, lastLogTerm: 0 };

describe('peer authentication over TCP', () => {
  it('lets members with the same secret exchange RPCs in both directions', async () => {
    const port = await serve(SECRET);
    const transport = client(port, SECRET);

    await expect(transport.requestVote('server', vote)).resolves.toEqual({ term: 1, voteGranted: true });
    await expect(
      transport.appendEntries('server', {
        term: 1,
        leaderId: 'client',
        prevLogIndex: 0,
        prevLogTerm: 0,
        entries: [],
        leaderCommit: 0,
      }),
    ).resolves.toEqual({ term: 1, success: true });
  });

  it('drops the connection when the caller has the wrong secret', async () => {
    const port = await serve(SECRET);
    const transport = client(port, OTHER);

    await expect(transport.requestVote('server', vote)).rejects.toMatchObject({ code: 'CONNECTION_LOST' });
  });

  it('drops the connection when the caller sends no tag', async () => {
    const port = await serve(SECRET);
    const transport = client(port, undefined);

    await expect(transport.requestVote('server', vote)).rejects.toMatchObject({ code: 'CONNECTION_LOST' });
  });

  it('refuses a reply that is not tagged, when the caller expects tags', async () => {
    const port = await serve(undefined);
    const transport = client(port, SECRET);

    await expect(transport.requestVote('server', vote)).rejects.toMatchObject({ code: 'AUTH_FAILED' });
  });

  it('refuses a request whose timestamp is outside the skew window', async () => {
    const port = await serve(SECRET);
    const stale = client(port, SECRET, () => Date.now() - 120_000);

    await expect(stale.requestVote('server', vote)).rejects.toMatchObject({ code: 'CONNECTION_LOST' });
  });

  it('drops the connection on a frame whose body was changed after it was tagged', async () => {
    const port = await serve(SECRET);
    const unsigned = { type: 'request', id: 1, method: 'requestVote', body: vote };
    const tagged = { ...unsigned, body: { ...vote, term: 99 }, auth: authenticate(SECRET, unsigned, Date.now()) };

    const socket = net.connect({ host: HOST, port });
    cleanups.push(() => {
      socket.destroy();
    });
    await new Promise<void>((resolve) => socket.once('connect', () => resolve()));
    socket.write(encodeFrame(tagged));

    await new Promise<void>((resolve) => socket.once('close', () => resolve()));
    expect(socket.destroyed).toBe(true);
  });

  it('answers a correctly tagged request on a raw socket', async () => {
    const port = await serve(SECRET);
    const unsigned = { type: 'request', id: 7, method: 'requestVote', body: vote };
    const signed = { ...unsigned, auth: authenticate(SECRET, unsigned, Date.now()) };

    const socket = net.connect({ host: HOST, port });
    cleanups.push(() => {
      socket.destroy();
    });
    await new Promise<void>((resolve) => socket.once('connect', () => resolve()));
    socket.write(encodeFrame(signed));

    const reply = await new Promise<Buffer>((resolve) => socket.once('data', (chunk: Buffer) => resolve(chunk)));
    const length = reply.readUInt32BE(0);
    const body = JSON.parse(reply.subarray(4, 4 + length).toString('utf8')) as { id: number; ok: boolean };
    expect(body).toMatchObject({ id: 7, ok: true });
  });
});
