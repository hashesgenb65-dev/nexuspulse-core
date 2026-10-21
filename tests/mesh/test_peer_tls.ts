/**
 * TLS between mesh members. Each run generates two self-signed certificates with the openssl CLI in a
 * temporary directory: one that the client trusts, and one it does not. Real sockets carry the RPCs.
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { execFileSync } from 'node:child_process';
import { PeerServer, TcpPeerTransport, TransportError } from '../../src/mesh/peer_transport';
import { loadTlsCredentials, type TlsCredentials } from '../../src/ingest/tls_credentials';
import { MIN_SECRET_BYTES } from '../../src/mesh/peer_auth';

const HOST = '127.0.0.1';
const SECRET = Buffer.alloc(MIN_SECRET_BYTES, 3);
const vote = { term: 1, candidateId: 'client', lastLogIndex: 0, lastLogTerm: 0 };
const handler = {
  handleVoteRequest: () => ({ term: 1, voteGranted: true }),
  handleAppendRequest: () => ({ term: 1, success: true }),
};

let dir: string;
let trusted: { cert: Buffer; credentials: TlsCredentials };
let untrusted: { cert: Buffer; credentials: TlsCredentials };
const cleanups: Array<() => void | Promise<void>> = [];

function makeCertificate(name: string): { cert: Buffer; credentials: TlsCredentials } {
  const certPath = path.join(dir, `${name}-cert.pem`);
  const keyPath = path.join(dir, `${name}-key.pem`);
  execFileSync(
    'openssl',
    [
      'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes',
      '-keyout', keyPath, '-out', certPath, '-days', '1', '-subj', `/CN=${name}`,
      '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
    ],
    { stdio: 'ignore' },
  );
  fs.chmodSync(keyPath, 0o600);
  return { cert: fs.readFileSync(certPath), credentials: loadTlsCredentials({ certPath, keyPath }) };
}

beforeAll(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'peer-tls-'));
  trusted = makeCertificate('trusted');
  untrusted = makeCertificate('untrusted');
});

afterAll(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function serve(tlsCredentials: TlsCredentials | undefined, secret?: Buffer): Promise<number> {
  const server = new PeerServer({
    handler,
    ...(secret === undefined ? {} : { secret }),
    ...(tlsCredentials === undefined ? {} : { tls: tlsCredentials }),
  });
  const port = await server.listen(0, HOST);
  cleanups.push(() => server.close());
  return port;
}

function client(
  port: number,
  options: { readonly ca?: Buffer; readonly secret?: Buffer } = {},
): TcpPeerTransport {
  const transport = new TcpPeerTransport({
    peers: { server: { host: HOST, port } },
    requestTimeoutMs: 1_000,
    connectTimeoutMs: 1_000,
    ...(options.secret === undefined ? {} : { secret: options.secret }),
    ...(options.ca === undefined ? {} : { tls: { ca: options.ca } }),
  });
  cleanups.push(() => transport.close());
  return transport;
}

describe('peer RPCs over TLS', () => {
  it('completes a vote request when the client trusts the server certificate', async () => {
    const port = await serve(trusted.credentials);
    const transport = client(port, { ca: trusted.cert });

    await expect(transport.requestVote('server', vote)).resolves.toEqual({ term: 1, voteGranted: true });
  });

  it('keeps authentication working on top of TLS', async () => {
    const port = await serve(trusted.credentials, SECRET);
    const transport = client(port, { ca: trusted.cert, secret: SECRET });

    await expect(transport.requestVote('server', vote)).resolves.toEqual({ term: 1, voteGranted: true });
  });

  it('refuses a server whose certificate the client does not trust', async () => {
    const port = await serve(untrusted.credentials);
    const transport = client(port, { ca: trusted.cert });

    await expect(transport.requestVote('server', vote)).rejects.toMatchObject({ code: 'UNREACHABLE' });
  });

  it('a clear-text client cannot reach a TLS server', async () => {
    const port = await serve(trusted.credentials);
    const transport = client(port);

    // The TLS server reads the clear-text frame as a broken TLS record and closes the socket. The
    // client sees that as a transport failure, never as a reply.
    await expect(transport.requestVote('server', vote)).rejects.toBeInstanceOf(TransportError);
  });

  it('a TLS client cannot reach a clear-text server', async () => {
    const port = await serve(undefined);
    const transport = client(port, { ca: trusted.cert });

    await expect(transport.requestVote('server', vote)).rejects.toMatchObject({ code: 'UNREACHABLE' });
  });

  it('clear-text members still talk to each other when neither side configures TLS', async () => {
    const port = await serve(undefined);
    const transport = client(port);

    await expect(transport.requestVote('server', vote)).resolves.toEqual({ term: 1, voteGranted: true });
  });
});
