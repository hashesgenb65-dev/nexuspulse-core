/**
 * TLS on the TCP ingest port. The certificate is generated with the openssl CLI into a temporary
 * directory for each run, so no key material is committed. Real TLS sockets carry the frames.
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import * as tls from 'node:tls';
import { execFileSync } from 'node:child_process';
import { once } from 'node:events';
import { createDefaultEngine, type TelemetryEvent } from '../../src/core/engine';
import { TcpIngestServer, encodeAuthRecord, encodeTelemetryRecord } from '../../src/ingest/tcp_ingest';
import {
  TlsConfigError,
  loadTlsCredentials,
  type TlsCredentials,
} from '../../src/ingest/tls_credentials';
import { createClientTransport, FrameFlags, FrameType, type FrameTransport } from '../../src/grpc/transport';
import { TokenAuth, type TokenSpec } from '../../src/api/auth';

const NOW = 1_700_000_000_000;
const WRITER = 'writer-secret-0123456789abcd';
const specs: TokenSpec[] = [{ name: 'edge', role: 'writer', secret: WRITER }];

let dir: string;
let certPath: string;
let keyPath: string;
let credentials: TlsCredentials;
const cleanups: Array<() => void | Promise<void>> = [];

beforeAll(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ingest-tls-'));
  certPath = path.join(dir, 'cert.pem');
  keyPath = path.join(dir, 'key.pem');
  execFileSync(
    'openssl',
    [
      'req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes',
      '-keyout', keyPath, '-out', certPath, '-days', '1', '-subj', '/CN=localhost',
      '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
    ],
    { stdio: 'ignore' },
  );
  fs.chmodSync(keyPath, 0o600);
  credentials = loadTlsCredentials({ certPath, keyPath });
});

afterAll(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function event(id: string): TelemetryEvent {
  return { id, source: 'edge-01', metric: 'cpu', value: 10, timestamp: NOW - 1_000 };
}

async function startTls(tlsOption: TlsCredentials | undefined): Promise<{ port: number; server: TcpIngestServer }> {
  const engine = createDefaultEngine({ clock: () => NOW });
  cleanups.push(() => {
    if (engine.metrics().state !== 'stopped') engine.stop();
  });
  const server = new TcpIngestServer({
    engine,
    auth: new TokenAuth(specs),
    ...(tlsOption === undefined ? {} : { tls: tlsOption }),
  });
  cleanups.push(() => server.close());
  const port = await server.listen(0, '127.0.0.1');
  return { port, server };
}

async function connectTls(port: number, ca: Buffer): Promise<{ socket: tls.TLSSocket; transport: FrameTransport }> {
  const socket = tls.connect({ host: '127.0.0.1', port, ca, servername: 'localhost' });
  cleanups.push(() => {
    socket.destroy();
  });
  await once(socket, 'secureConnect');
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

describe('loadTlsCredentials', () => {
  it('loads a certificate and key that are readable only by the owner', () => {
    expect(credentials.cert.toString('latin1')).toContain('-----BEGIN CERTIFICATE-----');
    expect(credentials.key.toString('latin1')).toContain('-----BEGIN PRIVATE KEY-----');
  });

  it('refuses a key file that other users can read', () => {
    const loose = path.join(dir, 'loose-key.pem');
    fs.copyFileSync(keyPath, loose);
    fs.chmodSync(loose, 0o644);
    expect(() => loadTlsCredentials({ certPath, keyPath: loose })).toThrow(TlsConfigError);
    expect(() => loadTlsCredentials({ certPath, keyPath: loose })).toThrow(/mode 0600/);
  });

  it('refuses a missing file without echoing its path', () => {
    const missing = path.join(dir, 'does-not-exist.pem');
    expect(() => loadTlsCredentials({ certPath: missing, keyPath })).toThrow(TlsConfigError);
    try {
      loadTlsCredentials({ certPath: missing, keyPath });
    } catch (error) {
      expect((error as Error).message).not.toContain(missing);
    }
  });

  it('refuses a file that is not PEM encoded', () => {
    const notPem = path.join(dir, 'not-pem.txt');
    fs.writeFileSync(notPem, 'hello');
    expect(() => loadTlsCredentials({ certPath: notPem, keyPath })).toThrow(/not PEM encoded/);
  });

  it('refuses empty paths', () => {
    expect(() => loadTlsCredentials({ certPath: '', keyPath })).toThrow(TlsConfigError);
  });
});

describe('TCP ingest over TLS', () => {
  it('accepts events from a client that completes TLS and sends a writer token', async () => {
    const cert = fs.readFileSync(certPath);
    const { port, server } = await startTls(credentials);
    const { transport } = await connectTls(port, cert);

    sendData(transport, encodeAuthRecord(WRITER));
    sendData(transport, encodeTelemetryRecord(event('evt-1')));

    await waitFor(() => server.stats().eventsAccepted === 1);
    expect(server.stats().authFailures).toBe(0);
  });

  it('refuses a client that does not trust the certificate', async () => {
    const { port } = await startTls(credentials);
    const socket = tls.connect({ host: '127.0.0.1', port, servername: 'localhost' });
    cleanups.push(() => {
      socket.destroy();
    });
    const [error] = (await once(socket, 'error')) as [Error];
    expect(error).toBeInstanceOf(Error);
  });

  it('does not speak the clear-text protocol to a client that skips TLS', async () => {
    const { port, server } = await startTls(credentials);
    const socket = net.connect({ host: '127.0.0.1', port });
    cleanups.push(() => {
      socket.destroy();
    });
    await once(socket, 'connect');
    const transport = createClientTransport(socket, { keepAliveIntervalMs: 0 });
    sendData(transport, encodeAuthRecord(WRITER));
    sendData(transport, encodeTelemetryRecord(event('evt-plain')));

    await once(socket, 'close');
    expect(server.stats().eventsAccepted).toBe(0);
  });

  it('refuses TLS 1.1 and older', async () => {
    const cert = fs.readFileSync(certPath);
    const { port } = await startTls(credentials);
    const socket = tls.connect({
      host: '127.0.0.1',
      port,
      ca: cert,
      servername: 'localhost',
      minVersion: 'TLSv1',
      maxVersion: 'TLSv1.1',
    });
    cleanups.push(() => {
      socket.destroy();
    });
    const [error] = (await once(socket, 'error')) as [Error];
    expect(error).toBeInstanceOf(Error);
  });
});

describe('TCP ingest without TLS', () => {
  it('still accepts clear-text connections when no TLS credentials are given', async () => {
    const { port, server } = await startTls(undefined);
    const socket = net.connect({ host: '127.0.0.1', port });
    cleanups.push(() => {
      socket.destroy();
    });
    await once(socket, 'connect');
    const transport = createClientTransport(socket, { keepAliveIntervalMs: 0 });
    sendData(transport, encodeAuthRecord(WRITER));
    sendData(transport, encodeTelemetryRecord(event('evt-clear')));

    await waitFor(() => server.stats().eventsAccepted === 1);
  });
});
