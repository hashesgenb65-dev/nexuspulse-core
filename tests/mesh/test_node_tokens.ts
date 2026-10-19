/**
 * A mesh member started with a tokensFile: its key-value API asks for a bearer token, and the
 * config rejects a tokensFile that is not a non-empty string.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { writeTokenFile } from '../../src/app/token_file';
import { MeshConfigError, parseNodeConfig, type MeshNodeConfig } from '../../src/mesh/node_config';
import { startMeshNode, type MeshNodeHandle } from '../../src/mesh/node_runner';

const HOST = '127.0.0.1';
const READER = 'reader-secret-0123456789abcd';
const WRITER = 'writer-secret-0123456789abcd';
const cleanups: Array<() => Promise<void>> = [];
const dirs: string[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.once('error', reject);
    probe.listen(0, HOST, () => {
      const { port } = probe.address() as AddressInfo;
      probe.close(() => resolve(port));
    });
  });
}

function tokensFile(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-tokens-'));
  dirs.push(dir);
  const file = path.join(dir, 'tokens.json');
  writeTokenFile(file, [
    { name: 'dash', role: 'reader', secret: READER },
    { name: 'edge', role: 'writer', secret: WRITER },
  ]);
  return file;
}

async function configWith(tokens: string): Promise<MeshNodeConfig> {
  return parseNodeConfig({
    nodeId: 'node-1',
    tokensFile: tokens,
    rpc: { host: HOST, port: await freePort() },
    http: { host: HOST, port: await freePort() },
    peers: [],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
    requestTimeoutMs: 300,
  });
}

function isAddrInUse(error: unknown): boolean {
  if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'EADDRINUSE') return true;
  return String(error).includes('EADDRINUSE');
}

/** Starts the member, retrying with fresh ports if another test took one first. */
async function start(tokens: string): Promise<MeshNodeHandle> {
  for (let attempt = 1; ; attempt++) {
    try {
      const handle = await startMeshNode(await configWith(tokens));
      cleanups.push(() => handle.stop());
      return handle;
    } catch (error) {
      if (attempt >= 3 || !isAddrInUse(error)) throw error;
    }
  }
}

describe('mesh member with a tokens file', () => {
  it('answers 401 without a token and serves the status with a reader token', async () => {
    const handle = await start(tokensFile());
    const url = `http://${HOST}:${handle.httpPort}/v1/status`;

    const anonymous = await fetch(url);
    expect(anonymous.status).toBe(401);
    expect(anonymous.headers.get('www-authenticate')).toBe('Bearer realm="nexuspulse"');

    const reader = await fetch(url, { headers: { authorization: `Bearer ${READER}` } });
    expect(reader.status).toBe(200);
  });

  it('refuses a write from a reader and accepts the same write from a writer', async () => {
    const handle = await start(tokensFile());
    const url = `http://${HOST}:${handle.httpPort}/v1/kv/color`;
    const body = JSON.stringify({ value: 'blue' });

    const asReader = await fetch(url, {
      method: 'PUT',
      headers: { authorization: `Bearer ${READER}`, 'content-type': 'application/json' },
      body,
    });
    expect(asReader.status).toBe(403);

    const asWriter = await fetch(url, {
      method: 'PUT',
      headers: { authorization: `Bearer ${WRITER}`, 'content-type': 'application/json' },
      body,
    });
    expect(asWriter.status).not.toBe(401);
    expect(asWriter.status).not.toBe(403);
  });
});

describe('tokensFile in the config', () => {
  it('rejects an empty or non-string tokensFile', () => {
    const base = { nodeId: 'node-1', rpc: { host: HOST, port: 1 }, http: { host: HOST, port: 2 }, peers: [] };
    expect(() => parseNodeConfig({ ...base, tokensFile: '' })).toThrowError(MeshConfigError);
    expect(() => parseNodeConfig({ ...base, tokensFile: 7 })).toThrowError(/tokensFile/);
  });
});
