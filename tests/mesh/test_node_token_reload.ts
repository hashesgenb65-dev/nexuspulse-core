/**
 * A mesh member with a tokens file reloads it on SIGHUP: rotated secrets work, removed names stop
 * working, and a broken file is reported while the tokens in use keep working.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { generateSecret } from '../../src/api/auth';
import { newTokenSpec, writeTokenFile } from '../../src/app/token_file';
import { parseNodeConfig, type MeshNodeConfig } from '../../src/mesh/node_config';
import { startMeshNode, type MeshNodeHandle } from '../../src/mesh/node_runner';

const HOST = '127.0.0.1';
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

async function configWith(tokensFile: string): Promise<MeshNodeConfig> {
  return parseNodeConfig({
    nodeId: 'node-1',
    tokensFile,
    rpc: { host: HOST, port: await freePort() },
    http: { host: HOST, port: await freePort() },
    peers: [],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
    requestTimeoutMs: 300,
  });
}

async function statusWith(port: number, secret: string): Promise<number> {
  const response = await fetch(`http://${HOST}:${port}/v1/kv/probe`, {
    headers: { authorization: `Bearer ${secret}` },
  });
  return response.status;
}

describe('mesh member token reload', () => {
  it('applies SIGHUP reloads, honours the grace period, and keeps tokens on a broken file', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'node-token-reload-'));
    dirs.push(dir);
    const tokensFile = path.join(dir, 'tokens.json');
    const ops = newTokenSpec('ops', 'writer');
    writeTokenFile(tokensFile, [ops]);

    const errors: Array<{ where: string }> = [];
    const handle: MeshNodeHandle = await startMeshNode(await configWith(tokensFile), {
      tokenGraceMs: 60_000,
      onError: (_error, where) => errors.push({ where }),
    });
    cleanups.push(() => handle.stop());

    expect(await statusWith(handle.httpPort, ops.secret)).not.toBe(401);

    // Rotate: the new secret works, and the old one works during the grace period.
    const rotated = generateSecret();
    fs.writeFileSync(tokensFile, JSON.stringify([{ name: 'ops', secret: rotated, role: 'writer' }]), { mode: 0o600 });
    process.emit('SIGHUP');
    expect(await statusWith(handle.httpPort, rotated)).not.toBe(401);
    expect(await statusWith(handle.httpPort, ops.secret)).not.toBe(401);

    // Remove the name: both secrets stop working at once.
    fs.writeFileSync(tokensFile, JSON.stringify([{ name: 'other', secret: generateSecret(), role: 'reader' }]), {
      mode: 0o600,
    });
    process.emit('SIGHUP');
    expect(await statusWith(handle.httpPort, rotated)).toBe(401);
    expect(await statusWith(handle.httpPort, ops.secret)).toBe(401);
  });

  it('reports a broken tokens file and keeps the tokens in use', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'node-token-reload-'));
    dirs.push(dir);
    const tokensFile = path.join(dir, 'tokens.json');
    const ops = newTokenSpec('ops', 'writer');
    writeTokenFile(tokensFile, [ops]);

    const errors: Array<{ where: string }> = [];
    const handle = await startMeshNode(await configWith(tokensFile), {
      onError: (_error, where) => errors.push({ where }),
    });
    cleanups.push(() => handle.stop());

    fs.writeFileSync(tokensFile, '[{"name": ', { mode: 0o600 });
    process.emit('SIGHUP');

    expect(errors).toEqual([{ where: 'token reload' }]);
    expect(await statusWith(handle.httpPort, ops.secret)).not.toBe(401);
  });

  it('stops reacting to SIGHUP once the member is stopped', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'node-token-reload-'));
    dirs.push(dir);
    const tokensFile = path.join(dir, 'tokens.json');
    writeTokenFile(tokensFile, [newTokenSpec('ops', 'writer')]);

    const errors: unknown[] = [];
    const handle = await startMeshNode(await configWith(tokensFile), { onError: (error) => errors.push(error) });
    const before = process.listenerCount('SIGHUP');
    await handle.stop();
    expect(process.listenerCount('SIGHUP')).toBe(before - 1);

    fs.writeFileSync(tokensFile, '[{"name": ', { mode: 0o600 });
    process.emit('SIGHUP');
    expect(errors).toEqual([]);
  });
});
