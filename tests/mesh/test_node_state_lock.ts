/**
 * The auth state file's lock: a running member holds it, the key command refuses to write while it is
 * held, a member refuses to start while another holder has it, and a failed start releases it.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { runApiKeyCli } from '../../src/cli/api_key_cli';
import { AuthStateError } from '../../src/auth/auth_state_store';
import { acquireStateLock, lockPathFor, StateLockError } from '../../src/auth/state_lock';
import { AuthManager, type ScopePermission } from '../../src/auth/manager';
import { FileAuthStateStore } from '../../src/auth/auth_state_store';
import { parseNodeConfig, type MeshNodeConfig } from '../../src/mesh/node_config';
import { startMeshNode, type MeshNodeHandle } from '../../src/mesh/node_runner';

const HOST = '127.0.0.1';
const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];
const cleanups: Array<() => Promise<void>> = [];
const dirs: string[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function stateFile(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'state-lock-'));
  dirs.push(dir);
  return path.join(dir, 'auth-state.json');
}

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

async function configWith(file: string): Promise<MeshNodeConfig> {
  return parseNodeConfig({
    nodeId: 'node-1',
    authStateFile: file,
    rpc: { host: HOST, port: await freePort() },
    http: { host: HOST, port: await freePort() },
    peers: [],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
    requestTimeoutMs: 300,
  });
}

async function start(config: MeshNodeConfig): Promise<MeshNodeHandle> {
  const handle = await startMeshNode(config);
  cleanups.push(() => handle.stop());
  return handle;
}

function createKey(file: string): number {
  return runApiKeyCli(['create', file, 'tenant-a', 'READ_ONLY', 'metrics:read'], { write: () => undefined, error: () => undefined });
}

describe('a running member', () => {
  it('holds the lock while it runs, and releases it when it stops', async () => {
    const file = stateFile();
    new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey('t1', 'READ_ONLY', scopes);
    const handle = await start(await configWith(file));
    expect(fs.existsSync(lockPathFor(file))).toBe(true);

    await handle.stop();
    expect(fs.existsSync(lockPathFor(file))).toBe(false);
  });

  it('makes the key command refuse to write, and lets it write again after the member stops', async () => {
    const file = stateFile();
    new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey('t1', 'READ_ONLY', scopes);
    const handle = await start(await configWith(file));

    expect(createKey(file)).toBe(1);
    await handle.stop();
    expect(createKey(file)).toBe(0);
  });

  it('refuses to start while another holder has the lock, and binds no port', async () => {
    const file = stateFile();
    new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey('t1', 'READ_ONLY', scopes);
    const config = await configWith(file);
    const release = acquireStateLock(file);
    try {
      await expect(startMeshNode(config)).rejects.toBeInstanceOf(StateLockError);
    } finally {
      release();
    }
    // The ports were free before, and they are still free: nothing was bound.
    await expect(
      new Promise<void>((resolve, reject) => {
        const probe = net.createServer();
        probe.once('error', reject);
        probe.listen(config.http.port, HOST, () => probe.close(() => resolve()));
      }),
    ).resolves.toBeUndefined();
  });

  it('releases the lock when start-up fails after taking it', async () => {
    const file = stateFile();
    new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey('t1', 'READ_ONLY', scopes);
    fs.chmodSync(file, 0o644);

    await expect(startMeshNode(await configWith(file))).rejects.toBeInstanceOf(AuthStateError);
    expect(fs.existsSync(lockPathFor(file))).toBe(false);
  });
});
