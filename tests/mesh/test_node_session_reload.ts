/**
 * SIGHUP on a running member reloads the auth state file: a key revoked in the file stops logging in
 * without a restart. A broken file is reported and the member keeps its current keys.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { FileAuthStateStore } from '../../src/auth/auth_state_store';
import { AuthManager, type ScopePermission } from '../../src/auth/manager';
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

async function loginStatus(port: number, apiKey: string): Promise<number> {
  const response = await fetch(`http://${HOST}:${port}/v1/sessions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ apiKey }),
  });
  return response.status;
}

describe('SIGHUP on a running member', () => {
  it('stops a key revoked in the file from logging in, without a restart', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'node-reload-'));
    dirs.push(dir);
    const file = path.join(dir, 'auth-state.json');
    const seeded = new AuthManager({ stateStore: new FileAuthStateStore(file) });
    const key = seeded.createApiKey('t1', 'ANALYST', scopes);

    const handle: MeshNodeHandle = await startMeshNode(await configWith(file));
    cleanups.push(() => handle.stop());
    expect(await loginStatus(handle.httpPort, key.rawApiKey)).toBe(201);

    // The file is changed underneath the running member. The lock only stops the key command, not this.
    new AuthManager({ stateStore: new FileAuthStateStore(file) }).revokeApiKey(key.keyId);
    process.emit('SIGHUP');

    expect(await loginStatus(handle.httpPort, key.rawApiKey)).toBe(401);
  });

  it('reports a broken file, refuses logins that need a save, and leaves the file alone', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'node-reload-'));
    dirs.push(dir);
    const file = path.join(dir, 'auth-state.json');
    const key = new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey('t1', 'ANALYST', scopes);

    const errors: string[] = [];
    const handle = await startMeshNode(await configWith(file), {
      onError: (_error, where) => errors.push(where),
    });
    cleanups.push(() => handle.stop());

    fs.writeFileSync(file, '{ broken', { mode: 0o600 });
    process.emit('SIGHUP');

    expect(errors).toContain('session reload');
    // A login needs a saved session, and the broken file is not overwritten, so the login is refused.
    expect(await loginStatus(handle.httpPort, key.rawApiKey)).toBe(503);
    expect(fs.readFileSync(file, 'utf8')).toBe('{ broken');
  });
});
