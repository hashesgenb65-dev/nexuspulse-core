/**
 * A mesh member with an authStateFile: a key created beforehand logs in over HTTP, its session works as a
 * bearer token, the key still works after a restart, and a broken state file stops start-up before any
 * port is bound.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { AuthStateError, FileAuthStateStore } from '../../src/auth/auth_state_store';
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

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'node-sessions-'));
  dirs.push(dir);
  return dir;
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

async function configWith(authStateFile: string | undefined): Promise<MeshNodeConfig> {
  return parseNodeConfig({
    nodeId: 'node-1',
    ...(authStateFile === undefined ? {} : { authStateFile }),
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

async function waitFor(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

async function login(port: number, apiKey: string): Promise<Response> {
  return fetch(`http://${HOST}:${port}/v1/sessions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ apiKey }),
  });
}

describe('mesh member with an auth state file', () => {
  it('logs in with a key created beforehand, and the session works as a bearer token', async () => {
    const file = path.join(tempDir(), 'auth-state.json');
    const { rawApiKey } = new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey(
      't1',
      'INGEST_SERVICE',
      scopes,
    );
    const handle = await start(await configWith(file));
    await waitFor(() => handle.node.state === 'LEADER');

    const response = await login(handle.httpPort, rawApiKey);
    expect(response.status).toBe(201);
    const { sessionId } = (await response.json()) as { sessionId: string };
    const written = await fetch(`http://${HOST}:${handle.httpPort}/v1/kv/sensor`, {
      method: 'PUT',
      headers: { authorization: `Bearer ${sessionId}`, 'content-type': 'application/json' },
      body: JSON.stringify({ value: 21 }),
    });
    expect(written.status).toBe(200);
  });

  it('refuses a wrong key', async () => {
    const file = path.join(tempDir(), 'auth-state.json');
    new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey('t1', 'READ_ONLY', scopes);
    const handle = await start(await configWith(file));
    expect((await login(handle.httpPort, 'np_id_0000000000000000.np_sec_nope')).status).toBe(401);
  });

  it('keeps the key working after a restart', async () => {
    const file = path.join(tempDir(), 'auth-state.json');
    const { rawApiKey } = new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey(
      't1',
      'READ_ONLY',
      scopes,
    );
    const config = await configWith(file);
    const first = await start(config);
    await first.stop();

    const second = await startMeshNode(await configWith(file));
    cleanups.push(() => second.stop());
    expect((await login(second.httpPort, rawApiKey)).status).toBe(201);
  });

  it('answers 404 to login when no auth state file is configured', async () => {
    const handle = await start(await configWith(undefined));
    expect((await login(handle.httpPort, 'np_id_0000000000000000.np_sec_x')).status).toBe(404);
  });

  it('refuses to start from a state file that other users can read, and binds no port', async () => {
    const file = path.join(tempDir(), 'auth-state.json');
    new AuthManager({ stateStore: new FileAuthStateStore(file) }).createApiKey('t1', 'READ_ONLY', scopes);
    fs.chmodSync(file, 0o644);
    const config = await configWith(file);

    await expect(startMeshNode(config)).rejects.toBeInstanceOf(AuthStateError);
    // The ports were free before, and they are still free: nothing was bound.
    await expect(
      new Promise<void>((resolve, reject) => {
        const probe = net.createServer();
        probe.once('error', reject);
        probe.listen(config.http.port, HOST, () => probe.close(() => resolve()));
      }),
    ).resolves.toBeUndefined();
  });
});
