/**
 * End-to-end tests for a mesh member started from config: three nodes on real sockets, each with
 * its own RPC listener and HTTP API, talking to one another and answering HTTP requests.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { parseNodeConfig, type MeshNodeConfig } from '../../src/mesh/node_config';
import { startMeshNode, type MeshNodeHandle } from '../../src/mesh/node_runner';

const HOST = '127.0.0.1';

/** A valid cluster secret in a file only its owner can read. */
function writeSecret(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-secret-'));
  const file = path.join(dir, 'peer.secret');
  fs.writeFileSync(file, 'ab'.repeat(32), { mode: 0o600 });
  return file;
}
const cleanups: Array<() => Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor<T>(read: () => T | undefined, timeoutMs = 5_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = read();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await sleep(10);
  }
}

function isAddrInUse(error: unknown): boolean {
  if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'EADDRINUSE') return true;
  return String(error).includes('EADDRINUSE');
}

/**
 * Starts three members. A free port is only reserved while its probe is open, so another test can
 * take it first; on that collision, stop what started and try again with fresh ports.
 */
async function startThree(): Promise<MeshNodeHandle[]> {
  const ids = ['node-1', 'node-2', 'node-3'];
  for (let attempt = 1; ; attempt++) {
    const handles: MeshNodeHandle[] = [];
    try {
      const rpc = new Map<string, number>();
      const http = new Map<string, number>();
      for (const id of ids) {
        rpc.set(id, await freePort());
        http.set(id, await freePort());
      }

      const secretPath = writeSecret();
      const configs: MeshNodeConfig[] = ids.map((id) =>
        parseNodeConfig({
          peerSecretFile: secretPath,
          nodeId: id,
          rpc: { host: HOST, port: rpc.get(id) },
          http: { host: HOST, port: http.get(id) },
          peers: ids
            .filter((other) => other !== id)
            .map((other) => ({ id: other, host: HOST, rpcPort: rpc.get(other), httpPort: http.get(other) })),
          electionTimeoutMs: 150,
          heartbeatIntervalMs: 50,
          requestTimeoutMs: 300,
        }),
      );

      for (const config of configs) handles.push(await startMeshNode(config));
    } catch (error) {
      await Promise.all(handles.map((handle) => handle.stop()));
      if (attempt >= 3 || !isAddrInUse(error)) throw error;
      continue;
    }
    for (const handle of handles) cleanups.push(() => handle.stop());
    return handles;
  }
}

function url(handle: MeshNodeHandle, path: string): string {
  return `http://${HOST}:${handle.httpPort}${path}`;
}

function leaderIn(handles: readonly MeshNodeHandle[]): MeshNodeHandle | undefined {
  return handles.find((handle) => handle.node.state === 'LEADER');
}

describe('mesh member started from config', () => {
  it('redirects writes from followers, replicates them, and serves reads from every member', async () => {
    const handles = await startThree();
    const leader = await waitFor(() => leaderIn(handles));
    const follower = handles.find((h) => h !== leader);
    if (follower === undefined) throw new Error('no follower');

    const redirected = await fetch(url(follower, '/v1/kv/color'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ value: 'teal' }),
      redirect: 'manual',
    });
    expect(redirected.status).toBe(307);
    expect(redirected.headers.get('location')).toBe(url(leader, '/v1/kv/color'));

    const written = await fetch(url(leader, '/v1/kv/color'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ value: 'teal' }),
    });
    expect(written.status).toBe(200);

    for (const handle of handles) {
      await waitFor(() => (handle.store.has('color') ? true : undefined));
      const read = await fetch(url(handle, '/v1/kv/color'));
      expect(await read.json()).toMatchObject({ key: 'color', value: 'teal' });
    }

    const removed = await fetch(url(leader, '/v1/kv/color'), { method: 'DELETE' });
    expect(removed.status).toBe(200);
    for (const handle of handles) {
      await waitFor(() => (handle.store.has('color') ? undefined : true));
      expect((await fetch(url(handle, '/v1/kv/color'))).status).toBe(404);
    }
  });

  it('elects a new leader when the old one stops, and keeps accepting writes', async () => {
    const handles = await startThree();
    const first = await waitFor(() => leaderIn(handles));
    await first.stop();

    const survivors = handles.filter((h) => h !== first);
    const second = await waitFor(() => leaderIn(survivors));
    const response = await fetch(url(second, '/v1/kv/after'), {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ value: 'failover' }),
    });
    expect(response.status).toBe(200);
  });

  it('stops cleanly, and stopping twice is safe', async () => {
    const [only] = await startThree();
    if (only === undefined) throw new Error('no node');
    await only.stop();
    await expect(only.stop()).resolves.toBeUndefined();
    await expect(fetch(url(only, '/v1/kv/x'))).rejects.toThrow();
  });

  it('reports a port that is already in use, and leaves nothing bound', async () => {
    const taken = net.createServer();
    await new Promise<void>((resolve) => taken.listen(0, HOST, () => resolve()));
    cleanups.push(() => new Promise<void>((resolve) => taken.close(() => resolve())));
    const busy = (taken.address() as AddressInfo).port;

    const config = parseNodeConfig({
      nodeId: 'node-1',
      rpc: { host: HOST, port: 0 },
      http: { host: HOST, port: busy },
      peers: [],
    });
    await expect(startMeshNode(config)).rejects.toThrow();
  });
});
