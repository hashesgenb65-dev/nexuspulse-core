/**
 * Restart tests for a member with a data directory: a restarted node keeps its term and its
 * committed writes, and a corrupt state file stops start-up before any port is bound.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { DurableStateError, STATE_FILE_NAME } from '../../src/mesh/durable_store';
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

async function singleMemberConfig(dataDir: string): Promise<MeshNodeConfig> {
  return parseNodeConfig({
    nodeId: 'solo-1',
    rpc: { host: HOST, port: await freePort() },
    http: { host: HOST, port: await freePort() },
    peers: [],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
    requestTimeoutMs: 300,
    dataDir,
  });
}

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'durable-restart-'));
  dirs.push(dir);
  return dir;
}

async function start(config: MeshNodeConfig): Promise<MeshNodeHandle> {
  const handle = await startMeshNode(config);
  cleanups.push(() => handle.stop());
  return handle;
}

describe('single member with a data directory', () => {
  it('keeps its term and its committed writes across a restart', async () => {
    const config = await singleMemberConfig(tempDir());

    const first = await start(config);
    await waitFor(() => first.node.state === 'LEADER');
    const written = await fetch(`http://${HOST}:${first.httpPort}/v1/kv/kept`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ value: 42 }),
    });
    expect(written.status).toBe(200);
    const termBefore = first.node.currentTerm;
    await first.stop();

    const second = await start(config);
    await waitFor(() => second.node.state === 'LEADER');
    expect(second.node.currentTerm).toBeGreaterThan(termBefore);

    // The entry comes back from disk. It applies once the new term's barrier commits.
    await waitFor(() => second.store.has('kept'));
    expect(second.store.get('kept')).toBe(42);
  });

  it('refuses to start from a corrupt state file', async () => {
    const dataDir = tempDir();
    fs.writeFileSync(path.join(dataDir, STATE_FILE_NAME), '{"version":1,');
    const config = await singleMemberConfig(dataDir);

    await expect(startMeshNode(config)).rejects.toThrowError(DurableStateError);
  });
});
