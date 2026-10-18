/**
 * End-to-end snapshot test: a three-member cluster over real sockets. One member is stopped, the
 * others commit enough entries to compact past where it stopped, and then it restarts with empty
 * state. It can only catch up by receiving a snapshot.
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
const COMPACT_THRESHOLD = 5;
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

async function waitFor(condition: () => boolean, timeoutMs = 8_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await sleep(10);
  }
}

async function buildConfigs(): Promise<MeshNodeConfig[]> {
  const ids = ['node-1', 'node-2', 'node-3'];
  const rpc = new Map<string, number>();
  const http = new Map<string, number>();
  for (const id of ids) {
    rpc.set(id, await freePort());
    http.set(id, await freePort());
  }
  const secretPath = writeSecret();
  return ids.map((id) =>
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
}

const reported: string[] = [];

function startOptions(config: MeshNodeConfig) {
  return {
    compactThreshold: COMPACT_THRESHOLD,
    onError: (error: unknown, where: string) =>
      reported.push(`${config.nodeId} ${where}: ${error instanceof Error ? error.message : String(error)}`),
  };
}

async function start(config: MeshNodeConfig): Promise<MeshNodeHandle> {
  const handle = await startMeshNode(config, startOptions(config));
  cleanups.push(() => handle.stop());
  return handle;
}

function isAddrInUse(error: unknown): boolean {
  if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'EADDRINUSE') return true;
  return String(error).includes('EADDRINUSE');
}

/**
 * Starts the three members. A free port is only reserved while its probe is open, so another test can
 * take it first; on that collision, stop what started and try again with fresh ports.
 */
async function startCluster(): Promise<{ configs: MeshNodeConfig[]; handles: Map<string, MeshNodeHandle> }> {
  for (let attempt = 1; ; attempt++) {
    const configs = await buildConfigs();
    const started: MeshNodeHandle[] = [];
    try {
      for (const config of configs) started.push(await startMeshNode(config, startOptions(config)));
    } catch (error) {
      await Promise.all(started.map((handle) => handle.stop()));
      if (attempt >= 3 || !isAddrInUse(error)) throw error;
      continue;
    }
    for (const handle of started) cleanups.push(() => handle.stop());
    const handles = new Map<string, MeshNodeHandle>();
    for (const handle of started) handles.set(handle.nodeId, handle);
    return { configs, handles };
  }
}

describe('snapshot catch-up over real sockets', () => {
  it('brings a restarted member up to date with a snapshot once the log has been compacted past it', async () => {
    // Real timers and sockets need more than the default 5 seconds on a loaded machine.
    const { configs, handles } = await startCluster();

    await waitFor(() => [...handles.values()].some((h) => h.node.state === 'LEADER'));
    const leader = [...handles.values()].find((h) => h.node.state === 'LEADER');
    if (leader === undefined) throw new Error('no leader');
    const laggard = [...handles.values()].find((h) => h !== leader);
    if (laggard === undefined) throw new Error('no follower');
    const laggardConfig = configs.find((c) => c.nodeId === laggard.nodeId);
    if (laggardConfig === undefined) throw new Error('no config');

    // Stop one follower before anything is written to it.
    await laggard.stop();
    handles.delete(laggard.nodeId);

    for (let i = 1; i <= 20; i++) {
      leader.node.proposeCommand('SET', { key: `k${i}`, value: { n: i } });
    }
    const survivors = [...handles.values()];
    // The leader's barrier is index 1, so the twenty writes end at index 21.
    await waitFor(() => survivors.every((h) => h.store.lastApplied === 21));
    await waitFor(() => leader.node.snapshotIndex > 0);
    expect(leader.node.snapshotIndex).toBeGreaterThan(0);

    // Restart with empty state. Its log cannot be extended from the leader's log, so the snapshot path is needed.
    const restarted = await start(laggardConfig);
    handles.set(restarted.nodeId, restarted);

    await waitFor(() => restarted.store.lastApplied === 21).catch((error: unknown) => {
      const state = { restarted: restarted.node.getStateSummary(), leader: leader.node.getStateSummary() };
      throw new Error(`${String(error)}\n${JSON.stringify(state, null, 1)}\nreported:\n${reported.join('\n')}`);
    });
    expect(restarted.node.snapshotIndex).toBeGreaterThan(0);
    expect(restarted.store.snapshot()).toEqual(leader.store.snapshot());

    // Entries written after the snapshot still replicate normally.
    leader.node.proposeCommand('SET', { key: 'after', value: 'snapshot' });
    await waitFor(() => restarted.store.get('after') === 'snapshot').catch((error: unknown) => {
      const state = {
        restarted: restarted.node.getStateSummary(),
        leader: leader.node.getStateSummary(),
        restartedApplied: restarted.store.lastApplied,
        leaderApplied: leader.store.lastApplied,
      };
      throw new Error(`${String(error)}\n${JSON.stringify(state, null, 1)}\nreported:\n${reported.join('\n')}`);
    });
    expect(restarted.store.lastApplied).toBe(22);
    expect(restarted.node.leaderId).toBe(leader.node.nodeId);
  }, 20_000);
});
