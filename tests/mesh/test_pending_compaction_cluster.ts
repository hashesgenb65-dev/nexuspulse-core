/**
 * Background compaction in a three-member cluster over real sockets, with a data directory for each
 * member. A follower is stopped, the others compact past where it stopped, and it restarts with its
 * state gone, so it can only catch up from a snapshot.
 *
 * While this runs, every member's state.json on disk is sampled. Whenever it refers to a snapshot,
 * that snapshot file must already exist (ordering rule R1). The timing of a background write is not
 * controlled here, so the test checks the invariant at every sample rather than assuming a write was
 * in flight when the follower came back.
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
const COMPACT_THRESHOLD = 5;
// 20 values of 16 KiB keep one AppendEntries frame under the 1 MiB peer frame limit.
const VALUE_BYTES = 16 * 1024;
const cleanups: Array<() => Promise<void>> = [];
const dirs: string[] = [];
const reported: string[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function tempDir(prefix: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  dirs.push(dir);
  return dir;
}

function writeSecret(): string {
  const file = path.join(tempDir('mesh-secret-'), 'peer.secret');
  fs.writeFileSync(file, 'ab'.repeat(32), { mode: 0o600 });
  return file;
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

async function buildConfigs(): Promise<{ configs: MeshNodeConfig[]; dataDirs: Map<string, string> }> {
  const ids = ['node-1', 'node-2', 'node-3'];
  const rpc = new Map<string, number>();
  const http = new Map<string, number>();
  for (const id of ids) {
    rpc.set(id, await freePort());
    http.set(id, await freePort());
  }
  const secretPath = writeSecret();
  const dataDirs = new Map<string, string>();
  const configs = ids.map((id) => {
    const dataDir = tempDir(`mesh-data-${id}-`);
    dataDirs.set(id, dataDir);
    return parseNodeConfig({
      peerSecretFile: secretPath,
      nodeId: id,
      dataDir,
      rpc: { host: HOST, port: rpc.get(id) },
      http: { host: HOST, port: http.get(id) },
      peers: ids
        .filter((other) => other !== id)
        .map((other) => ({ id: other, host: HOST, rpcPort: rpc.get(other), httpPort: http.get(other) })),
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
      requestTimeoutMs: 300,
    });
  });
  return { configs, dataDirs };
}

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

/**
 * Returns a description of every member whose state.json refers to a snapshot file that is missing,
 * or an empty list when the ordering rule holds. A state.json that is not yet written is skipped.
 */
function orderingViolations(dataDirs: Map<string, string>): string[] {
  const violations: string[] = [];
  for (const [id, dir] of dataDirs) {
    const file = path.join(dir, 'state.json');
    let state: { snapshotIndex?: number };
    try {
      state = JSON.parse(fs.readFileSync(file, 'utf8')) as { snapshotIndex?: number };
    } catch {
      continue;
    }
    const index = state.snapshotIndex ?? 0;
    if (index > 0 && !fs.existsSync(path.join(dir, `snapshot-${index}.json`))) {
      violations.push(`${id}: state.json refers to snapshot ${index}, which is missing`);
    }
  }
  return violations;
}

describe('background compaction over real sockets', () => {
  it('keeps state.json consistent with the snapshot files, and catches a restarted member up from a snapshot', async () => {
    const { configs, dataDirs } = await buildConfigs();
    const handles = new Map<string, MeshNodeHandle>();
    for (const config of configs) handles.set(config.nodeId, await start(config));

    const violations: string[] = [];
    const sampler = setInterval(() => violations.push(...orderingViolations(dataDirs)), 5);
    try {
      await waitFor(() => [...handles.values()].some((h) => h.node.state === 'LEADER'));
      const leader = [...handles.values()].find((h) => h.node.state === 'LEADER');
      if (leader === undefined) throw new Error('no leader');
      const laggard = [...handles.values()].find((h) => h !== leader);
      if (laggard === undefined) throw new Error('no follower');
      const laggardConfig = configs.find((c) => c.nodeId === laggard.nodeId);
      if (laggardConfig === undefined) throw new Error('no config');

      await laggard.stop();
      handles.delete(laggard.nodeId);
      fs.rmSync(dataDirs.get(laggard.nodeId)!, { recursive: true, force: true });
      fs.mkdirSync(dataDirs.get(laggard.nodeId)!, { mode: 0o700 });

      // Larger values make each background write take a little longer, so a write is more likely to be in flight.
      const value = 'x'.repeat(VALUE_BYTES);
      for (let i = 1; i <= 20; i++) {
        leader.node.proposeCommand('SET', { key: `k${i}`, value: { n: i, pad: value } });
      }
      const survivors = [...handles.values()];
      await waitFor(() => survivors.every((h) => h.store.lastApplied === 21)).catch((error: unknown) => {
        const state = survivors.map((h) => ({ id: h.nodeId, applied: h.store.lastApplied, node: h.node.getStateSummary() }));
        throw new Error(`${String(error)}\n${JSON.stringify(state)}\nreported:\n${reported.join('\n')}`);
      });
      await waitFor(() => leader.node.snapshotIndex > 0).catch((error: unknown) => {
        throw new Error(`${String(error)}\nleader snapshotIndex ${leader.node.snapshotIndex}\nreported:\n${reported.join('\n')}`);
      });

      const restarted = await start(laggardConfig);
      handles.set(restarted.nodeId, restarted);
      await waitFor(() => restarted.store.lastApplied === 21).catch((error: unknown) => {
        throw new Error(`${String(error)}\nreported:\n${reported.join('\n')}`);
      });
      expect(restarted.node.snapshotIndex).toBeGreaterThan(0);
      expect(restarted.store.snapshot()).toEqual(leader.store.snapshot());

      leader.node.proposeCommand('SET', { key: 'after', value: 'snapshot' });
      await waitFor(() => restarted.store.get('after') === 'snapshot');
    } finally {
      clearInterval(sampler);
    }

    expect(violations).toEqual([]);
    expect(reported.filter((line) => line.includes('snapshot'))).toEqual([]);
  }, 30_000);
});
