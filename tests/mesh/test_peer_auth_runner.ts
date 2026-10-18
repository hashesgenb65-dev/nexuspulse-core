/**
 * Peer authentication at the node level: a member with peers must have a secret, and a member with
 * the wrong secret cannot join the cluster.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { PeerAuthError } from '../../src/mesh/peer_auth';
import { parseNodeConfig, type MeshNodeConfig } from '../../src/mesh/node_config';
import { startMeshNode, type MeshNodeHandle } from '../../src/mesh/node_runner';

const HOST = '127.0.0.1';
const cleanups: Array<() => Promise<void>> = [];
const dirs: string[] = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function secretFile(hexByte: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-auth-'));
  dirs.push(dir);
  const file = path.join(dir, 'peer.secret');
  fs.writeFileSync(file, hexByte.repeat(32), { mode: 0o600 });
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

function isAddrInUse(error: unknown): boolean {
  if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'EADDRINUSE') return true;
  return String(error).includes('EADDRINUSE');
}

/**
 * Starts every member. A free port is only reserved while its probe is open, so another test can
 * take it first; on that collision, stop what started and try again with fresh ports.
 */
async function startCluster(secrets: Record<string, string>): Promise<MeshNodeHandle[]> {
  for (let attempt = 1; ; attempt++) {
    const handles: MeshNodeHandle[] = [];
    try {
      for (const config of await buildConfigs(secrets)) {
        handles.push(await startMeshNode(config));
      }
    } catch (error) {
      await Promise.all(handles.map((handle) => handle.stop()));
      if (attempt >= 3 || !isAddrInUse(error)) throw error;
      continue;
    }
    for (const handle of handles) cleanups.push(() => handle.stop());
    return handles;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function buildConfigs(secrets: Record<string, string>): Promise<MeshNodeConfig[]> {
  const ids = Object.keys(secrets);
  const rpc = new Map<string, number>();
  const http = new Map<string, number>();
  for (const id of ids) {
    rpc.set(id, await freePort());
    http.set(id, await freePort());
  }
  return ids.map((id) => {
    const secret = secrets[id];
    if (secret === undefined) throw new Error('no secret');
    return parseNodeConfig({
      nodeId: id,
      peerSecretFile: secretFile(secret),
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
}

describe('peer authentication at node level', () => {
  it('refuses to start with peers and no secret, before binding any port', async () => {
    const [config] = await buildConfigs({ 'node-1': 'ab', 'node-2': 'ab' });
    if (config === undefined) throw new Error('no config');
    const withoutSecret = parseNodeConfig({
      nodeId: config.nodeId,
      rpc: config.rpc,
      http: config.http,
      peers: [...config.peers],
    });

    await expect(startMeshNode(withoutSecret)).rejects.toThrowError(PeerAuthError);
    await expect(startMeshNode(withoutSecret)).rejects.toThrowError(/needs peerSecretFile/);
  });

  it('keeps a member with the wrong secret out of the cluster, while the others elect a leader', async () => {
    const handles = await startCluster({ 'node-1': 'ab', 'node-2': 'ab', 'node-3': 'cd' });
    const [good1, good2, outsider] = handles;
    if (good1 === undefined || good2 === undefined || outsider === undefined) throw new Error('no handles');

    const deadline = Date.now() + 5_000;
    while (![good1, good2].some((h) => h.node.state === 'LEADER')) {
      if (Date.now() > deadline) throw new Error('no leader among the members that share the secret');
      await sleep(20);
    }
    await sleep(500);

    expect(outsider.node.state).not.toBe('LEADER');
    expect(outsider.node.leaderId).toBeNull();
    expect([good1, good2].filter((h) => h.node.state === 'LEADER')).toHaveLength(1);
  });
});
