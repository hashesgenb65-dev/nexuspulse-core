/**
 * A mesh member started with an auditFile: committed writes are stored, survive a restart, and a
 * damaged audit file stops start-up before any port is bound.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as net from 'node:net';
import * as os from 'node:os';
import * as path from 'node:path';
import type { AddressInfo } from 'node:net';
import { AuditStoreError } from '../../src/audit/durable_audit';
import { MeshConfigError, parseNodeConfig, type MeshNodeConfig } from '../../src/mesh/node_config';
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

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-audit-'));
  dirs.push(dir);
  return dir;
}

async function configWith(auditFile: string): Promise<MeshNodeConfig> {
  return parseNodeConfig({
    nodeId: 'node-1',
    auditFile,
    rpc: { host: HOST, port: await freePort() },
    http: { host: HOST, port: await freePort() },
    peers: [],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
    requestTimeoutMs: 300,
  });
}

async function start(auditFile: string): Promise<MeshNodeHandle> {
  const handle = await startMeshNode(await configWith(auditFile));
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

describe('mesh member with an audit file', () => {
  it('stores a committed write, and the record is still there after a restart', async () => {
    const auditFile = path.join(tempDir(), 'audit.log');

    const first = await start(auditFile);
    await waitFor(() => first.node.state === 'LEADER');
    const written = await fetch(`http://${HOST}:${first.httpPort}/v1/kv/sensor`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ value: { temp: 21 } }),
    });
    expect(written.status).toBe(200);
    await first.stop();

    const second = await start(auditFile);
    const listed = await fetch(`http://${HOST}:${second.httpPort}/v1/audit`);
    const body = (await listed.json()) as { total: number; records: Array<{ resource: string; action: string }> };
    expect(body.total).toBe(1);
    expect(body.records[0]).toMatchObject({ action: 'SET', resource: 'kv:sensor' });
    expect(fs.readFileSync(auditFile, 'utf8')).not.toContain('"temp":21');
  });

  it('refuses to start from an audit file whose records were altered', async () => {
    const dir = tempDir();
    const auditFile = path.join(dir, 'audit.log');
    fs.writeFileSync(
      auditFile,
      `${JSON.stringify({
        recordId: 'aud_1',
        timestamp: 1,
        actorId: 'x',
        action: 'SET',
        resource: 'kv:a',
        changes: {},
        previousHash: '0'.repeat(64),
        recordHash: 'f'.repeat(64),
      })}\n`,
    );

    await expect(startMeshNode(await configWith(auditFile))).rejects.toBeInstanceOf(AuditStoreError);
  });

  it('rejects an auditFile that is not a non-empty string in the config', () => {
    expect(() => parseNodeConfig({ nodeId: 'n', auditFile: '' })).toThrow(MeshConfigError);
    expect(() => parseNodeConfig({ nodeId: 'n', auditFile: 42 })).toThrow(MeshConfigError);
  });
});
