/**
 * Tests for the mesh node config: validation with every issue reported, defaults, and file loading.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  DEFAULT_TIMING,
  MAX_CONFIG_BYTES,
  MeshConfigError,
  loadNodeConfig,
  parseNodeConfig,
} from '../../src/mesh/node_config';

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function tempFile(name: string, contents: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-config-'));
  tempDirs.push(dir);
  const file = path.join(dir, name);
  fs.writeFileSync(file, contents);
  return file;
}

function validConfig(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    nodeId: 'node-1',
    rpc: { host: '127.0.0.1', port: 7001 },
    http: { host: '127.0.0.1', port: 8001 },
    peers: [
      { id: 'node-2', host: '127.0.0.2', rpcPort: 7002, httpPort: 8002 },
      { id: 'node-3', host: '127.0.0.3', rpcPort: 7003, httpPort: 8003 },
    ],
    electionTimeoutMs: 1000,
    heartbeatIntervalMs: 100,
    requestTimeoutMs: 500,
    ...overrides,
  };
}

function issuesOf(raw: unknown): string[] {
  try {
    parseNodeConfig(raw);
  } catch (error) {
    if (error instanceof MeshConfigError) return error.issues.map((issue) => issue.path);
    throw error;
  }
  throw new Error('expected the config to be rejected');
}

describe('parseNodeConfig', () => {
  it('accepts a complete config and keeps its values', () => {
    const config = parseNodeConfig(validConfig());
    expect(config.nodeId).toBe('node-1');
    expect(config.rpc).toEqual({ host: '127.0.0.1', port: 7001 });
    expect(config.http).toEqual({ host: '127.0.0.1', port: 8001 });
    expect(config.peers.map((p) => p.id)).toEqual(['node-2', 'node-3']);
    expect(config.peers[0]).toEqual({ id: 'node-2', host: '127.0.0.2', rpcPort: 7002, httpPort: 8002 });
    expect(config.electionTimeoutMs).toBe(1000);
    expect(config.heartbeatIntervalMs).toBe(100);
    expect(config.requestTimeoutMs).toBe(500);
  });

  it('fills in default timings and an empty peer list when they are left out', () => {
    const config = parseNodeConfig({
      nodeId: 'solo',
      rpc: { host: '127.0.0.1', port: 0 },
      http: { host: '127.0.0.1', port: 0 },
    });
    expect(config.peers).toEqual([]);
    expect(config.electionTimeoutMs).toBe(DEFAULT_TIMING.electionTimeoutMs);
    expect(config.heartbeatIntervalMs).toBe(DEFAULT_TIMING.heartbeatIntervalMs);
    expect(config.requestTimeoutMs).toBe(DEFAULT_TIMING.requestTimeoutMs);
  });

  it('reports every problem in one error, in field order', () => {
    const paths = issuesOf({
      nodeId: 'Bad Id',
      rpc: { host: '', port: 70000 },
      http: 'not an object',
      peers: [
        { id: 'node-1', host: 'h', rpcPort: 1, httpPort: 1 },
        { id: 'node-2', host: 'h', rpcPort: 0, httpPort: 1 },
        { id: 'node-2', host: 'h', rpcPort: 1, httpPort: 1 },
      ],
      electionTimeoutMs: 100,
      heartbeatIntervalMs: 200,
    });
    expect(paths).toEqual([
      'nodeId',
      'rpc.host',
      'rpc.port',
      'http',
      'peers[1].rpcPort',
      'peers[2].id',
      'heartbeatIntervalMs',
    ]);
  });

  it('rejects a node that lists itself as a peer', () => {
    expect(issuesOf(validConfig({ peers: [{ id: 'node-1', host: 'h', rpcPort: 1, httpPort: 1 }] }))).toEqual([
      'peers[0].id',
    ]);
  });

  it('rejects a non-object root and a non-array peers field', () => {
    expect(issuesOf(['not', 'an', 'object'])).toEqual(['$']);
    expect(issuesOf(validConfig({ peers: 'node-2' }))).toEqual(['peers']);
  });

  it('rejects too many peers', () => {
    const peers = Array.from({ length: 33 }, (_, i) => ({
      id: `node-${i + 2}`,
      host: 'h',
      rpcPort: 1,
      httpPort: 1,
    }));
    expect(issuesOf(validConfig({ peers }))).toEqual(['peers']);
  });

  it('rejects timings that are not positive integers or are out of range', () => {
    expect(issuesOf(validConfig({ electionTimeoutMs: 1.5 }))).toEqual(['electionTimeoutMs']);
    expect(issuesOf(validConfig({ requestTimeoutMs: 0 }))).toEqual(['requestTimeoutMs']);
    expect(issuesOf(validConfig({ electionTimeoutMs: 61_000 }))).toEqual(['electionTimeoutMs']);
  });

  it('rejects a peer port of 0, which is only allowed for this node', () => {
    expect(issuesOf(validConfig({ peers: [{ id: 'node-2', host: 'h', rpcPort: 0, httpPort: 1 }] }))).toEqual([
      'peers[0].rpcPort',
    ]);
  });
});

describe('loadNodeConfig', () => {
  it('reads and validates a config file', () => {
    const file = tempFile('node.json', JSON.stringify(validConfig()));
    expect(loadNodeConfig(file).nodeId).toBe('node-1');
  });

  it('reports a missing file', () => {
    const missing = path.join(os.tmpdir(), 'no-such-mesh-config-file.json');
    expect(() => loadNodeConfig(missing)).toThrowError(MeshConfigError);
  });

  it('reports a directory given in place of a file', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-config-dir-'));
    tempDirs.push(dir);
    expect(() => loadNodeConfig(dir)).toThrowError(/cannot be read as a file/);
  });

  it('reports text that is not JSON', () => {
    const file = tempFile('broken.json', '{ nodeId: ');
    expect(() => loadNodeConfig(file)).toThrowError(/is not valid JSON/);
  });

  it('refuses a file over the size limit before reading it', () => {
    const file = tempFile('huge.json', ' '.repeat(MAX_CONFIG_BYTES + 1));
    expect(() => loadNodeConfig(file)).toThrowError(/is larger than/);
  });

  it('gives the error code MESH_CONFIG', () => {
    try {
      parseNodeConfig('nope');
      throw new Error('expected a rejection');
    } catch (error) {
      expect(error).toBeInstanceOf(MeshConfigError);
      expect((error as MeshConfigError).code).toBe('MESH_CONFIG');
    }
  });
});
