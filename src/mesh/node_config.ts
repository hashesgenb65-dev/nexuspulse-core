/**
 * Configuration for one member of a NexusPulse mesh, read from a JSON file.
 *
 * Example:
 * {
 *   "nodeId": "node-1",
 *   "rpc":  { "host": "10.0.0.1", "port": 7001 },
 *   "http": { "host": "10.0.0.1", "port": 8001 },
 *   "peers": [ { "id": "node-2", "host": "10.0.0.2", "rpcPort": 7001, "httpPort": 8001 } ],
 *   "electionTimeoutMs": 1000,
 *   "heartbeatIntervalMs": 100,
 *   "requestTimeoutMs": 500
 * }
 *
 * This node's own ports may be 0, which picks free ports. Peer ports must be real ports.
 * Every member lists all the others under `peers`. Timing fields are optional and default to
 * the values in {@link DEFAULT_TIMING}.
 */

import * as fs from 'node:fs';

export const MAX_CONFIG_BYTES = 64 * 1024;
export const MAX_PEERS = 32;
export const DEFAULT_TIMING = Object.freeze({
  electionTimeoutMs: 1_000,
  heartbeatIntervalMs: 100,
  requestTimeoutMs: 500,
});

const NODE_ID_PATTERN = /^[a-z0-9][a-z0-9-]{0,62}$/;
const MAX_TIMING_MS = 60_000;

export interface Endpoint {
  readonly host: string;
  readonly port: number;
}

export interface PeerConfig {
  readonly id: string;
  readonly host: string;
  readonly rpcPort: number;
  readonly httpPort: number;
}

export interface MeshNodeConfig {
  readonly nodeId: string;
  /** Where this node accepts peer RPCs. */
  readonly rpc: Endpoint;
  /** Where this node serves the key-value HTTP API. */
  readonly http: Endpoint;
  readonly peers: readonly PeerConfig[];
  readonly electionTimeoutMs: number;
  readonly heartbeatIntervalMs: number;
  readonly requestTimeoutMs: number;
}

export interface ConfigIssue {
  readonly path: string;
  readonly message: string;
}

/** Raised when a config file cannot be read or is invalid. Code: MESH_CONFIG. */
export class MeshConfigError extends Error {
  public readonly code = 'MESH_CONFIG';
  public readonly issues: readonly ConfigIssue[];

  constructor(issues: readonly ConfigIssue[]) {
    super(`invalid mesh config: ${issues.map((i) => `${i.path} ${i.message}`).join('; ')}`);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'MeshConfigError';
    this.issues = Object.freeze([...issues]);
  }
}

type Report = (path: string, message: string) => void;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isInteger(value: unknown, min: number, max: number): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= min && value <= max;
}

function parseEndpoint(value: unknown, path: string, report: Report): Endpoint {
  if (!isRecord(value)) {
    report(path, 'must be an object with host and port');
    return { host: '127.0.0.1', port: 0 };
  }
  const { host, port } = value;
  if (typeof host !== 'string' || host.length === 0) report(`${path}.host`, 'must be a non-empty string');
  if (!isInteger(port, 0, 65_535)) report(`${path}.port`, 'must be an integer from 0 to 65535');
  return {
    host: typeof host === 'string' ? host : '127.0.0.1',
    port: isInteger(port, 0, 65_535) ? port : 0,
  };
}

function parsePeers(value: unknown, selfId: string, report: Report): PeerConfig[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) {
    report('peers', 'must be an array');
    return [];
  }
  if (value.length > MAX_PEERS) report('peers', `at most ${MAX_PEERS} peers are allowed`);

  const seen = new Set<string>();
  const peers: PeerConfig[] = [];
  for (const [index, item] of value.entries()) {
    const path = `peers[${index}]`;
    if (!isRecord(item)) {
      report(path, 'must be an object');
      continue;
    }
    const { id, host, rpcPort, httpPort } = item;

    if (typeof id !== 'string' || !NODE_ID_PATTERN.test(id)) {
      report(`${path}.id`, 'must be 1-63 lowercase letters, digits or hyphens');
    } else if (id === selfId) {
      report(`${path}.id`, 'must not be this node');
    } else if (seen.has(id)) {
      report(`${path}.id`, 'is listed more than once');
    } else {
      seen.add(id);
    }
    if (typeof host !== 'string' || host.length === 0) report(`${path}.host`, 'must be a non-empty string');
    if (!isInteger(rpcPort, 1, 65_535)) report(`${path}.rpcPort`, 'must be an integer from 1 to 65535');
    if (!isInteger(httpPort, 1, 65_535)) report(`${path}.httpPort`, 'must be an integer from 1 to 65535');

    peers.push({
      id: typeof id === 'string' ? id : '',
      host: typeof host === 'string' ? host : '',
      rpcPort: isInteger(rpcPort, 1, 65_535) ? rpcPort : 0,
      httpPort: isInteger(httpPort, 1, 65_535) ? httpPort : 0,
    });
  }
  return peers;
}

function parseTiming(value: unknown, path: string, fallback: number, report: Report): number {
  if (value === undefined) return fallback;
  if (!isInteger(value, 1, MAX_TIMING_MS)) {
    report(path, `must be an integer from 1 to ${MAX_TIMING_MS}`);
    return fallback;
  }
  return value;
}

/**
 * Validates a parsed config object. Every problem is reported together.
 *
 * @throws {MeshConfigError} If anything is invalid.
 */
export function parseNodeConfig(raw: unknown): MeshNodeConfig {
  const issues: ConfigIssue[] = [];
  const report: Report = (path, message) => issues.push({ path, message });

  if (!isRecord(raw)) throw new MeshConfigError([{ path: '$', message: 'must be an object' }]);

  const nodeId = typeof raw.nodeId === 'string' && NODE_ID_PATTERN.test(raw.nodeId) ? raw.nodeId : '';
  if (nodeId === '') report('nodeId', 'must be 1-63 lowercase letters, digits or hyphens');

  const rpc = parseEndpoint(raw.rpc, 'rpc', report);
  const http = parseEndpoint(raw.http, 'http', report);
  const peers = parsePeers(raw.peers, nodeId, report);

  const electionTimeoutMs = parseTiming(
    raw.electionTimeoutMs,
    'electionTimeoutMs',
    DEFAULT_TIMING.electionTimeoutMs,
    report,
  );
  const heartbeatIntervalMs = parseTiming(
    raw.heartbeatIntervalMs,
    'heartbeatIntervalMs',
    DEFAULT_TIMING.heartbeatIntervalMs,
    report,
  );
  const requestTimeoutMs = parseTiming(raw.requestTimeoutMs, 'requestTimeoutMs', DEFAULT_TIMING.requestTimeoutMs, report);
  if (heartbeatIntervalMs >= electionTimeoutMs) {
    report('heartbeatIntervalMs', 'must be shorter than electionTimeoutMs');
  }

  if (issues.length > 0) throw new MeshConfigError(issues);
  return { nodeId, rpc, http, peers, electionTimeoutMs, heartbeatIntervalMs, requestTimeoutMs };
}

/**
 * Reads and validates a config file.
 *
 * @throws {MeshConfigError} If the file is missing, too large, not JSON, or invalid.
 */
export function loadNodeConfig(file: string): MeshNodeConfig {
  let size: number;
  try {
    const stat = fs.statSync(file);
    if (!stat.isFile()) throw new Error('not a file');
    size = stat.size;
  } catch {
    throw new MeshConfigError([{ path: file, message: 'cannot be read as a file' }]);
  }
  if (size > MAX_CONFIG_BYTES) {
    throw new MeshConfigError([{ path: file, message: `is larger than ${MAX_CONFIG_BYTES} bytes` }]);
  }

  let raw: unknown;
  try {
    raw = JSON.parse(fs.readFileSync(file, 'utf8')) as unknown;
  } catch {
    throw new MeshConfigError([{ path: file, message: 'is not valid JSON' }]);
  }
  return parseNodeConfig(raw);
}
