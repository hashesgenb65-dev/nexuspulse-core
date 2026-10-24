/**
 * Starts one mesh member from its config: the peer RPC listener, the peer client, the consensus
 * driver, the replicated key-value store, and the key-value HTTP API.
 *
 * Call {@link MeshNodeHandle.stop} to shut down. It stops timers, closes both listeners and the
 * peer connections, and drops in-flight HTTP connections. Calling it twice is safe.
 */

import * as http from 'node:http';
import { RaftConsensusNode } from './consensus.js';
import { ConsensusDriver } from './driver.js';
import { DEFAULT_COMMIT_TIMEOUT_MS, KvApi, createKvServer, type KvPeerHttp } from './kv_api.js';
import type { MeshNodeConfig } from './node_config.js';
import { PeerServer, TcpPeerTransport, systemScheduler } from './peer_transport.js';
import { KeyValueStore } from './state_machine.js';
import { DEFAULT_COMPACT_THRESHOLD, StateHost } from './snapshot.js';
import { DurableStateFile, NodePersister, restoreNode } from './durable_store.js';
import { loadPeerSecret, PeerAuthError } from './peer_auth.js';
import { loadRotatingTokenFile, reloadTokenFile } from '../app/token_file.js';
import { openDurableAudit } from '../audit/durable_audit.js';

export interface MeshNodeHandle {
  readonly nodeId: string;
  /** Port the peer RPC listener is bound to. */
  readonly rpcPort: number;
  /** Port the key-value HTTP API is bound to. */
  readonly httpPort: number;
  readonly node: RaftConsensusNode;
  readonly store: KeyValueStore;
  stop(): Promise<void>;
}

export interface StartMeshNodeOptions {
  /** Receives transport and consensus failures. They never stop the node. */
  onError?: (error: unknown, where: string) => void;
  /** Returns a number in [0, 1). Defaults to Math.random. */
  random?: () => number;
  /** How long a write waits to commit before it answers 504. */
  commitTimeoutMs?: number;
  /** Applied entries kept in the log before they are folded into a snapshot. */
  compactThreshold?: number;
  /** How long a replaced API token stays valid after a SIGHUP reload. Defaults to 5 minutes. */
  tokenGraceMs?: number;
}

function listenHttp(server: http.Server, port: number, host: string): Promise<number> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => {
      server.off('error', reject);
      const address = server.address();
      if (address === null || typeof address === 'string') {
        reject(new Error('HTTP server has no port'));
        return;
      }
      resolve(address.port);
    });
  });
}

function closeHttp(server: http.Server): Promise<void> {
  return new Promise((resolve) => {
    server.close(() => resolve());
    server.closeAllConnections();
  });
}

/**
 * Starts the member described by `config`. Resolves once both listeners are bound and the
 * consensus driver is running.
 *
 * @throws {Error} If either listener cannot bind, for example because the port is in use.
 */
export async function startMeshNode(config: MeshNodeConfig, options: StartMeshNodeOptions = {}): Promise<MeshNodeHandle> {
  // A cluster member must authenticate its peers. Check this before anything is bound or written.
  if (config.peers.length > 0 && config.peerSecretFile === undefined) {
    throw new PeerAuthError('a node with peers needs peerSecretFile, so that peer traffic is authenticated');
  }
  const secret = config.peerSecretFile === undefined ? undefined : loadPeerSecret(config.peerSecretFile);
  // Loaded before anything is bound, so a bad token file stops start-up with no port open.
  const tokenFile = config.tokensFile;
  const tokens =
    tokenFile === undefined
      ? undefined
      : loadRotatingTokenFile(tokenFile, options.tokenGraceMs === undefined ? {} : { graceMs: options.tokenGraceMs });
  // Opened and verified before anything is bound, so a broken audit file stops start-up with no port open.
  const audit = config.auditFile === undefined ? undefined : openDurableAudit(config.auditFile);

  const peerAddresses: Record<string, { host: string; port: number }> = {};
  const peerHttp = new Map<string, KvPeerHttp>();
  for (const peer of config.peers) {
    peerAddresses[peer.id] = { host: peer.host, port: peer.rpcPort };
    peerHttp.set(peer.id, { host: peer.host, httpPort: peer.httpPort });
  }

  const transport = new TcpPeerTransport({
    peers: peerAddresses,
    requestTimeoutMs: config.requestTimeoutMs,
    connectTimeoutMs: config.requestTimeoutMs,
    secret,
  });
  const node = new RaftConsensusNode({
    nodeId: config.nodeId,
    clusterPeers: config.peers.map((peer) => peer.id),
    electionTimeoutMs: config.electionTimeoutMs,
    heartbeatIntervalMs: config.heartbeatIntervalMs,
  });
  const store = new KeyValueStore();
  const host = new StateHost({
    node,
    store,
    compactThreshold: options.compactThreshold ?? DEFAULT_COMPACT_THRESHOLD,
  });
  // Restore what was saved before the driver starts. A corrupt file stops start-up here, before any port is bound.
  const durableFile = config.dataDir === undefined ? undefined : new DurableStateFile(config.dataDir);
  const saved = durableFile?.load();
  if (saved !== undefined) restoreNode(saved, node, host);
  const persister =
    durableFile === undefined ? undefined : new NodePersister({ file: durableFile, node, host });
  if (persister !== undefined && saved !== undefined) persister.markSaved();

  const driver = new ConsensusDriver({
    node,
    transport,
    scheduler: systemScheduler(),
    electionTimeoutMs: config.electionTimeoutMs,
    heartbeatIntervalMs: config.heartbeatIntervalMs,
    random: options.random,
    onError: options.onError,
    onCommitAdvance: () => {
      host.applyCommitted();
      // Compaction may have changed the log, so save again.
      driver.persistState();
    },
    snapshotSource: () => host.latestSnapshot,
    installSnapshot: (snapshot) => host.install(snapshot),
    persist: persister === undefined ? undefined : () => persister.save(),
  });

  const rpcServer = new PeerServer({
    secret,
    handler: {
      handleVoteRequest: (request) => driver.handleVoteRequest(request),
      handleAppendRequest: (request) => driver.handleAppendRequest(request),
      handleSnapshotRequest: (request) => driver.handleSnapshotRequest(request),
    },
  });
  const api = new KvApi({
    node,
    store,
    applier: host,
    peerHttp,
    commitTimeoutMs: options.commitTimeoutMs ?? DEFAULT_COMMIT_TIMEOUT_MS,
    persist: () => driver.persistState(),
    durable: persister !== undefined,
    ...(tokens === undefined ? {} : { auth: tokens }),
    ...(audit === undefined ? {} : { audit: audit.ledger }),
  });
  const httpServer = createKvServer(api);

  // If a later listener fails, close what is already open before reporting the error.
  const rpcPort = await rpcServer.listen(config.rpc.port, config.rpc.host).catch((error: unknown) => {
    transport.close();
    audit?.close();
    throw error;
  });
  const httpPort = await listenHttp(httpServer, config.http.port, config.http.host).catch(async (error: unknown) => {
    await rpcServer.close();
    transport.close();
    audit?.close();
    throw error;
  });

  driver.start();

  // SIGHUP re-reads the token file. A file that fails a check is reported, and the current tokens stay in place.
  const onReload = (): void => {
    if (tokens === undefined || tokenFile === undefined) return;
    try {
      reloadTokenFile(tokens, tokenFile);
    } catch (error) {
      options.onError?.(error, 'token reload');
    }
  };
  if (tokens !== undefined) process.on('SIGHUP', onReload);

  let stopping: Promise<void> | undefined;
  const stop = (): Promise<void> => {
    stopping ??= (async () => {
      if (tokens !== undefined) process.off('SIGHUP', onReload);
      driver.stop();
      transport.close();
      await rpcServer.close();
      await closeHttp(httpServer);
      audit?.close();
    })();
    return stopping;
  };

  return { nodeId: config.nodeId, rpcPort, httpPort, node, store, stop };
}
