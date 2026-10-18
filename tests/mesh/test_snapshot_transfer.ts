/**
 * Snapshot transfer over real sockets: a snapshot larger than one chunk arrives intact, and the
 * follower's answer, or its failure, reaches the leader unchanged.
 */

import { describe, it, expect, afterEach } from 'vitest';
import type { InstallSnapshotReply, InstallSnapshotRequest, SnapshotOffer } from '../../src/mesh/consensus';
import { PeerServer, TcpPeerTransport, type RpcHandler } from '../../src/mesh/peer_transport';
import { MAX_CHUNK_BYTES, encodeSnapshot } from '../../src/mesh/snapshot_chunks';

const HOST = '127.0.0.1';
const cleanups: Array<() => void | Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

/** Starts a server whose snapshot handler answers with `reply`, and returns a client for it. */
async function connect(reply: (request: InstallSnapshotRequest) => InstallSnapshotReply) {
  const calls: InstallSnapshotRequest[] = [];
  const handler: RpcHandler = {
    handleVoteRequest: () => ({ term: 1, voteGranted: true }),
    handleAppendRequest: () => ({ term: 1, success: true }),
    handleSnapshotRequest: (request) => {
      calls.push(request);
      return reply(request);
    },
  };
  const server = new PeerServer({ handler });
  const port = await server.listen(0, HOST);
  cleanups.push(() => server.close());

  const transport = new TcpPeerTransport({
    peers: { server: { host: HOST, port } },
    requestTimeoutMs: 5_000,
    connectTimeoutMs: 1_000,
  });
  cleanups.push(() => transport.close());
  return { calls, transport };
}

/** Text whose UTF-8 encoding is several chunks long and has multi-byte characters across chunk edges. */
function bigOffer(index: number, term: number): SnapshotOffer {
  return { index, term, data: { text: 'é✓漢'.repeat(200_000) } };
}

describe('snapshot transfer over sockets', () => {
  it('delivers a multi-chunk snapshot to the handler exactly once and returns its answer', async () => {
    const offer = bigOffer(42, 3);
    expect(encodeSnapshot(offer).length).toBeGreaterThan(2 * MAX_CHUNK_BYTES);

    const { calls, transport } = await connect(() => ({ term: 5, installed: true }));
    const request: InstallSnapshotRequest = { term: 5, leaderId: 'leader', snapshot: offer };

    await expect(transport.installSnapshot('server', request)).resolves.toEqual({ term: 5, installed: true });
    expect(calls).toHaveLength(1);
    expect(calls[0]).toEqual({ term: 5, leaderId: 'leader', snapshot: offer });
  });

  it('passes a small snapshot through as a single chunk', async () => {
    const offer: SnapshotOffer = { index: 7, term: 2, data: { n: 1 } };
    const { calls, transport } = await connect(() => ({ term: 2, installed: true }));

    await expect(
      transport.installSnapshot('server', { term: 2, leaderId: 'leader', snapshot: offer }),
    ).resolves.toEqual({ term: 2, installed: true });
    expect(calls[0]?.snapshot).toEqual(offer);
  });

  it('reports a follower that refused the snapshot, and its newer term', async () => {
    const { transport } = await connect(() => ({ term: 9, installed: false }));

    await expect(
      transport.installSnapshot('server', { term: 5, leaderId: 'leader', snapshot: bigOffer(42, 3) }),
    ).resolves.toEqual({ term: 9, installed: false });
  });

  it('fails the call when the handler throws', async () => {
    const { transport } = await connect(() => {
      throw new Error('store is read-only');
    });

    await expect(
      transport.installSnapshot('server', { term: 5, leaderId: 'leader', snapshot: bigOffer(42, 3) }),
    ).rejects.toThrow();
  });
});
