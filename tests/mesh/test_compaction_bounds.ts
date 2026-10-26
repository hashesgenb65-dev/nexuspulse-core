/**
 * Pending snapshot bounds: the still-pending warning fires once while a write is pending, it does not fire
 * after the write has finished, dispose cancels it, and pendingWarnMs is validated.
 * Only the timer is faked, so the background write still runs for real.
 */

import { describe, it, expect, afterEach, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { KeyValueStore } from '../../src/mesh/state_machine';
import { MAX_PENDING_WARN_MS, StateHost } from '../../src/mesh/snapshot';

const THRESHOLD = 5;
const dirs: string[] = [];

afterEach(() => {
  vi.useRealTimers();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'compaction-bounds-'));
  dirs.push(dir);
  return dir;
}

function soloNode(): RaftConsensusNode {
  const node = new RaftConsensusNode({ nodeId: 'solo', clusterPeers: [], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
  node.start();
  node.startElection();
  return node;
}

async function waitFor(condition: () => boolean, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met before timeout');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function rig(pendingWarnMs: number) {
  const node = soloNode();
  const store = new KeyValueStore();
  const errors: string[] = [];
  const host = new StateHost({
    node,
    store,
    compactThreshold: THRESHOLD,
    snapshotDirectory: tempDir(),
    pendingWarnMs,
    onError: (error) => errors.push((error as Error).message),
  });
  return { node, store, host, errors };
}

function write(r: ReturnType<typeof rig>, count: number): void {
  for (let i = 1; i <= count; i++) {
    r.node.proposeCommand('SET', { key: `k${i}`, value: i });
    r.host.applyCommitted();
  }
}

describe('still-pending warning', () => {
  it('reports once while the write is pending, then the write still completes', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const r = rig(500);
    write(r, THRESHOLD);
    expect(r.host.compactionPending).toBe(true);
    expect(r.host.pendingAgeMs).toBeGreaterThanOrEqual(0);

    vi.advanceTimersByTime(500);
    vi.advanceTimersByTime(500);
    expect(r.errors.filter((m) => m.includes('still pending'))).toHaveLength(1);
    expect(r.errors[0]).toContain('the log is kept whole');

    vi.useRealTimers();
    await waitFor(() => !r.host.compactionPending);
    expect(r.node.snapshotIndex).toBe(THRESHOLD);
    expect(r.host.pendingAgeMs).toBeUndefined();
    expect(r.errors.filter((m) => m.includes('still pending'))).toHaveLength(1);
  });

  it('does not report when the write finishes before the time runs out', async () => {
    const r = rig(60_000);
    write(r, THRESHOLD);
    await waitFor(() => !r.host.compactionPending);
    expect(r.errors).toEqual([]);
  });

  it('is cancelled by dispose', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const r = rig(500);
    write(r, THRESHOLD);
    r.host.dispose();
    vi.advanceTimersByTime(5_000);
    expect(r.errors).toEqual([]);
    vi.useRealTimers();
  });
});

describe('pendingWarnMs validation', () => {
  it('accepts integers from 1 to the maximum, and rejects anything else', () => {
    const node = soloNode();
    const store = new KeyValueStore();
    const make = (pendingWarnMs: number) => () => new StateHost({ node, store, pendingWarnMs });
    expect(make(1)).not.toThrow();
    expect(make(MAX_PENDING_WARN_MS)).not.toThrow();
    expect(make(0)).toThrow(RangeError);
    expect(make(MAX_PENDING_WARN_MS + 1)).toThrow(RangeError);
    expect(make(1.5)).toThrow(RangeError);
  });
});
