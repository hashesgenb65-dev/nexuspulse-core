/**
 * Background compaction: the log is trimmed only after the snapshot file is on disk, a crash at any
 * point before that leaves a state that loads, and a failed or abandoned write changes nothing.
 * A single node drives the consensus, and the real durable store and snapshot writer write to a
 * temporary directory.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { RaftConsensusNode } from '../../src/mesh/consensus';
import { DurableStateFile, NodePersister } from '../../src/mesh/durable_store';
import { KeyValueStore } from '../../src/mesh/state_machine';
import { StateHost } from '../../src/mesh/snapshot';

const THRESHOLD = 5;
const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'pending-compaction-'));
  dirs.push(dir);
  return dir;
}

function soloNode(): RaftConsensusNode {
  const node = new RaftConsensusNode({
    nodeId: 'solo',
    clusterPeers: [],
    electionTimeoutMs: 150,
    heartbeatIntervalMs: 50,
  });
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

interface Rig {
  node: RaftConsensusNode;
  store: KeyValueStore;
  host: StateHost;
  persister: NodePersister;
  dir: string;
  errors: Array<{ where: string; message: string }>;
}

function rig(dir: string = tempDir()): Rig {
  const node = soloNode();
  const store = new KeyValueStore();
  const errors: Array<{ where: string; message: string }> = [];
  const persistHolder: { persister?: NodePersister } = {};
  const host = new StateHost({
    node,
    store,
    compactThreshold: THRESHOLD,
    snapshotDirectory: dir,
    onSnapshotCommitted: () => {
      persistHolder.persister?.save();
    },
    onError: (error, where) => errors.push({ where, message: (error as Error).message }),
  });
  const persister = new NodePersister({ file: new DurableStateFile(dir), node, host });
  persistHolder.persister = persister;
  return { node, store, host, persister, dir, errors };
}

/** Proposes and applies `count` SET commands, applying after each so compaction is checked each time. */
function write(r: Rig, from: number, count: number): void {
  for (let i = from; i < from + count; i++) {
    r.node.proposeCommand('SET', { key: `k${i}`, value: i });
    r.host.applyCommitted();
  }
}

describe('while a snapshot is being written', () => {
  it('keeps the full log and the old snapshot index on disk', async () => {
    const r = rig();
    write(r, 1, THRESHOLD);
    expect(r.host.compactionPending).toBe(true);
    expect(r.node.snapshotIndex).toBe(0);

    r.persister.save();
    const onDisk = new DurableStateFile(r.dir).load();
    expect(onDisk?.snapshotIndex).toBe(0);
    expect(onDisk?.log.length).toBe(THRESHOLD);

    await waitFor(() => !r.host.compactionPending);
  });

  it('does not start a second write while one is pending', async () => {
    const r = rig();
    // All of these run in one synchronous loop, so the first write cannot finish during them.
    write(r, 1, THRESHOLD * 3);
    expect(r.node.snapshotIndex).toBe(0);
    await waitFor(() => !r.host.compactionPending);
    expect(r.node.snapshotIndex).toBe(THRESHOLD);
  });
});

describe('after the snapshot is on disk', () => {
  it('trims the log and refers to the snapshot, and the snapshot file exists', async () => {
    const r = rig();
    write(r, 1, THRESHOLD);
    await waitFor(() => !r.host.compactionPending);

    expect(r.node.snapshotIndex).toBe(THRESHOLD);
    expect(r.node.snapshotTerm).toBe(r.node.termAt(THRESHOLD));
    expect(fs.existsSync(path.join(r.dir, `snapshot-${THRESHOLD}.json`))).toBe(true);
    const loaded = new DurableStateFile(r.dir).load();
    expect(loaded?.snapshotIndex).toBe(THRESHOLD);
    expect(loaded?.log.length).toBe(0);
  });

  it('holds the same state as the store when the snapshot was taken', async () => {
    const r = rig();
    write(r, 1, THRESHOLD);
    await waitFor(() => !r.host.compactionPending);

    const file = JSON.parse(fs.readFileSync(path.join(r.dir, `snapshot-${THRESHOLD}.json`), 'utf8')) as {
      data: { lastApplied: number; data: Record<string, unknown> };
    };
    expect(file.data.lastApplied).toBe(THRESHOLD);
    expect(Object.keys(file.data.data)).toHaveLength(THRESHOLD);
  });
});

describe('a crash before the trim', () => {
  it('loads from the old state, with the full log, while the snapshot is still being written', async () => {
    const r = rig();
    write(r, 1, THRESHOLD);
    r.persister.save();
    // Crash here: the write is in flight and state.json still has the full log and the old index.
    const restarted = new DurableStateFile(r.dir).load();
    expect(restarted?.snapshotIndex).toBe(0);
    expect(restarted?.log.length).toBe(THRESHOLD);
    expect(restarted?.snapshot).toBeNull();

    await waitFor(() => !r.host.compactionPending);
  });

  it('loads when the snapshot file exists but state.json still has the pre-trim version', async () => {
    const r = rig();
    write(r, 1, THRESHOLD);
    r.persister.save();
    const preTrimState = fs.readFileSync(path.join(r.dir, 'state.json'));
    await waitFor(() => !r.host.compactionPending);
    expect(fs.existsSync(path.join(r.dir, `snapshot-${THRESHOLD}.json`))).toBe(true);

    // Crash after the snapshot file was renamed into place but before state.json was rewritten.
    fs.writeFileSync(path.join(r.dir, 'state.json'), preTrimState, { mode: 0o600 });

    const loaded = new DurableStateFile(r.dir).load();
    expect(loaded?.snapshotIndex).toBe(0);
    expect(loaded?.log.length).toBe(THRESHOLD);
  });
});

describe('a failed or abandoned write', () => {
  it('leaves the log whole, reports the failure, and lets a later compaction try again', async () => {
    const missing = path.join(tempDir(), 'missing');
    const r = rig(missing);
    write(r, 1, THRESHOLD);
    await waitFor(() => r.errors.length > 0);

    expect(r.errors[0]?.where).toBe('snapshot');
    expect(r.host.compactionPending).toBe(false);
    expect(r.node.snapshotIndex).toBe(0);
    expect(r.node.log.length).toBe(THRESHOLD);
  });

  it('is not trimmed after the node is disposed', async () => {
    const r = rig();
    write(r, 1, THRESHOLD);
    r.host.dispose();
    await waitFor(() => !r.host.compactionPending);
    expect(r.node.snapshotIndex).toBe(0);
  });

  it('is not trimmed when a newer snapshot was installed while it was pending', async () => {
    const r = rig();
    write(r, 1, THRESHOLD);
    // An installed snapshot at a higher index moves the node past the pending one.
    const leader = new KeyValueStore();
    for (let i = 1; i <= THRESHOLD + 2; i++) {
      leader.apply({ index: i, term: 1, command: 'SET', payload: { key: `x${i}`, value: i } });
    }
    expect(r.host.install({ index: THRESHOLD + 2, term: 1, data: leader.image() })).toBe(true);
    await waitFor(() => !r.host.compactionPending);

    expect(r.node.snapshotIndex).toBe(THRESHOLD + 2);
    expect(r.errors).toEqual([]);
  });
});
