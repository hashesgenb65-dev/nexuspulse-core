/**
 * Tests for durable state: validation of the state file, atomic save and load, change-only
 * persistence, restoring a node, the barrier entry, and the driver's fail-stop on a failed write.
 */

import { describe, it, expect, afterEach, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { RaftConsensusNode, type LogEntry } from '../../src/mesh/consensus';
import { ConsensusDriver, type PeerTransport, type Scheduler } from '../../src/mesh/driver';
import {
  DurableStateError,
  DurableStateFile,
  NodePersister,
  STATE_FILE_NAME,
  STATE_FILE_VERSION,
  parseDurableState,
  restoreNode,
  type DurableState,
} from '../../src/mesh/durable_store';
import { StateHost } from '../../src/mesh/snapshot';
import { BARRIER_COMMAND, KeyValueStore, parseCommand } from '../../src/mesh/state_machine';

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'durable-'));
  dirs.push(dir);
  return dir;
}

function set(index: number, term: number, key: string, value: unknown): LogEntry {
  return { index, term, command: 'SET', payload: { key, value } };
}

function validFileBody(): Record<string, unknown> {
  return {
    version: STATE_FILE_VERSION,
    currentTerm: 2,
    votedFor: 'node-1',
    snapshotIndex: 0,
    snapshotTerm: 0,
    snapshot: null,
    log: [set(1, 1, 'a', 1), { index: 2, term: 2, command: BARRIER_COMMAND, payload: {} }],
  };
}

function soloNode(): RaftConsensusNode {
  const node = new RaftConsensusNode({ nodeId: 'solo', clusterPeers: [], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
  node.start();
  return node;
}

function snapshotOf(entries: LogEntry[]): { index: number; term: number; data: unknown } {
  const store = new KeyValueStore();
  for (const entry of entries) store.apply(entry);
  return { index: entries.length, term: entries[entries.length - 1]?.term ?? 0, data: store.image() };
}

describe('parseDurableState', () => {
  it('accepts a consistent file', () => {
    const state = parseDurableState(validFileBody());
    expect(state.currentTerm).toBe(2);
    expect(state.votedFor).toBe('node-1');
    expect(state.log.map((e) => e.index)).toEqual([1, 2]);
    expect(state.snapshot).toBeNull();
  });

  it('rejects a file with the wrong version or no version', () => {
    expect(() => parseDurableState({ ...validFileBody(), version: 2 })).toThrowError(/unsupported state file version/);
    const { version: _ignored, ...withoutVersion } = validFileBody();
    expect(() => parseDurableState(withoutVersion)).toThrowError(DurableStateError);
  });

  it('rejects bad scalar fields', () => {
    expect(() => parseDurableState({ ...validFileBody(), currentTerm: -1 })).toThrowError(/currentTerm/);
    expect(() => parseDurableState({ ...validFileBody(), votedFor: 7 })).toThrowError(/votedFor/);
    expect(() => parseDurableState({ ...validFileBody(), snapshotIndex: 1.5 })).toThrowError(/snapshotIndex/);
  });

  it('rejects a snapshot index with no snapshot, and a snapshot that does not match its index', () => {
    expect(() => parseDurableState({ ...validFileBody(), snapshotIndex: 1 })).toThrowError(/no snapshot/);
    const snapshot = snapshotOf([set(1, 1, 'a', 1)]);
    expect(() =>
      parseDurableState({ ...validFileBody(), snapshotIndex: 3, snapshot: { ...snapshot, index: 1 } }),
    ).toThrowError(/must match snapshotIndex/);
  });

  it('rejects a log that is not contiguous from the snapshot', () => {
    const body = validFileBody();
    body.log = [set(1, 1, 'a', 1), set(3, 2, 'b', 2)];
    expect(() => parseDurableState(body)).toThrowError(/must have index 2/);
  });

  it('rejects an entry from a term after the current one, or a term that goes backwards', () => {
    const future = validFileBody();
    future.log = [set(1, 1, 'a', 1), set(2, 3, 'b', 2)];
    expect(() => parseDurableState(future)).toThrowError(/impossible term/);

    const backwards = validFileBody();
    backwards.log = [set(1, 2, 'a', 1), set(2, 1, 'b', 2)];
    expect(() => parseDurableState(backwards)).toThrowError(/impossible term/);
  });

  it('rejects an entry with no payload', () => {
    const body = validFileBody();
    body.log = [{ index: 1, term: 1, command: 'SET' }];
    expect(() => parseDurableState(body)).toThrowError(/malformed/);
  });
});

describe('DurableStateFile', () => {
  it('reports no state for a fresh directory', () => {
    expect(new DurableStateFile(tempDir()).load()).toBeUndefined();
  });

  it('round-trips a state, including a snapshot', () => {
    const dir = tempDir();
    const file = new DurableStateFile(dir);
    const snapshot = snapshotOf([set(1, 1, 'a', 1), set(2, 1, 'b', { n: [1] })]);
    const state: DurableState = {
      currentTerm: 3,
      votedFor: 'node-2',
      snapshotIndex: 2,
      snapshotTerm: 1,
      snapshot: snapshot as DurableState['snapshot'],
      log: [{ index: 3, term: 3, command: BARRIER_COMMAND, payload: {} }],
    };

    file.save(state);
    expect(file.load()).toEqual(state);
    expect(file.location).toBe(path.join(dir, STATE_FILE_NAME));
  });

  it('leaves no temporary file behind, and replaces the previous state', () => {
    const dir = tempDir();
    const file = new DurableStateFile(dir);
    const base = { currentTerm: 1, votedFor: null, snapshotIndex: 0, snapshotTerm: 0, snapshot: null, log: [] };
    file.save(base);
    file.save({ ...base, currentTerm: 4 });

    expect(file.load()?.currentTerm).toBe(4);
    expect(fs.readdirSync(dir)).toEqual([STATE_FILE_NAME]);
  });

  it('refuses a file that is not JSON', () => {
    const dir = tempDir();
    fs.writeFileSync(path.join(dir, STATE_FILE_NAME), '{"version":1,');
    expect(() => new DurableStateFile(dir).load()).toThrowError(/is not valid JSON/);
  });

  it('refuses a file that parses but is inconsistent', () => {
    const dir = tempDir();
    fs.writeFileSync(path.join(dir, STATE_FILE_NAME), JSON.stringify({ ...validFileBody(), currentTerm: -5 }));
    expect(() => new DurableStateFile(dir).load()).toThrowError(DurableStateError);
  });
});

describe('NodePersister', () => {
  it('writes only when the durable state has changed', () => {
    const dir = tempDir();
    const node = soloNode();
    const host = new StateHost({ node, store: new KeyValueStore() });
    const persister = new NodePersister({ file: new DurableStateFile(dir), node, host });

    expect(persister.save()).toBe(true);
    expect(persister.save()).toBe(false);

    node.startElection();
    expect(persister.save()).toBe(true);
    expect(persister.save()).toBe(false);
  });

  it('does not write back a state it has just restored', () => {
    const dir = tempDir();
    const file = new DurableStateFile(dir);
    file.save(parseDurableState(validFileBody()));
    const node = soloNode();
    const host = new StateHost({ node, store: new KeyValueStore() });
    restoreNode(file.load() as DurableState, node, host);

    const persister = new NodePersister({ file, node, host });
    persister.markSaved();
    expect(persister.save()).toBe(false);
  });

  it('saves the log entries it has been given', () => {
    const dir = tempDir();
    const node = soloNode();
    const host = new StateHost({ node, store: new KeyValueStore() });
    const file = new DurableStateFile(dir);
    const persister = new NodePersister({ file, node, host });

    node.startElection();
    node.proposeCommand('SET', { key: 'a', value: 1 });
    persister.save();

    // startElection alone appends no barrier; the driver does that. This checks only that entries are saved.
    expect(file.load()?.log.map((e) => e.command)).toEqual(['SET']);
  });
});

describe('restoreNode', () => {
  it('sets the term, vote, log and snapshot, and starts the commit index at the snapshot', () => {
    const snapshot = snapshotOf([set(1, 1, 'a', 1), set(2, 1, 'b', 2)]);
    const state: DurableState = {
      currentTerm: 4,
      votedFor: 'node-3',
      snapshotIndex: 2,
      snapshotTerm: 1,
      snapshot: snapshot as DurableState['snapshot'],
      log: [{ index: 3, term: 4, command: BARRIER_COMMAND, payload: {} }],
    };
    const node = soloNode();
    const store = new KeyValueStore();
    const host = new StateHost({ node, store });

    restoreNode(state, node, host);

    expect(node.currentTerm).toBe(4);
    expect(node.votedFor).toBe('node-3');
    expect(node.snapshotIndex).toBe(2);
    expect(node.lastIndex).toBe(3);
    expect(node.commitIndex).toBe(2);
    expect(store.get('b')).toBe(2);
    expect(store.lastApplied).toBe(2);
  });
});

describe('barrier command', () => {
  it('is accepted without a key and changes nothing in the store', () => {
    expect(parseCommand({ index: 1, term: 1, command: BARRIER_COMMAND, payload: undefined })).toEqual({
      ok: true,
      command: { op: 'BARRIER' },
    });

    const store = new KeyValueStore();
    expect(store.apply({ index: 1, term: 1, command: BARRIER_COMMAND, payload: {} })).toBe('applied');
    expect(store.lastApplied).toBe(1);
    expect(store.size).toBe(0);
    expect(store.rejected).toEqual([]);
  });

  it('is appended by a driver when it wins an election, and the host is told to apply it', () => {
    const tasks: Array<() => void> = [];
    const scheduler: Scheduler = {
      schedule: (_ms, task) => {
        tasks.push(task);
        return tasks.length;
      },
      cancel: () => undefined,
    };
    const node = new RaftConsensusNode({ nodeId: 'solo', clusterPeers: [], electionTimeoutMs: 150, heartbeatIntervalMs: 50 });
    const onCommitAdvance = vi.fn();
    const driver = new ConsensusDriver({
      node,
      transport: {} as PeerTransport,
      scheduler,
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
      random: () => 0,
      onCommitAdvance,
    });

    driver.start();
    tasks[0]?.();

    expect(node.state).toBe('LEADER');
    expect(node.log.map((e) => e.command)).toEqual([BARRIER_COMMAND]);
    expect(node.commitIndex).toBe(1);
    expect(onCommitAdvance).toHaveBeenCalled();
    driver.stop();
  });
});

describe('driver persistence', () => {
  it('saves before it answers a vote request', () => {
    const node = soloNode();
    const persist = vi.fn();
    const driver = new ConsensusDriver({
      node,
      transport: {} as PeerTransport,
      scheduler: { schedule: () => 0, cancel: () => undefined },
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
      persist,
    });

    const reply = driver.handleVoteRequest({ term: 1, candidateId: 'other', lastLogIndex: 0, lastLogTerm: 0 });
    expect(reply.voteGranted).toBe(true);
    expect(persist).toHaveBeenCalledOnce();
  });

  it('stops for good when a write fails, and answers no later request', () => {
    const node = soloNode();
    const onError = vi.fn();
    const driver = new ConsensusDriver({
      node,
      transport: {} as PeerTransport,
      scheduler: { schedule: () => 0, cancel: () => undefined },
      electionTimeoutMs: 150,
      heartbeatIntervalMs: 50,
      onError,
      persist: () => {
        throw new Error('disk full');
      },
    });

    expect(() => driver.handleVoteRequest({ term: 1, candidateId: 'other', lastLogIndex: 0, lastLogTerm: 0 })).toThrowError(
      /disk full/,
    );
    expect(onError).toHaveBeenCalledWith(expect.any(Error), 'persist');
    expect(() => driver.handleVoteRequest({ term: 2, candidateId: 'other', lastLogIndex: 0, lastLogTerm: 0 })).toThrowError(
      /stopped after a failed write/,
    );
  });
});
