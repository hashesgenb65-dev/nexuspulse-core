/**
 * The snapshot lives in its own file: written once per index, kept out of state.json, and old ones
 * removed. Version 1 files, which kept it inline, still load.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  DurableStateError,
  DurableStateFile,
  STATE_FILE_NAME,
  type DurableState,
} from '../../src/mesh/durable_store';

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function freshDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-snap-'));
  dirs.push(dir);
  return dir;
}

function snapshotAt(index: number, term: number): NonNullable<DurableState['snapshot']> {
  return { index, term, data: { data: { k: `v${index}` }, lastApplied: index, rejected: [] } } as unknown as NonNullable<
    DurableState['snapshot']
  >;
}

function stateWith(snapshot: DurableState['snapshot'], overrides: Partial<DurableState> = {}): DurableState {
  return {
    currentTerm: 2,
    votedFor: null,
    snapshotIndex: snapshot?.index ?? 0,
    snapshotTerm: snapshot?.term ?? 0,
    snapshot,
    log: [],
    ...overrides,
  };
}

describe('snapshot file', () => {
  it('keeps the snapshot out of state.json and writes it to its own file', () => {
    const dir = freshDir();
    const file = new DurableStateFile(dir);
    file.save(stateWith(snapshotAt(3, 1)));

    const body = JSON.parse(fs.readFileSync(path.join(dir, STATE_FILE_NAME), 'utf8')) as Record<string, unknown>;
    expect(body).not.toHaveProperty('snapshot');
    expect(body['snapshotIndex']).toBe(3);
    expect(fs.readdirSync(dir).sort()).toEqual(['snapshot-3.json', STATE_FILE_NAME]);

    expect(file.load()?.snapshot).toEqual(snapshotAt(3, 1));
  });

  it('does not rewrite the snapshot file when only the term or the log changes', () => {
    const dir = freshDir();
    const file = new DurableStateFile(dir);
    const state = stateWith(snapshotAt(3, 1));
    file.save(state);

    const snapshotFile = path.join(dir, 'snapshot-3.json');
    fs.writeFileSync(snapshotFile, 'marker');
    file.save({ ...state, currentTerm: 3 });

    expect(fs.readFileSync(snapshotFile, 'utf8')).toBe('marker');
    expect((JSON.parse(fs.readFileSync(path.join(dir, STATE_FILE_NAME), 'utf8')) as { currentTerm: number }).currentTerm).toBe(3);
  });

  it('removes the snapshot files of older indexes once a newer snapshot is saved', () => {
    const dir = freshDir();
    const file = new DurableStateFile(dir);
    file.save(stateWith(snapshotAt(3, 1)));
    file.save(stateWith(snapshotAt(9, 2)));

    expect(fs.readdirSync(dir).sort()).toEqual(['snapshot-9.json', STATE_FILE_NAME]);
    expect(file.load()?.snapshot).toEqual(snapshotAt(9, 2));
  });

  it('loads a version 1 file that kept the snapshot inline, and moves the snapshot on the next save', () => {
    const dir = freshDir();
    const legacy = {
      version: 1,
      currentTerm: 2,
      votedFor: 'node-1',
      snapshotIndex: 3,
      snapshotTerm: 1,
      snapshot: snapshotAt(3, 1),
      log: [],
    };
    fs.writeFileSync(path.join(dir, STATE_FILE_NAME), JSON.stringify(legacy));

    const file = new DurableStateFile(dir);
    const loaded = file.load();
    expect(loaded?.snapshot).toEqual(snapshotAt(3, 1));
    expect(loaded?.votedFor).toBe('node-1');

    file.save(loaded as DurableState);
    const body = JSON.parse(fs.readFileSync(path.join(dir, STATE_FILE_NAME), 'utf8')) as Record<string, unknown>;
    expect(body['version']).toBe(2);
    expect(body).not.toHaveProperty('snapshot');
    expect(fs.existsSync(path.join(dir, 'snapshot-3.json'))).toBe(true);
  });

  it('reports a missing snapshot file rather than starting with an empty store', () => {
    const dir = freshDir();
    const file = new DurableStateFile(dir);
    file.save(stateWith(snapshotAt(3, 1)));
    fs.rmSync(path.join(dir, 'snapshot-3.json'));

    expect(() => file.load()).toThrowError(DurableStateError);
    expect(() => file.load()).toThrowError(/snapshot file for index 3 is missing/);
  });

  it('refuses a snapshot file that belongs to another index', () => {
    const dir = freshDir();
    const file = new DurableStateFile(dir);
    file.save(stateWith(snapshotAt(3, 1)));
    fs.writeFileSync(path.join(dir, 'snapshot-3.json'), JSON.stringify(snapshotAt(4, 1)));

    expect(() => file.load()).toThrowError(/snapshot must match snapshotIndex/);
  });
});
