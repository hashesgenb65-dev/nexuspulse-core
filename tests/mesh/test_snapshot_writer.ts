/**
 * The streamed snapshot writer: its file parses to the same object the durable store reads, it yields
 * to the event loop between batches, it never replaces an existing snapshot, and it refuses bad input.
 */

import { describe, it, expect, afterEach, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import type { LogEntry } from '../../src/mesh/consensus';
import { KeyValueStore } from '../../src/mesh/state_machine';
import { writeSnapshotFileAsync } from '../../src/mesh/snapshot_writer';

const dirs: string[] = [];

afterEach(() => {
  vi.restoreAllMocks();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function freshDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'snapshot-writer-'));
  dirs.push(dir);
  return dir;
}

function storeWith(count: number, valueFor: (i: number) => unknown = (i) => ({ n: i, text: 'x'.repeat(20) })): KeyValueStore {
  const store = new KeyValueStore();
  for (let i = 0; i < count; i++) {
    const entry: LogEntry = { index: i + 1, term: 1, command: 'SET', payload: { key: `k${i}`, value: valueFor(i) } };
    store.apply(entry);
  }
  return store;
}

describe('file content', () => {
  it('parses to the same object as the store image, for several batch sizes', async () => {
    const store = storeWith(2_500);
    for (const batchEntries of [1, 7, 1_000, 5_000]) {
      const dir = freshDir();
      const image = store.referenceImage();
      const file = await writeSnapshotFileAsync(dir, { index: 2_500, term: 3, data: image }, { batchEntries });
      const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
      expect(parsed).toEqual({ index: 2_500, term: 3, data: JSON.parse(JSON.stringify(image)) });
    }
  });

  it('restores to a store with the same state', async () => {
    const store = storeWith(1_200);
    const dir = freshDir();
    const file = await writeSnapshotFileAsync(dir, { index: 1_200, term: 1, data: store.referenceImage() }, { batchEntries: 100 });
    const parsed = JSON.parse(fs.readFileSync(file, 'utf8')) as { data: Parameters<KeyValueStore['restore']>[0] };

    const target = new KeyValueStore();
    target.restore(parsed.data);
    expect(target.snapshot()).toEqual(store.snapshot());
    expect(target.lastApplied).toBe(1_200);
  });

  it('keeps rejected entries and empty stores', async () => {
    const store = new KeyValueStore();
    store.apply({ index: 1, term: 1, command: 'NOPE', payload: {} });
    const dir = freshDir();
    const file = await writeSnapshotFileAsync(dir, { index: 1, term: 1, data: store.referenceImage() });
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).data).toEqual(JSON.parse(JSON.stringify(store.referenceImage())));
  });

  it('leaves out a key whose value is undefined, as JSON does', async () => {
    const dir = freshDir();
    const data = { lastApplied: 1, data: { keep: 1, gone: undefined as unknown }, rejected: [] };
    const file = await writeSnapshotFileAsync(dir, { index: 1, term: 1, data });
    expect(JSON.parse(fs.readFileSync(file, 'utf8')).data.data).toEqual({ keep: 1 });
  });
});

describe('yielding', () => {
  it('gives the event loop a turn after each full batch', async () => {
    const store = storeWith(1_000);
    const dir = freshDir();
    const spy = vi.spyOn(globalThis, 'setImmediate');
    await writeSnapshotFileAsync(dir, { index: 1_000, term: 1, data: store.referenceImage() }, { batchEntries: 100 });
    expect(spy.mock.calls.length).toBeGreaterThanOrEqual(10);
  });
});

describe('the file on disk', () => {
  it('is not replaced when a snapshot for the same index already exists', async () => {
    const dir = freshDir();
    const first = await writeSnapshotFileAsync(dir, { index: 5, term: 1, data: storeWith(3).referenceImage() });
    const before = fs.readFileSync(first, 'utf8');
    await writeSnapshotFileAsync(dir, { index: 5, term: 9, data: storeWith(50).referenceImage() });
    expect(fs.readFileSync(first, 'utf8')).toBe(before);
  });

  it('leaves no temporary file behind after a successful write', async () => {
    const dir = freshDir();
    await writeSnapshotFileAsync(dir, { index: 2, term: 1, data: storeWith(10).referenceImage() });
    expect(fs.readdirSync(dir).filter((name) => name.endsWith('.tmp'))).toEqual([]);
  });

  it('is owner-only', async () => {
    const dir = freshDir();
    const file = await writeSnapshotFileAsync(dir, { index: 2, term: 1, data: storeWith(10).referenceImage() });
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
  });

  it('fails cleanly when the directory does not exist', async () => {
    const dir = path.join(freshDir(), 'missing');
    await expect(writeSnapshotFileAsync(dir, { index: 1, term: 1, data: storeWith(1).referenceImage() })).rejects.toThrow();
  });

  it('refuses a batch size that is not a positive integer', async () => {
    const dir = freshDir();
    const data = storeWith(1).referenceImage();
    for (const batchEntries of [0, -1, 1.5]) {
      await expect(writeSnapshotFileAsync(dir, { index: 1, term: 1, data }, { batchEntries })).rejects.toThrow(RangeError);
    }
  });
});
