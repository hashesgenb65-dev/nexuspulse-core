/**
 * The reference image used for compaction: it has the same content as a full image, later writes
 * to the store do not change it, and it restores to an equal store.
 */

import { describe, it, expect } from 'vitest';
import type { LogEntry } from '../../src/mesh/consensus';
import { KeyValueStore } from '../../src/mesh/state_machine';

function set(index: number, key: string, value: unknown): LogEntry {
  return { index, term: 1, command: 'SET', payload: { key, value } };
}

function del(index: number, key: string): LogEntry {
  return { index, term: 1, command: 'DELETE', payload: { key } };
}

function applied(entries: LogEntry[]): KeyValueStore {
  const store = new KeyValueStore();
  for (const entry of entries) store.apply(entry);
  return store;
}

describe('referenceImage', () => {
  it('has the same content as a full image', () => {
    const store = applied([set(1, 'a', { x: [1] }), set(2, 'b', 'two'), set(3, 'a', 'replaced')]);
    expect(store.referenceImage()).toEqual(store.image());
  });

  it('is not changed by a later write to the same key', () => {
    const store = applied([set(1, 'a', 'old')]);
    const reference = store.referenceImage();
    store.apply(set(2, 'a', 'new'));
    expect(reference.data.a).toBe('old');
    expect(reference.lastApplied).toBe(1);
  });

  it('is not changed by a later delete or by new keys', () => {
    const store = applied([set(1, 'a', 1), set(2, 'b', 2)]);
    const reference = store.referenceImage();
    store.apply(del(3, 'a'));
    store.apply(set(4, 'c', 3));
    expect(reference.data).toEqual({ a: 1, b: 2 });
    expect(reference.lastApplied).toBe(2);
  });

  it('restores to a store with the same state', () => {
    const store = applied([set(1, 'a', { x: 1 }), set(2, 'b', [1, 2]), del(3, 'a')]);
    const target = new KeyValueStore();
    target.restore(store.referenceImage());
    expect(target.snapshot()).toEqual(store.snapshot());
    expect(target.lastApplied).toBe(store.lastApplied);
  });

  it('is an empty image for an empty store', () => {
    expect(new KeyValueStore().referenceImage()).toEqual({ lastApplied: 0, data: {}, rejected: [] });
  });
});
