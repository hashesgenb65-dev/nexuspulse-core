/**
 * @file tests/db/test_storage.ts
 * @description Vitest suite for /src/db/storage.ts.
 *
 * Every test runs against real files in a fresh temporary directory. Crash
 * damage is simulated by editing the bytes on disk, and record checksums are
 * computed with Node's zlib.crc32 so the tests do not reuse the code under test.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { crc32 as zlibCrc32 } from 'node:zlib';
import {
  RecordStore,
  StorageError,
  StorageOptionError,
  StorageKeyError,
  StorageValidationError,
  StorageRecordTooLargeError,
  StorageLockedError,
  StorageClosedError,
  StorageCorruptionError,
  RECORD_MAGIC,
  HEADER_BYTES,
  MAX_KEY_BYTES,
  MAX_PAYLOAD_BYTES,
} from '../../src/db/record_store.js';
import { defineSchema, type Schema } from '../../src/db/schema.js';

interface Doc {
  name: string;
  count?: number;
}

const DOC_SCHEMA: Schema = defineSchema({
  type: 'object',
  properties: {
    name: { type: 'string', minLength: 1 },
    count: { type: 'integer', min: 0 },
  },
  required: ['name'],
});

/** Builds one record exactly as the on-disk format describes, with an independent checksum. */
function rawRecord(body: string): Buffer {
  const bodyBytes = Buffer.from(body, 'utf8');
  const header = Buffer.alloc(HEADER_BYTES);
  header.writeUInt32BE(RECORD_MAGIC, 0);
  header.writeUInt32BE(bodyBytes.length, 4);
  header.writeUInt32BE(zlibCrc32(bodyBytes), 8);
  return Buffer.concat([header, bodyBytes]);
}

const putRecord = (key: string, value: unknown): Buffer =>
  rawRecord(JSON.stringify({ op: 'put', key, value }));
const delRecord = (key: string): Buffer => rawRecord(JSON.stringify({ op: 'del', key }));

let dir: string;
let file: string;
const opened: RecordStore<unknown>[] = [];

/** Opens a store and tracks it so afterEach can close it even when a test fails. */
function open<T = Doc>(options: { schema?: Schema; fsync?: boolean } = {}): RecordStore<T> {
  const store = RecordStore.open<T>({
    file,
    schema: options.schema ?? DOC_SCHEMA,
    ...(options.fsync === undefined ? {} : { fsync: options.fsync }),
  });
  opened.push(store as RecordStore<unknown>);
  return store;
}

beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'np-store-'));
  file = path.join(dir, 'data.npdb');
});

afterEach(() => {
  for (const store of opened.splice(0)) store.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Errors and options
// ---------------------------------------------------------------------------

describe('error classes', () => {
  it('carry stable codes and inherit from StorageError', () => {
    const err = new StorageKeyError('bad', { x: 1 });
    expect(err).toBeInstanceOf(StorageError);
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe('StorageKeyError');
    expect(err.code).toBe('STORAGE_INVALID_KEY');
    expect(err.details).toEqual({ x: 1 });
    expect(Object.isFrozen(err.details)).toBe(true);
  });

  it('use the documented code for each class', () => {
    expect(new StorageOptionError('x').code).toBe('STORAGE_INVALID_OPTION');
    expect(new StorageValidationError('x').code).toBe('STORAGE_VALIDATION_FAILED');
    expect(new StorageRecordTooLargeError('x').code).toBe('STORAGE_RECORD_TOO_LARGE');
    expect(new StorageLockedError('x').code).toBe('STORAGE_LOCKED');
    expect(new StorageClosedError('x').code).toBe('STORAGE_CLOSED');
    expect(new StorageCorruptionError('x').code).toBe('STORAGE_CORRUPT');
  });
});

describe('open options', () => {
  it('rejects options that are not an object', () => {
    expect(() => RecordStore.open(null as never)).toThrow(StorageOptionError);
  });

  it('rejects an empty or non-string file name', () => {
    expect(() => RecordStore.open({ file: '', schema: DOC_SCHEMA })).toThrow(/file must be a non-empty string/);
    expect(() => RecordStore.open({ file: 7 as unknown as string, schema: DOC_SCHEMA })).toThrow(
      StorageOptionError,
    );
  });

  it('rejects a directory that does not exist', () => {
    const missing = path.join(dir, 'nope', 'data.npdb');
    expect(() => RecordStore.open({ file: missing, schema: DOC_SCHEMA })).toThrow(/directory does not exist/);
  });

  it('rejects a non-boolean fsync option', () => {
    expect(() =>
      RecordStore.open({ file, schema: DOC_SCHEMA, fsync: 'yes' as unknown as boolean }),
    ).toThrow(/fsync must be a boolean/);
  });

  it('rejects an invalid schema definition as a StorageOptionError', () => {
    expect(() => RecordStore.open({ file, schema: { type: 'nope' } as unknown as Schema })).toThrow(
      /schema is invalid/,
    );
  });

  it('creates the file on open and leaves the lock file absent after close', () => {
    const store = open();
    expect(fs.existsSync(file)).toBe(true);
    expect(fs.existsSync(`${file}.lock`)).toBe(true);
    store.close();
    expect(fs.existsSync(`${file}.lock`)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Basic operations
// ---------------------------------------------------------------------------

describe('basic operations', () => {
  it('round-trips a value and reports presence and size', () => {
    const store = open();
    store.put('a', { name: 'alpha', count: 1 });
    expect(store.get('a')).toEqual({ name: 'alpha', count: 1 });
    expect(store.has('a')).toBe(true);
    expect(store.size).toBe(1);
  });

  it('returns undefined for an absent key', () => {
    const store = open();
    expect(store.get('missing')).toBeUndefined();
    expect(store.has('missing')).toBe(false);
  });

  it('overwrites a value and keeps the key in its original position', () => {
    const store = open();
    store.put('first', { name: 'one' });
    store.put('second', { name: 'two' });
    store.put('first', { name: 'uno' });
    expect(store.get('first')).toEqual({ name: 'uno' });
    expect(store.keys()).toEqual(['first', 'second']);
    expect(store.size).toBe(2);
  });

  it('returns a fresh copy, so mutating a result does not change the store', () => {
    const store = open();
    store.put('k', { name: 'orig' });
    const copy = store.get('k') as { name: string };
    copy.name = 'changed';
    expect(store.get('k')).toEqual({ name: 'orig' });
  });

  it('does not alias the caller\'s object after put', () => {
    const store = open();
    const input = { name: 'before' };
    store.put('k', input);
    input.name = 'after';
    expect(store.get('k')).toEqual({ name: 'before' });
  });

  it('delete returns true for a present key and false otherwise', () => {
    const store = open();
    store.put('k', { name: 'x' });
    expect(store.delete('k')).toBe(true);
    expect(store.delete('k')).toBe(false);
    expect(store.has('k')).toBe(false);
    expect(store.size).toBe(0);
  });

  it('does not write anything when deleting an absent key', () => {
    const store = open();
    const before = fs.statSync(file).size;
    store.delete('ghost');
    expect(fs.statSync(file).size).toBe(before);
    expect(store.stats().deletes).toBe(0);
  });

  it('entries() yields fresh copies in insertion order', () => {
    const store = open();
    store.put('b', { name: 'two' });
    store.put('a', { name: 'one' });
    const seen = [...store.entries()];
    expect(seen).toEqual([
      ['b', { name: 'two' }],
      ['a', { name: 'one' }],
    ]);
  });

  it('stores zero as a real value, distinct from an absent key', () => {
    const numbers = open<number>({ schema: defineSchema({ type: 'number' }) });
    numbers.put('zero', 0);
    expect(numbers.get('zero')).toBe(0);
    expect(numbers.has('zero')).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Keys and value validation
// ---------------------------------------------------------------------------

describe('key validation', () => {
  it.each<[string, unknown]>([
    ['an empty string', ''],
    ['a number', 1],
    ['null', null],
    ['undefined', undefined],
  ])('rejects %s', (_label, key) => {
    const store = open();
    expect(() => store.put(key as string, { name: 'x' })).toThrow(StorageKeyError);
    expect(() => store.get(key as string)).toThrow(StorageKeyError);
    expect(() => store.has(key as string)).toThrow(StorageKeyError);
    expect(() => store.delete(key as string)).toThrow(StorageKeyError);
  });

  it('accepts a key of exactly MAX_KEY_BYTES bytes and rejects one byte more', () => {
    const store = open();
    const atLimit = 'k'.repeat(MAX_KEY_BYTES);
    store.put(atLimit, { name: 'ok' });
    expect(store.has(atLimit)).toBe(true);
    expect(() => store.put('k'.repeat(MAX_KEY_BYTES + 1), { name: 'x' })).toThrow(StorageKeyError);
  });

  it('measures the key limit in UTF-8 bytes, not characters', () => {
    const store = open();
    // "é" is two bytes in UTF-8, so 129 of them are 258 bytes.
    expect(() => store.put('é'.repeat(129), { name: 'x' })).toThrow(StorageKeyError);
    store.put('é'.repeat(128), { name: 'x' });
    expect(store.has('é'.repeat(128))).toBe(true);
  });

  it('round-trips keys containing separators, quotes and lone surrogates', () => {
    const store = open();
    const tricky = ['a/b', 'quote"here', 'back\\slash', 'lone\uD800'];
    for (const key of tricky) store.put(key, { name: key });
    store.close();
    const reopened = open();
    for (const key of tricky) expect(reopened.get(key)).toEqual({ name: key });
  });
});

describe('value validation', () => {
  it('rejects a value that fails the schema and writes nothing', () => {
    const store = open();
    const before = fs.statSync(file).size;
    expect(() => store.put('k', { count: 1 } as unknown as Doc)).toThrow(StorageValidationError);
    expect(fs.statSync(file).size).toBe(before);
    expect(store.has('k')).toBe(false);
  });

  it('reports every issue with its path', () => {
    const store = open();
    try {
      store.put('k', { name: '', count: -1, extra: true } as unknown as Doc);
      throw new Error('expected a validation error');
    } catch (err) {
      expect(err).toBeInstanceOf(StorageValidationError);
      const issues = (err as StorageValidationError).details['issues'] as { path: string }[];
      expect(issues.map((i) => i.path)).toEqual(['/name', '/count', '/extra']);
    }
  });

  it('leaves the previous value in place when an overwrite fails validation', () => {
    const store = open();
    store.put('k', { name: 'good' });
    expect(() => store.put('k', { name: '' })).toThrow(StorageValidationError);
    expect(store.get('k')).toEqual({ name: 'good' });
  });

  it('rejects a record larger than the payload limit before writing it', () => {
    const big = open<string>({ schema: defineSchema({ type: 'string' }) });
    const before = fs.statSync(file).size;
    expect(() => big.put('k', 'x'.repeat(MAX_PAYLOAD_BYTES))).toThrow(StorageRecordTooLargeError);
    expect(fs.statSync(file).size).toBe(before);
    expect(big.has('k')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

describe('persistence across reopen', () => {
  it('restores puts and deletes exactly', () => {
    const first = open();
    first.put('keep', { name: 'kept', count: 2 });
    first.put('drop', { name: 'gone' });
    first.put('keep', { name: 'kept-again' });
    first.delete('drop');
    first.close();

    const second = open();
    expect(second.keys()).toEqual(['keep']);
    expect(second.get('keep')).toEqual({ name: 'kept-again' });
    expect(second.get('drop')).toBeUndefined();
  });

  it('keeps insertion order across reopen', () => {
    const first = open();
    first.put('c', { name: 'c' });
    first.put('a', { name: 'a' });
    first.put('b', { name: 'b' });
    first.close();
    expect(open().keys()).toEqual(['c', 'a', 'b']);
  });

  it('appends to the existing file on reopen instead of overwriting it', () => {
    const first = open();
    first.put('one', { name: '1' });
    first.close();
    const second = open();
    second.put('two', { name: '2' });
    second.close();
    const third = open();
    expect(third.keys()).toEqual(['one', 'two']);
  });

  it('starts empty and reports zero counters for a new file', () => {
    const store = open();
    expect(store.stats()).toEqual({
      entries: 0,
      appends: 0,
      deletes: 0,
      compactions: 0,
      recoveredTornBytes: 0,
      fileBytes: 0,
    });
  });

  it('works with fsync disabled', () => {
    const store = open({ fsync: false });
    store.put('k', { name: 'no-sync' });
    store.close();
    expect(open({ fsync: false }).get('k')).toEqual({ name: 'no-sync' });
  });
});

// ---------------------------------------------------------------------------
// Locking and lifecycle
// ---------------------------------------------------------------------------

describe('locking and lifecycle', () => {
  it('refuses a second open while the first store is open', () => {
    open();
    expect(() => RecordStore.open({ file, schema: DOC_SCHEMA })).toThrow(StorageLockedError);
  });

  it('allows opening again after close', () => {
    const first = open();
    first.close();
    expect(() => open()).not.toThrow();
  });

  it('does not leave a lock behind when the file is corrupt and open fails', () => {
    fs.writeFileSync(file, rawRecord('{"op":"nope","key":"k"}'));
    expect(() => RecordStore.open({ file, schema: DOC_SCHEMA })).toThrow(StorageCorruptionError);
    expect(fs.existsSync(`${file}.lock`)).toBe(false);
  });

  it('throws StorageClosedError on every operation after close', () => {
    const store = open();
    store.put('k', { name: 'x' });
    store.close();
    expect(() => store.put('k', { name: 'y' })).toThrow(StorageClosedError);
    expect(() => store.get('k')).toThrow(StorageClosedError);
    expect(() => store.has('k')).toThrow(StorageClosedError);
    expect(() => store.delete('k')).toThrow(StorageClosedError);
    expect(() => store.keys()).toThrow(StorageClosedError);
    expect(() => [...store.entries()]).toThrow(StorageClosedError);
    expect(() => store.compact()).toThrow(StorageClosedError);
    expect(() => store.stats()).toThrow(StorageClosedError);
  });

  it('close is idempotent', () => {
    const store = open();
    store.close();
    expect(() => store.close()).not.toThrow();
  });

  it('a stale lock file from a crashed process blocks opening until removed', () => {
    fs.writeFileSync(`${file}.lock`, '99999\n');
    expect(() => open()).toThrow(/locked by another process/);
    fs.rmSync(`${file}.lock`);
    expect(() => open()).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// Crash recovery
// ---------------------------------------------------------------------------

describe('torn tail recovery', () => {
  it('drops a partial header at the end and reports the removed bytes', () => {
    fs.writeFileSync(file, Buffer.concat([putRecord('a', { name: 'alpha' }), Buffer.from([0x4e, 0x50, 0x44])]));
    const store = open();
    expect(store.get('a')).toEqual({ name: 'alpha' });
    expect(store.stats().recoveredTornBytes).toBe(3);
    expect(fs.statSync(file).size).toBe(putRecord('a', { name: 'alpha' }).length);
  });

  it('drops a record whose payload was cut short', () => {
    const good = putRecord('a', { name: 'alpha' });
    const partial = putRecord('b', { name: 'bravo' });
    fs.writeFileSync(file, Buffer.concat([good, partial.subarray(0, partial.length - 4)]));
    const store = open();
    expect(store.keys()).toEqual(['a']);
    expect(store.stats().recoveredTornBytes).toBe(partial.length - 4);
  });

  it('drops a zero-filled tail, as left by some filesystems after a crash', () => {
    fs.writeFileSync(file, Buffer.concat([putRecord('a', { name: 'alpha' }), Buffer.alloc(64)]));
    const store = open();
    expect(store.keys()).toEqual(['a']);
    expect(store.stats().recoveredTornBytes).toBe(64);
  });

  it('drops a final record whose checksum is wrong', () => {
    const good = putRecord('a', { name: 'alpha' });
    const bad = Buffer.from(putRecord('b', { name: 'bravo' }));
    const lastIndex = bad.length - 1;
    bad.writeUInt8(bad.readUInt8(lastIndex) ^ 0xff, lastIndex);
    fs.writeFileSync(file, Buffer.concat([good, bad]));
    const store = open();
    expect(store.keys()).toEqual(['a']);
    expect(store.stats().recoveredTornBytes).toBe(bad.length);
  });

  it('after recovery, new writes append cleanly and survive another reopen', () => {
    fs.writeFileSync(file, Buffer.concat([putRecord('a', { name: 'alpha' }), Buffer.from([1, 2])]));
    open().close();
    const store = open();
    store.put('b', { name: 'bravo' });
    store.close();
    expect(open().keys()).toEqual(['a', 'b']);
  });
});

describe('corruption that is not a torn tail', () => {
  it('reports a checksum mismatch in the middle of the log and refuses to open', () => {
    const first = putRecord('a', { name: 'alpha' });
    const damaged = Buffer.from(first);
    const target = HEADER_BYTES + 3;
    damaged.writeUInt8(damaged.readUInt8(target) ^ 0x01, target); // flip one bit in the body
    fs.writeFileSync(file, Buffer.concat([damaged, putRecord('b', { name: 'bravo' })]));
    try {
      open();
      throw new Error('expected corruption');
    } catch (err) {
      expect(err).toBeInstanceOf(StorageCorruptionError);
      expect((err as StorageCorruptionError).details['offset']).toBe(0);
      expect((err as StorageCorruptionError).message).toContain('checksum mismatch');
    }
    expect(fs.existsSync(`${file}.lock`)).toBe(false);
  });

  it('replays delete records that were written outside the store', () => {
    fs.writeFileSync(
      file,
      Buffer.concat([putRecord('a', { name: 'alpha' }), putRecord('b', { name: 'bravo' }), delRecord('a')]),
    );
    expect(open().keys()).toEqual(['b']);
  });

  it('reports a bad magic number followed by real data', () => {
    const bogus = Buffer.from(putRecord('a', { name: 'alpha' }));
    bogus.writeUInt32BE(0x12345678, 0);
    fs.writeFileSync(file, Buffer.concat([bogus, putRecord('b', { name: 'bravo' })]));
    expect(() => open()).toThrow(/bad magic number/);
  });

  it('reports a length field above the payload limit', () => {
    const header = Buffer.alloc(HEADER_BYTES);
    header.writeUInt32BE(RECORD_MAGIC, 0);
    header.writeUInt32BE(MAX_PAYLOAD_BYTES + 1, 4);
    fs.writeFileSync(file, header);
    expect(() => open()).toThrow(/exceeds the payload limit/);
  });

  it('reports a body that is not JSON even though its checksum is valid', () => {
    fs.writeFileSync(file, Buffer.concat([rawRecord('{not json'), putRecord('b', { name: 'x' })]));
    expect(() => open()).toThrow(/not valid JSON/);
  });

  it('reports a body that is JSON but not an object', () => {
    fs.writeFileSync(file, Buffer.concat([rawRecord('[1,2]'), putRecord('b', { name: 'x' })]));
    expect(() => open()).toThrow(/not an object/);
  });

  it('reports an invalid key inside a record', () => {
    fs.writeFileSync(file, Buffer.concat([rawRecord('{"op":"put","key":"","value":{"name":"x"}}'), putRecord('b', { name: 'y' })]));
    expect(() => open()).toThrow(/invalid key/);
  });

  it('reports an unknown operation', () => {
    fs.writeFileSync(file, Buffer.concat([rawRecord('{"op":"upsert","key":"a"}'), putRecord('b', { name: 'y' })]));
    expect(() => open()).toThrow(/unknown operation/);
  });

  it('reports a stored value that no longer matches the schema, with the issues attached', () => {
    fs.writeFileSync(file, Buffer.concat([putRecord('a', { count: 1 }), putRecord('b', { name: 'ok' })]));
    try {
      open();
      throw new Error('expected corruption');
    } catch (err) {
      expect(err).toBeInstanceOf(StorageCorruptionError);
      const issues = (err as StorageCorruptionError).details['issues'] as { path: string }[];
      expect(issues.map((i) => i.path)).toEqual(['/name']);
    }
  });

  it('a schema change that rejects old data stops the store from loading silently', () => {
    const v1 = open<Record<string, unknown>>({
      schema: defineSchema({ type: 'object', properties: {}, additionalProperties: true }),
    });
    v1.put('a', { legacy: true });
    v1.close();
    const strict = defineSchema({ type: 'object', properties: { name: { type: 'string' } }, required: ['name'] });
    expect(() => RecordStore.open({ file, schema: strict })).toThrow(StorageCorruptionError);
  });
});

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

describe('compaction', () => {
  it('shrinks the file after overwrites and deletes while keeping the live state', () => {
    const store = open();
    for (let i = 0; i < 50; i++) store.put('k', { name: `v${i}` });
    store.put('gone', { name: 'x' });
    store.delete('gone');
    const result = store.compact();
    expect(result.bytesAfter).toBeLessThan(result.bytesBefore);
    expect(result.bytesAfter).toBe(fs.statSync(file).size);
    expect(store.get('k')).toEqual({ name: 'v49' });
    expect(store.keys()).toEqual(['k']);
    expect(store.stats().compactions).toBe(1);
  });

  it('keeps insertion order through compaction', () => {
    const store = open();
    store.put('c', { name: 'c' });
    store.put('a', { name: 'a' });
    store.put('c', { name: 'c2' });
    store.compact();
    expect(store.keys()).toEqual(['c', 'a']);
    store.close();
    expect(open().keys()).toEqual(['c', 'a']);
  });

  it('new writes after compaction are durable across reopen', () => {
    const store = open();
    store.put('a', { name: 'before' });
    store.compact();
    store.put('b', { name: 'after' });
    store.close();
    const reopened = open();
    expect(reopened.get('a')).toEqual({ name: 'before' });
    expect(reopened.get('b')).toEqual({ name: 'after' });
  });

  it('compacting an empty store yields an empty file', () => {
    const store = open();
    const result = store.compact();
    expect(result).toEqual({ bytesBefore: 0, bytesAfter: 0 });
    expect(fs.statSync(file).size).toBe(0);
  });

  it('leaves no temporary file behind', () => {
    const store = open();
    store.put('a', { name: 'x' });
    store.compact();
    expect(fs.existsSync(`${file}.compact`)).toBe(false);
  });

  it('a compacted file reopens to the same state as before compaction', () => {
    const store = open();
    for (let i = 0; i < 20; i++) store.put(`k${i % 5}`, { name: `n${i}`, count: i });
    store.delete('k2');
    const before = [...store.entries()];
    store.compact();
    store.close();
    expect([...open().entries()]).toEqual(before);
  });
});

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

describe('stats', () => {
  it('counts appends and deletes and reflects the live size and file size', () => {
    const store = open();
    store.put('a', { name: 'x' });
    store.put('b', { name: 'y' });
    store.delete('a');
    const stats = store.stats();
    expect(stats.entries).toBe(1);
    expect(stats.appends).toBe(2);
    expect(stats.deletes).toBe(1);
    expect(stats.fileBytes).toBe(fs.statSync(file).size);
  });

  it('returns a frozen snapshot', () => {
    const store = open();
    expect(Object.isFrozen(store.stats())).toBe(true);
  });
});
