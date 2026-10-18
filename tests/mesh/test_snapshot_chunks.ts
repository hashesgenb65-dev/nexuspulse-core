import { describe, expect, it } from 'vitest';
import type { SnapshotOffer } from '../../src/mesh/consensus.js';
import {
  MAX_CHUNK_BYTES,
  MAX_SNAPSHOT_BYTES,
  SnapshotAssembler,
  encodeSnapshot,
  sliceChunk,
  type SnapshotChunk,
} from '../../src/mesh/snapshot_chunks.js';

const snapshot: SnapshotOffer = {
  index: 7,
  term: 2,
  data: { note: 'héllo ✓ 漢字', rows: [1, 2, 3], nested: { ok: true } },
};

function transfer(offer: SnapshotOffer, size: number): SnapshotChunk[] {
  const bytes = encodeSnapshot(offer);
  const chunks: SnapshotChunk[] = [];
  for (let offset = 0; offset < bytes.length; offset += size) {
    chunks.push(sliceChunk(bytes, offer.term, offer.index, offset, size));
  }
  return chunks;
}

function header(overrides: Partial<SnapshotChunk> = {}): SnapshotChunk {
  return { term: 2, index: 7, total: 4, offset: 0, data: Buffer.from('abcd').toString('base64'), ...overrides };
}

describe('encodeSnapshot and sliceChunk', () => {
  it('encodes the snapshot as JSON that decodes back to the same offer', () => {
    const bytes = encodeSnapshot(snapshot);
    expect(JSON.parse(bytes.toString('utf8'))).toEqual({ index: 7, term: 2, data: snapshot.data });
  });

  it('cuts chunks whose decoded bytes concatenate back to the whole encoding', () => {
    const bytes = encodeSnapshot(snapshot);
    const chunks = transfer(snapshot, 5);
    const rebuilt = Buffer.concat(chunks.map((chunk) => Buffer.from(chunk.data, 'base64')));
    expect(rebuilt.equals(bytes)).toBe(true);
    expect(chunks.every((chunk) => chunk.total === bytes.length)).toBe(true);
  });

  it('keeps every chunk frame well under the 1 MiB frame cap', () => {
    const big: SnapshotOffer = { index: 1, term: 1, data: 'x'.repeat(3 * MAX_CHUNK_BYTES) };
    const chunks = transfer(big, MAX_CHUNK_BYTES);
    expect(chunks.length).toBeGreaterThan(2);
    for (const chunk of chunks) {
      expect(JSON.stringify(chunk).length).toBeLessThan(1024 * 1024);
    }
  });

  it('rejects offsets outside the snapshot and sizes outside the allowed range', () => {
    const bytes = encodeSnapshot(snapshot);
    expect(() => sliceChunk(bytes, 2, 7, bytes.length, 4)).toThrow(RangeError);
    expect(() => sliceChunk(bytes, 2, 7, -1, 4)).toThrow(RangeError);
    expect(() => sliceChunk(bytes, 2, 7, 0, 0)).toThrow(RangeError);
    expect(() => sliceChunk(bytes, 2, 7, 0, MAX_CHUNK_BYTES + 1)).toThrow(RangeError);
  });

  it('refuses to encode data that is not JSON-serializable', () => {
    expect(() => encodeSnapshot({ index: 1, term: 1, data: 10n })).toThrow(TypeError);
  });

  it('refuses a snapshot whose encoding exceeds the size limit', () => {
    const huge: SnapshotOffer = { index: 1, term: 1, data: 'x'.repeat(MAX_SNAPSHOT_BYTES) };
    expect(() => encodeSnapshot(huge)).toThrow(RangeError);
  }, 30_000);
});

describe('SnapshotAssembler', () => {
  it('returns the snapshot only when the last chunk arrives', () => {
    const assembler = new SnapshotAssembler();
    const chunks = transfer(snapshot, 7);
    const last = chunks.pop();
    expect(last).toBeDefined();

    for (const chunk of chunks) {
      expect(assembler.accept(chunk).status).toBe('partial');
    }
    const result = assembler.accept(last!);
    expect(result).toEqual({ status: 'complete', snapshot });
    expect(assembler.nextOffset).toBe(0);
  });

  it('reports the next offset after each in-order chunk', () => {
    const assembler = new SnapshotAssembler();
    const chunks = transfer(snapshot, 10);
    let expected = 0;
    for (const chunk of chunks.slice(0, -1)) {
      expected += Buffer.from(chunk.data, 'base64').length;
      expect(assembler.accept(chunk)).toEqual({ status: 'partial', nextOffset: expected });
    }
  });

  it('ignores a duplicate chunk and names the offset to resume from', () => {
    const assembler = new SnapshotAssembler();
    const chunks = transfer(snapshot, 10);
    const first = chunks[0]!;
    const second = chunks[1]!;
    assembler.accept(first);
    assembler.accept(second);

    const again = assembler.accept(first);
    expect(again).toEqual({ status: 'partial', nextOffset: assembler.nextOffset });
    expect(assembler.nextOffset).toBe(Buffer.from(first.data, 'base64').length + Buffer.from(second.data, 'base64').length);
  });

  it('ignores an out-of-order chunk and asks for the expected offset', () => {
    const assembler = new SnapshotAssembler();
    const chunks = transfer(snapshot, 10);
    assembler.accept(chunks[0]!);

    const skipped = assembler.accept(chunks[2]!);
    expect(skipped).toEqual({ status: 'partial', nextOffset: assembler.nextOffset });
    expect(assembler.nextOffset).toBe(Buffer.from(chunks[0]!.data, 'base64').length);
  });

  it('starts over when a chunk from a new transfer begins at offset 0', () => {
    const assembler = new SnapshotAssembler();
    const old = transfer(snapshot, 10);
    assembler.accept(old[0]!);
    assembler.accept(old[1]!);

    const newer: SnapshotOffer = { ...snapshot, index: 9 };
    const fresh = transfer(newer, 10);
    expect(assembler.accept(fresh[0]!).status).toBe('partial');
    expect(assembler.nextOffset).toBe(Buffer.from(fresh[0]!.data, 'base64').length);

    let result: ReturnType<SnapshotAssembler['accept']> | undefined;
    for (const chunk of fresh.slice(1)) result = assembler.accept(chunk);
    expect(result).toEqual({ status: 'complete', snapshot: newer });
  });

  it('ignores a chunk at a non-zero offset when no transfer is in progress', () => {
    const assembler = new SnapshotAssembler();
    const chunks = transfer(snapshot, 10);
    expect(assembler.accept(chunks[1]!)).toEqual({ status: 'partial', nextOffset: 0 });
    expect(assembler.nextOffset).toBe(0);
  });

  it('discards a partial transfer on reset', () => {
    const assembler = new SnapshotAssembler();
    const chunks = transfer(snapshot, 10);
    assembler.accept(chunks[0]!);
    assembler.reset();
    expect(assembler.nextOffset).toBe(0);
    expect(assembler.accept(chunks[1]!)).toEqual({ status: 'partial', nextOffset: 0 });
  });

  it('refuses a chunk whose data is not base64', () => {
    const result = new SnapshotAssembler().accept(header({ data: 'not base64!' }));
    expect(result).toEqual({ status: 'refused', reason: 'chunk data is not base64' });
  });

  it('refuses a chunk with empty data', () => {
    const result = new SnapshotAssembler().accept(header({ data: '' }));
    expect(result.status).toBe('refused');
  });

  it('refuses a chunk whose data is larger than one chunk allows', () => {
    const oversized = Buffer.alloc(MAX_CHUNK_BYTES + 1, 0x61).toString('base64');
    const result = new SnapshotAssembler().accept(header({ total: MAX_CHUNK_BYTES + 1, data: oversized }));
    expect(result.status).toBe('refused');
  });

  it('refuses a chunk whose offset is outside the snapshot', () => {
    const result = new SnapshotAssembler().accept(header({ offset: 4 }));
    expect(result.status).toBe('refused');
  });

  it('refuses a snapshot total above the size limit', () => {
    const result = new SnapshotAssembler().accept(header({ total: MAX_SNAPSHOT_BYTES + 1 }));
    expect(result.status).toBe('refused');
  });

  it('refuses a chunk that runs past the declared total', () => {
    const result = new SnapshotAssembler().accept(header({ total: 2 }));
    expect(result).toEqual({ status: 'refused', reason: 'chunk runs past the end of the snapshot' });
  });

  it('refuses a completed transfer whose bytes are not JSON', () => {
    const bytes = Buffer.from('not json');
    const result = new SnapshotAssembler().accept({
      term: 2,
      index: 7,
      total: bytes.length,
      offset: 0,
      data: bytes.toString('base64'),
    });
    expect(result).toEqual({ status: 'refused', reason: 'snapshot bytes are not JSON' });
  });

  it('refuses a completed transfer whose header does not match its chunks', () => {
    const bytes = encodeSnapshot({ index: 8, term: 2, data: 'x' });
    const result = new SnapshotAssembler().accept({
      term: 2,
      index: 7,
      total: bytes.length,
      offset: 0,
      data: bytes.toString('base64'),
    });
    expect(result).toEqual({ status: 'refused', reason: 'snapshot header does not match the transfer' });
  });

  it('is clear for the next transfer after a refusal', () => {
    const assembler = new SnapshotAssembler();
    const bad = Buffer.from('nope');
    assembler.accept({ term: 2, index: 7, total: bad.length, offset: 0, data: bad.toString('base64') });
    expect(assembler.nextOffset).toBe(0);

    const chunks = transfer(snapshot, 64);
    let result: ReturnType<SnapshotAssembler['accept']> | undefined;
    for (const chunk of chunks) result = assembler.accept(chunk);
    expect(result).toEqual({ status: 'complete', snapshot });
  });
});
