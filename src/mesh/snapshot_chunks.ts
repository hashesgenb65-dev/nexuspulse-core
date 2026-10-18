/**
 * Splits a snapshot into bounded chunks for transfer, and reassembles them on the follower.
 *
 * A snapshot is encoded as UTF-8 JSON. Each chunk carries a byte range of that encoding in base64,
 * so a chunk is plain text inside a JSON frame and can never split a multi-byte character. The
 * assembler accepts chunks strictly in order. A chunk that is not the next one is not an error: the
 * result names the offset the leader should resume from, so duplicates and retries are harmless.
 */

import type { SnapshotOffer } from './consensus.js';

/** Largest byte range one chunk may carry before base64 encoding. Keeps each frame well under 1 MiB. */
export const MAX_CHUNK_BYTES = 512 * 1024;

/** Largest encoded snapshot the assembler accepts. Bounds the memory a single peer can make us hold. */
export const MAX_SNAPSHOT_BYTES = 256 * 1024 * 1024;

const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/** Identifies one transfer. Two chunks belong to the same transfer only if all four fields match. */
export interface ChunkHeader {
  readonly term: number;
  readonly index: number;
  /** Length in bytes of the whole encoded snapshot. */
  readonly total: number;
  /** Byte offset of this chunk within the encoded snapshot. */
  readonly offset: number;
}

export interface SnapshotChunk extends ChunkHeader {
  /** Base64 of the bytes at [offset, offset + decoded length). */
  readonly data: string;
}

export type ChunkResult =
  /** The chunk was taken or ignored. The leader should send `nextOffset` next. */
  | { readonly status: 'partial'; readonly nextOffset: number }
  /** The final chunk arrived and the snapshot decoded. */
  | { readonly status: 'complete'; readonly snapshot: SnapshotOffer }
  /** The chunk is malformed. The partial transfer, if any, has been discarded. */
  | { readonly status: 'refused'; readonly reason: string };

/**
 * Encodes a snapshot as the UTF-8 JSON bytes that are sent in chunks.
 *
 * @throws {TypeError} If the snapshot data cannot be represented as JSON.
 * @throws {RangeError} If the encoding exceeds {@link MAX_SNAPSHOT_BYTES}.
 */
export function encodeSnapshot(snapshot: SnapshotOffer): Buffer {
  const json = JSON.stringify({ index: snapshot.index, term: snapshot.term, data: snapshot.data });
  if (json === undefined) throw new TypeError('snapshot data is not JSON-serializable');
  const bytes = Buffer.from(json, 'utf8');
  if (bytes.length > MAX_SNAPSHOT_BYTES) {
    throw new RangeError(`snapshot is ${bytes.length} bytes, the limit is ${MAX_SNAPSHOT_BYTES}`);
  }
  return bytes;
}

/**
 * Cuts the chunk that starts at `offset` out of an encoded snapshot.
 *
 * @throws {RangeError} If the offset is outside the snapshot or the size is not 1..MAX_CHUNK_BYTES.
 */
export function sliceChunk(
  bytes: Buffer,
  term: number,
  index: number,
  offset: number,
  size: number = MAX_CHUNK_BYTES,
): SnapshotChunk {
  if (!Number.isInteger(offset) || offset < 0 || offset >= bytes.length) {
    throw new RangeError(`offset ${offset} is outside a snapshot of ${bytes.length} bytes`);
  }
  if (!Number.isInteger(size) || size <= 0 || size > MAX_CHUNK_BYTES) {
    throw new RangeError(`chunk size must be an integer from 1 to ${MAX_CHUNK_BYTES}, got ${size}`);
  }
  const end = Math.min(bytes.length, offset + size);
  return {
    term,
    index,
    total: bytes.length,
    offset,
    data: bytes.subarray(offset, end).toString('base64'),
  };
}

/** Receives the chunks of one snapshot transfer at a time, and reports when the snapshot is whole. */
export class SnapshotAssembler {
  private header: ChunkHeader | undefined;
  private parts: Buffer[] = [];
  private received = 0;

  /** Bytes of the transfer in progress. The offset the next in-order chunk must start at. */
  get nextOffset(): number {
    return this.received;
  }

  /** Discards any transfer in progress. */
  reset(): void {
    this.header = undefined;
    this.parts = [];
    this.received = 0;
  }

  /**
   * Takes one chunk. Chunks must arrive in order. A duplicate or out-of-order chunk is ignored, and
   * the result carries the offset to resume from. A chunk that starts a new transfer must start at
   * offset 0; any other chunk for an unknown transfer is ignored the same way.
   */
  accept(chunk: SnapshotChunk): ChunkResult {
    const problem = validateChunk(chunk);
    if (problem !== undefined) return { status: 'refused', reason: problem };

    if (this.header === undefined || !sameTransfer(this.header, chunk)) {
      if (chunk.offset !== 0) return { status: 'partial', nextOffset: 0 };
      this.reset();
      this.header = { term: chunk.term, index: chunk.index, total: chunk.total, offset: 0 };
    }

    if (chunk.offset !== this.received) return { status: 'partial', nextOffset: this.received };

    const bytes = Buffer.from(chunk.data, 'base64');
    if (this.received + bytes.length > chunk.total) {
      return { status: 'refused', reason: 'chunk runs past the end of the snapshot' };
    }
    this.parts.push(bytes);
    this.received += bytes.length;
    if (this.received < chunk.total) return { status: 'partial', nextOffset: this.received };

    const header = this.header;
    const whole = Buffer.concat(this.parts);
    this.reset();
    return decodeSnapshot(whole, header);
  }
}

function validateChunk(chunk: SnapshotChunk): string | undefined {
  if (!Number.isInteger(chunk.term) || chunk.term < 0) return 'chunk term is not a non-negative integer';
  if (!Number.isInteger(chunk.index) || chunk.index < 1) return 'chunk index is not a positive integer';
  if (!Number.isInteger(chunk.total) || chunk.total < 1 || chunk.total > MAX_SNAPSHOT_BYTES) {
    return `chunk total must be an integer from 1 to ${MAX_SNAPSHOT_BYTES}`;
  }
  if (!Number.isInteger(chunk.offset) || chunk.offset < 0 || chunk.offset >= chunk.total) {
    return 'chunk offset is outside the snapshot';
  }
  if (typeof chunk.data !== 'string' || !BASE64.test(chunk.data)) return 'chunk data is not base64';
  const decoded = Buffer.byteLength(chunk.data, 'base64');
  if (decoded < 1 || decoded > MAX_CHUNK_BYTES) return 'chunk data is empty or too large';
  return undefined;
}

function sameTransfer(a: ChunkHeader, b: ChunkHeader): boolean {
  return a.term === b.term && a.index === b.index && a.total === b.total;
}

function decodeSnapshot(bytes: Buffer, header: ChunkHeader): ChunkResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(bytes.toString('utf8'));
  } catch {
    return { status: 'refused', reason: 'snapshot bytes are not JSON' };
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    return { status: 'refused', reason: 'snapshot is not an object' };
  }
  const fields = parsed as Record<string, unknown>;
  if (fields['index'] !== header.index || fields['term'] !== header.term || !('data' in fields)) {
    return { status: 'refused', reason: 'snapshot header does not match the transfer' };
  }
  return { status: 'complete', snapshot: { index: header.index, term: header.term, data: fields['data'] } };
}
