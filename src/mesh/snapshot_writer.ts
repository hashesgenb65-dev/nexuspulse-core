/**
 * Writes a snapshot file without holding the event loop.
 *
 * The file has exactly the format the durable store reads: one JSON object with `index`, `term` and
 * `data`, where `data` is the store image. The JSON is streamed: entries are serialized in batches,
 * and the loop gets a turn between batches. The file is flushed, renamed into place, and the
 * directory is flushed, the same order as the durable store uses.
 *
 * Nothing here decides when a snapshot is written or when the log is trimmed. That is the caller's
 * job, and it must wait for the file before it lets `state.json` refer to the snapshot.
 */

import { promises as fs } from 'node:fs';
import type { FileHandle } from 'node:fs/promises';
import * as path from 'node:path';
import type { StoreImage } from './state_machine.js';

/** Entries serialized between two yields to the event loop. */
export const SNAPSHOT_WRITE_BATCH_ENTRIES = 1_000;

export interface SnapshotFileInput {
  readonly index: number;
  readonly term: number;
  readonly data: StoreImage;
}

export interface WriteSnapshotFileOptions {
  /** Entries serialized between two yields. A positive integer. Defaults to SNAPSHOT_WRITE_BATCH_ENTRIES. */
  readonly batchEntries?: number;
}

function yieldToLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

async function writeAll(handle: FileHandle, text: string): Promise<void> {
  const bytes = Buffer.from(text, 'utf8');
  let offset = 0;
  while (offset < bytes.length) {
    const { bytesWritten } = await handle.write(bytes, offset, bytes.length - offset);
    offset += bytesWritten;
  }
}

async function exists(file: string): Promise<boolean> {
  try {
    await fs.access(file);
    return true;
  } catch {
    return false;
  }
}

async function syncDirectory(directory: string): Promise<void> {
  let handle: FileHandle;
  try {
    handle = await fs.open(directory, 'r');
  } catch {
    return;
  }
  try {
    await handle.sync();
  } catch {
    // Not supported on every platform; the file itself was flushed before the rename.
  } finally {
    await handle.close();
  }
}

/**
 * Writes `snapshot-<index>.json` in `directory` and returns its path. If that file already exists it is
 * left unchanged, because a snapshot never changes for its index.
 *
 * A value that is undefined is left out, as `JSON.stringify` leaves it out of an object.
 *
 * @throws {RangeError} If `batchEntries` is not a positive integer.
 * @throws {Error} If a write or flush fails. The temporary file is removed before the error is rethrown.
 */
export async function writeSnapshotFileAsync(
  directory: string,
  snapshot: SnapshotFileInput,
  options: WriteSnapshotFileOptions = {},
): Promise<string> {
  const batchEntries = options.batchEntries ?? SNAPSHOT_WRITE_BATCH_ENTRIES;
  if (!Number.isInteger(batchEntries) || batchEntries <= 0) {
    throw new RangeError(`batchEntries must be a positive integer, got ${batchEntries}`);
  }

  const target = path.join(directory, `snapshot-${snapshot.index}.json`);
  if (await exists(target)) return target;

  const temporary = `${target}.tmp`;
  const handle = await fs.open(temporary, 'w', 0o600);
  try {
    const { lastApplied, data, rejected } = snapshot.data;
    await writeAll(
      handle,
      `{"index":${JSON.stringify(snapshot.index)},"term":${JSON.stringify(snapshot.term)},` +
        `"data":{"lastApplied":${JSON.stringify(lastApplied)},"data":{`,
    );

    let batch = '';
    let written = 0;
    for (const key of Object.keys(data)) {
      const value = data[key];
      if (value === undefined) continue;
      batch += (written === 0 ? '' : ',') + JSON.stringify(key) + ':' + JSON.stringify(value);
      written++;
      if (written % batchEntries === 0) {
        await writeAll(handle, batch);
        batch = '';
        await yieldToLoop();
      }
    }
    await writeAll(handle, `${batch}},"rejected":${JSON.stringify(rejected)}}}`);
    await handle.sync();
    await handle.close();
  } catch (error) {
    await handle.close().catch(() => undefined);
    await fs.rm(temporary, { force: true }).catch(() => undefined);
    throw error;
  }

  await fs.rename(temporary, target);
  await syncDirectory(directory);
  return target;
}
