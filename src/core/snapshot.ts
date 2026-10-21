/**
 * Writes a point-in-time snapshot of the state machine to disk without blocking the event loop.
 *
 * Entries are written in batches of SNAPSHOT_BATCH_ENTRIES. Each batch is one awaited write, so the
 * loop runs between batches and the process never holds more than one batch of serialized text. The
 * file is flushed to disk before it is renamed into place, and the directory is flushed after the
 * rename, so a crash leaves either no snapshot or a complete one under the final name.
 */

import { promises as fs } from 'node:fs';
import type { FileHandle } from 'node:fs/promises';
import * as path from 'node:path';

/** Entries serialized and written between two yields to the event loop. */
export const SNAPSHOT_BATCH_ENTRIES = 1_000;

export interface SnapshotManagerOptions {
  readonly dataDir: string;
}

/** Yields once to the event loop, so timers and I/O callbacks can run. */
function yieldToLoop(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

/** Writes all of `text` to the handle. A single write call may write fewer bytes than asked. */
async function writeAll(handle: FileHandle, text: string): Promise<void> {
  const bytes = Buffer.from(text, 'utf8');
  let offset = 0;
  while (offset < bytes.length) {
    const { bytesWritten } = await handle.write(bytes, offset, bytes.length - offset);
    offset += bytesWritten;
  }
}

/** Flushes a directory entry so a rename survives a crash. Platforms that cannot open directories skip it. */
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
    // Directory fsync is not supported everywhere; the file itself was already flushed.
  } finally {
    await handle.close();
  }
}

export class SnapshotManager {
  private readonly dataDir: string;

  constructor(config: SnapshotManagerOptions) {
    this.dataDir = config.dataDir;
  }

  /**
   * Writes the map as a snapshot for `lastIncludedIndex` and returns the final path. The first line
   * is a header with the index and a timestamp; each following line is one `{ key, value }` entry.
   *
   * @throws {Error} If any write or flush fails. The temporary file is removed before the error is rethrown.
   */
  async createSnapshotAsync(stateMachineMap: Map<string, string>, lastIncludedIndex: number): Promise<string> {
    const tmpPath = path.join(this.dataDir, `snapshot-${lastIncludedIndex}.tmp`);
    const finalPath = path.join(this.dataDir, `snapshot-${lastIncludedIndex}.bin`);

    const handle = await fs.open(tmpPath, 'w');
    try {
      await writeAll(handle, JSON.stringify({ lastIncludedIndex, timestamp: Date.now() }) + '\n');

      let batch = '';
      let count = 0;
      for (const [key, value] of stateMachineMap.entries()) {
        batch += JSON.stringify({ key, value }) + '\n';
        count++;
        if (count % SNAPSHOT_BATCH_ENTRIES === 0) {
          await writeAll(handle, batch);
          batch = '';
          await yieldToLoop();
        }
      }
      if (batch.length > 0) await writeAll(handle, batch);

      await handle.sync();
    } catch (error) {
      await handle.close().catch(() => undefined);
      await fs.rm(tmpPath, { force: true }).catch(() => undefined);
      throw error;
    }
    await handle.close();

    await fs.rename(tmpPath, finalPath);
    await syncDirectory(this.dataDir);
    return finalPath;
  }
}
