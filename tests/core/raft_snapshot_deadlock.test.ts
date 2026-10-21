/**
 * Snapshot writes must not block the event loop. A blocked loop cannot send heartbeats, which is how
 * a leader loses its followers. These tests run the real SnapshotManager, and a blocking writer as a
 * control so the timing check can tell the two apart.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { SnapshotManager } from '../../src/core/snapshot';

const ENTRY_COUNT = 50_000;
const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function freshDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'snapshot-'));
  dirs.push(dir);
  return dir;
}

function bigMap(count: number): Map<string, string> {
  const map = new Map<string, string>();
  for (let i = 0; i < count; i++) map.set(`metric:${i}`, `payload-${i}-${'x'.repeat(64)}`);
  return map;
}

/** Counts how many times a 1 ms timer fires while `work` runs. A blocked event loop fires it zero times. */
async function timerTicksDuring(work: () => unknown): Promise<number> {
  let ticks = 0;
  const timer = setInterval(() => {
    ticks++;
  }, 1);
  try {
    await work();
  } finally {
    clearInterval(timer);
  }
  return ticks;
}

/** The control: the legacy approach, serializing the whole map and writing it in one synchronous call. */
function writeSnapshotBlocking(dir: string, map: Map<string, string>, index: number): void {
  const lines = [JSON.stringify({ lastIncludedIndex: index, timestamp: Date.now() })];
  for (const [key, value] of map) lines.push(JSON.stringify({ key, value }));
  fs.writeFileSync(path.join(dir, `snapshot-${index}.bin`), lines.join('\n') + '\n');
}

describe('SnapshotManager.createSnapshotAsync', () => {
  it('writes a header and every entry in order, and leaves no temporary file behind', async () => {
    const dir = freshDir();
    const map = bigMap(ENTRY_COUNT);
    const finalPath = await new SnapshotManager({ dataDir: dir }).createSnapshotAsync(map, 42);

    expect(finalPath).toBe(path.join(dir, 'snapshot-42.bin'));
    const lines = fs.readFileSync(finalPath, 'utf8').trimEnd().split('\n');
    expect(lines).toHaveLength(ENTRY_COUNT + 1);
    expect(JSON.parse(lines[0] as string)).toMatchObject({ lastIncludedIndex: 42 });
    expect(JSON.parse(lines[1] as string)).toEqual({ key: 'metric:0', value: map.get('metric:0') });
    expect(JSON.parse(lines[ENTRY_COUNT] as string)).toEqual({
      key: `metric:${ENTRY_COUNT - 1}`,
      value: map.get(`metric:${ENTRY_COUNT - 1}`),
    });
    expect(fs.readdirSync(dir)).toEqual(['snapshot-42.bin']);
  });

  it('lets timers run while a large snapshot is being written', async () => {
    const dir = freshDir();
    const manager = new SnapshotManager({ dataDir: dir });
    const map = bigMap(ENTRY_COUNT);

    const ticks = await timerTicksDuring(() => manager.createSnapshotAsync(map, 7));
    expect(ticks).toBeGreaterThan(0);
  });

  it('control: the blocking writer gives timers no chance to run, so the check above is meaningful', async () => {
    const dir = freshDir();
    const map = bigMap(ENTRY_COUNT);

    const ticks = await timerTicksDuring(() => writeSnapshotBlocking(dir, map, 8));
    expect(ticks).toBe(0);
  });

  it('removes the temporary file and rethrows when writing fails partway', async () => {
    const dir = freshDir();
    const broken = {
      entries: () => {
        throw new Error('disk full');
      },
    } as unknown as Map<string, string>;

    await expect(new SnapshotManager({ dataDir: dir }).createSnapshotAsync(broken, 9)).rejects.toThrow('disk full');
    expect(fs.readdirSync(dir)).toEqual([]);
  });
});
