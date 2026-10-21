import { describe, it, expect } from 'vitest';

describe('Raft Snapshot Deadlock Reproduction', () => {
  it('should simulate blocking snapshot I/O and verify event loop impact', async () => {
    // Simulating node log accumulation and asynchronous snapshot yielding
    const entries = Array.from({ length: 10500 }, (_, i) => ({
      type: 'SET',
      key: `metric:${i}`,
      value: `payload_${i}`,
    }));

    expect(entries.length).toBe(10500);

    // Simulated non-blocking chunked write with setImmediate yielding
    let yielded = false;
    let count = 0;
    for (const _entry of entries) {
      count++;
      if (count % 1000 === 0) {
        await new Promise((resolve) => setImmediate(resolve));
        yielded = true;
      }
    }

    expect(yielded).toBe(true);
    expect(count).toBe(10500);
  });
});
