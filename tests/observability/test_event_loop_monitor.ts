/**
 * The event-loop lag monitor and its Prometheus output. The loop is blocked on purpose to check that
 * the monitor sees the stall. Timing thresholds are loose, so the test does not depend on machine speed.
 */

import { describe, it, expect, afterEach } from 'vitest';
import { createDefaultEngine } from '../../src/core/engine';
import { EventLoopMonitor } from '../../src/observability/event_loop_monitor';
import { nexusPulseFamilies, renderNexusPulseMetrics, type MetricsInput } from '../../src/observability/prometheus';

const monitors: EventLoopMonitor[] = [];

afterEach(() => {
  for (const monitor of monitors.splice(0)) monitor.stop();
});

function monitor(options: { resolutionMs?: number } = {}): EventLoopMonitor {
  const created = new EventLoopMonitor(options);
  monitors.push(created);
  return created;
}

function blockFor(ms: number): void {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    // Busy loop on purpose: holds the event loop, the way a synchronous write does.
  }
}

async function letLoopRun(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

const engineStats: MetricsInput['engine'] = createDefaultEngine().metrics();

describe('EventLoopMonitor', () => {
  it('reports zero figures before it is started', () => {
    expect(monitor().snapshot()).toEqual({ samples: 0, p50Ms: 0, p99Ms: 0, maxMs: 0 });
  });

  it('records samples once started', async () => {
    const lag = monitor({ resolutionMs: 5 });
    lag.start();
    await letLoopRun(60);
    lag.stop();
    expect(lag.snapshot().samples).toBeGreaterThan(0);
  });

  it('sees a blocked loop as a large maximum delay', async () => {
    const lag = monitor({ resolutionMs: 5 });
    lag.start();
    await letLoopRun(20);
    blockFor(80);
    await letLoopRun(20);
    lag.stop();

    expect(lag.snapshot().maxMs).toBeGreaterThan(50);
    expect(lag.exceeded(50)).toBe(true);
  });

  it('does not report a stall when the loop stays responsive', async () => {
    const lag = monitor({ resolutionMs: 5 });
    lag.start();
    await letLoopRun(100);
    lag.stop();

    // A quiet loop has a small maximum delay. The threshold is loose to tolerate slow CI.
    expect(lag.exceeded(500)).toBe(false);
  });

  it('forgets earlier stalls after reset', async () => {
    const lag = monitor({ resolutionMs: 5 });
    lag.start();
    await letLoopRun(20);
    blockFor(80);
    await letLoopRun(60); // give the sampler a few ticks to record the stall
    expect(lag.exceeded(50)).toBe(true);

    lag.reset();
    expect(lag.snapshot().samples).toBe(0);
    await letLoopRun(60);
    expect(lag.exceeded(50)).toBe(false);
    lag.stop();
  });

  it('can be started and stopped more than once without error', () => {
    const lag = monitor();
    lag.start();
    lag.start();
    expect(lag.isRunning).toBe(true);
    lag.stop();
    lag.stop();
    expect(lag.isRunning).toBe(false);
  });

  it('rejects a resolution outside 1 to 1000 ms', () => {
    expect(() => monitor({ resolutionMs: 0 })).toThrow(RangeError);
    expect(() => monitor({ resolutionMs: 1.5 })).toThrow(RangeError);
    expect(() => monitor({ resolutionMs: 1_001 })).toThrow(RangeError);
  });
});

describe('event-loop metrics in the exposition output', () => {
  it('adds lag gauges and a sample counter when a snapshot is given', () => {
    const families = nexusPulseFamilies({
      engine: engineStats,
      eventLoop: { samples: 42, p50Ms: 1, p99Ms: 9, maxMs: 17 },
    });
    const names = families.map((family) => family.name);
    expect(names).toContain('nexuspulse_event_loop_lag_p99_ms');
    expect(names).toContain('nexuspulse_event_loop_lag_max_ms');
    expect(names).toContain('nexuspulse_event_loop_samples_total');
  });

  it('renders the lag values as exposition text', () => {
    const text = renderNexusPulseMetrics({
      engine: engineStats,
      eventLoop: { samples: 42, p50Ms: 1, p99Ms: 9, maxMs: 17 },
    });
    expect(text).toContain('nexuspulse_event_loop_lag_max_ms 17');
    expect(text).toContain('nexuspulse_event_loop_samples_total 42');
  });

  it('omits the lag families when no snapshot is given', () => {
    const text = renderNexusPulseMetrics({ engine: engineStats });
    expect(text).not.toContain('nexuspulse_event_loop_lag');
  });
});
