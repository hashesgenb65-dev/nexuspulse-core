/**
 * Event-loop lag monitor.
 *
 * A stalled event loop delays every timer and every I/O callback, including the heartbeats that keep
 * a Raft leader in charge. This module samples the loop's delay with `perf_hooks` and reports the
 * median, 99th percentile and maximum delay in milliseconds. It is the signal that would have shown
 * the stall behind INC-2026-0814 before quorum was affected.
 */

import { monitorEventLoopDelay, type IntervalHistogram } from 'node:perf_hooks';

export const DEFAULT_SAMPLE_RESOLUTION_MS = 10;
const NS_PER_MS = 1_000_000;

export interface EventLoopLagSnapshot {
  /** Number of delay samples since the monitor was started or last reset. */
  readonly samples: number;
  readonly p50Ms: number;
  readonly p99Ms: number;
  readonly maxMs: number;
}

export interface EventLoopMonitorOptions {
  /** How often the loop delay is sampled, in milliseconds. Default 10. */
  readonly resolutionMs?: number;
}

/** Samples event-loop delay. Call start() once, stop() when done, and snapshot() to read. */
export class EventLoopMonitor {
  private readonly histogram: IntervalHistogram;
  private running = false;

  constructor(options: EventLoopMonitorOptions = {}) {
    const resolutionMs = options.resolutionMs ?? DEFAULT_SAMPLE_RESOLUTION_MS;
    if (!Number.isInteger(resolutionMs) || resolutionMs < 1 || resolutionMs > 1_000) {
      throw new RangeError('resolutionMs must be an integer from 1 to 1000');
    }
    this.histogram = monitorEventLoopDelay({ resolution: resolutionMs });
  }

  get isRunning(): boolean {
    return this.running;
  }

  start(): void {
    if (this.running) return;
    this.histogram.enable();
    this.running = true;
  }

  stop(): void {
    if (!this.running) return;
    this.histogram.disable();
    this.running = false;
  }

  /** Current delay figures. Percentiles are 0 when there are no samples yet. */
  snapshot(): EventLoopLagSnapshot {
    const samples = this.histogram.count;
    if (samples === 0) return { samples: 0, p50Ms: 0, p99Ms: 0, maxMs: 0 };
    return {
      samples,
      p50Ms: this.histogram.percentile(50) / NS_PER_MS,
      p99Ms: this.histogram.percentile(99) / NS_PER_MS,
      maxMs: this.histogram.max / NS_PER_MS,
    };
  }

  /** True when the worst delay since the last reset is above `thresholdMs`. */
  exceeded(thresholdMs: number): boolean {
    return this.snapshot().maxMs > thresholdMs;
  }

  /** Clears the samples, so the next snapshot covers only what happens after this call. */
  reset(): void {
    this.histogram.reset();
  }
}
