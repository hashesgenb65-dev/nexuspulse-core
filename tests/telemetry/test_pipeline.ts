/**
 * Tests for the telemetry pipeline: sliding-window statistics, anomaly flags,
 * window pruning, dead-letter routing, alert subscribers and batch ingest.
 */

import { describe, it, expect } from 'vitest';
import {
  TelemetryPipelineEngine,
  type AnomalyAlert,
  type SlidingWindowConfig,
  type TelemetryEvent,
} from '../../src/telemetry/pipeline';

const WINDOW_MS = 60_000;

function makePipeline(overrides: Partial<SlidingWindowConfig> = {}): TelemetryPipelineEngine {
  return new TelemetryPipelineEngine({
    windowDurationMs: WINDOW_MS,
    maxBufferCapacity: 10_000,
    zScoreAnomalyThreshold: 3.0,
    minimumSampleSize: 15,
    ...overrides,
  });
}

function ev(
  eventId: string,
  numericValue: number,
  timestamp: number,
  sourceId = 'edge-01',
  metricKey = 'cpu',
): TelemetryEvent {
  return { eventId, sourceId, metricKey, numericValue, metadata: {}, timestamp };
}

/** Feeds `count` events alternating between 9 and 11, so the mean is 10 and the deviation about 1. */
async function feedAlternating(pipeline: TelemetryPipelineEngine, count: number, sourceId = 'edge-01'): Promise<void> {
  for (let i = 0; i < count; i++) {
    await pipeline.ingestEvent(ev(`alt-${sourceId}-${i}`, i % 2 === 0 ? 9 : 11, i * 10, sourceId));
  }
}

describe('statistics for one window', () => {
  it('reports a single sample as its own mean, min, max and median', async () => {
    const pipeline = makePipeline();
    const { summary } = await pipeline.ingestEvent(ev('a', 5, 1_000));
    expect(summary.sampleCount).toBe(1);
    expect(summary.mean).toBe(5);
    expect(summary.standardDeviation).toBe(0);
    expect(summary.min).toBe(5);
    expect(summary.max).toBe(5);
    expect(summary.median).toBe(5);
  });

  it('computes mean, population deviation and extremes', async () => {
    const pipeline = makePipeline();
    for (const [i, value] of [1, 2, 3, 4, 5].entries()) {
      await pipeline.ingestEvent(ev(`v${i}`, value, 1_000 + i));
    }
    const summary = pipeline.computeStatistics('edge-01', 'cpu');
    expect(summary.sampleCount).toBe(5);
    expect(summary.mean).toBe(3);
    expect(summary.standardDeviation).toBeCloseTo(Math.SQRT2, 4);
    expect(summary.min).toBe(1);
    expect(summary.max).toBe(5);
    expect(summary.median).toBe(3);
  });

  it('interpolates the 95th and 99th percentiles between neighbouring samples', async () => {
    const pipeline = makePipeline();
    for (const [i, value] of [1, 2, 3, 4, 5].entries()) {
      await pipeline.ingestEvent(ev(`p${i}`, value, 1_000 + i));
    }
    const summary = pipeline.computeStatistics('edge-01', 'cpu');
    expect(summary.p95).toBeCloseTo(4.8, 6);
    expect(summary.p99).toBeCloseTo(4.96, 6);
  });

  it('returns zeros for a metric that has never been seen', () => {
    const summary = makePipeline().computeStatistics('nobody', 'nothing');
    expect(summary).toEqual({
      metricKey: 'nothing',
      sampleCount: 0,
      mean: 0,
      standardDeviation: 0,
      min: 0,
      max: 0,
      median: 0,
      p95: 0,
      p99: 0,
    });
  });

  it('keeps sources separate', async () => {
    const pipeline = makePipeline();
    await pipeline.ingestEvent(ev('a1', 10, 1_000, 'edge-a'));
    await pipeline.ingestEvent(ev('b1', 100, 1_000, 'edge-b'));
    expect(pipeline.computeStatistics('edge-a', 'cpu').mean).toBe(10);
    expect(pipeline.computeStatistics('edge-b', 'cpu').mean).toBe(100);
  });
});

describe('anomaly detection', () => {
  it('does not evaluate anomalies until the minimum sample size is reached', async () => {
    const pipeline = makePipeline();
    await feedAlternating(pipeline, 13);
    // 13 earlier events plus this one is 14 samples, one short of the minimum of 15.
    const result = await pipeline.ingestEvent(ev('early', 1_000, 500));
    expect(result.summary.sampleCount).toBe(14);
    expect(result.isAnomaly).toBe(false);
  });

  it('flags a large outlier once the window has enough samples', async () => {
    const pipeline = makePipeline();
    await feedAlternating(pipeline, 20);
    const result = await pipeline.ingestEvent(ev('spike', 100, 300));
    expect(result.isAnomaly).toBe(true);
  });

  it('does not flag a value close to the mean', async () => {
    const pipeline = makePipeline();
    await feedAlternating(pipeline, 20);
    const result = await pipeline.ingestEvent(ev('normal', 10, 300));
    expect(result.isAnomaly).toBe(false);
  });

  it('does not flag a repeat of a constant window', async () => {
    const pipeline = makePipeline();
    for (let i = 0; i < 15; i++) await pipeline.ingestEvent(ev(`c${i}`, 5, i * 10));
    const result = await pipeline.ingestEvent(ev('same', 5, 200));
    expect(result.summary.standardDeviation).toBe(0);
    expect(result.isAnomaly).toBe(false);
  });

  it('flags a change after a constant window', async () => {
    const pipeline = makePipeline();
    for (let i = 0; i < 15; i++) await pipeline.ingestEvent(ev(`c${i}`, 5, i * 10));
    // The new value is included in the window before scoring, so the deviation is non-zero
    // and its z-score is about 3.9.
    const result = await pipeline.ingestEvent(ev('changed', 6, 200));
    expect(result.isAnomaly).toBe(true);
  });
});

describe('alert subscribers', () => {
  it('delivers an anomaly alert with the event details', async () => {
    const pipeline = makePipeline();
    const alerts: AnomalyAlert[] = [];
    pipeline.subscribeAlerts((alert) => void alerts.push(alert));

    await feedAlternating(pipeline, 20);
    await pipeline.ingestEvent(ev('spike', 100, 300));

    expect(alerts).toHaveLength(1);
    const [alert] = alerts;
    expect(alert?.sourceId).toBe('edge-01');
    expect(alert?.metricKey).toBe('cpu');
    expect(alert?.observedValue).toBe(100);
    // The window includes the spike itself when the z-score is computed (about 4.47), which is HIGH.
    expect(alert?.zScore).toBeCloseTo(4.47, 2);
    expect(alert?.severity).toBe('HIGH');
  });

  it('keeps delivering to other subscribers when one throws', async () => {
    const pipeline = makePipeline();
    const received: string[] = [];
    pipeline.subscribeAlerts(() => {
      throw new Error('subscriber failure');
    });
    pipeline.subscribeAlerts((alert) => void received.push(alert.alertId));

    await feedAlternating(pipeline, 20);
    const result = await pipeline.ingestEvent(ev('spike', 100, 300));

    expect(result.isAnomaly).toBe(true);
    expect(received).toHaveLength(1);
  });

  it('waits for an async subscriber before ingest returns', async () => {
    const pipeline = makePipeline();
    let finished = false;
    pipeline.subscribeAlerts(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      finished = true;
    });

    await feedAlternating(pipeline, 20);
    await pipeline.ingestEvent(ev('spike', 100, 300));
    expect(finished).toBe(true);
  });
});

describe('window pruning', () => {
  it('drops events older than the window', async () => {
    const pipeline = makePipeline();
    await pipeline.ingestEvent(ev('old', 1, 0));
    const result = await pipeline.ingestEvent(ev('new', 2, WINDOW_MS + 1));
    expect(result.summary.sampleCount).toBe(1);
    expect(result.summary.mean).toBe(2);
  });

  it('keeps events that are exactly on the window edge', async () => {
    const pipeline = makePipeline();
    await pipeline.ingestEvent(ev('edge', 1, 1_000));
    const result = await pipeline.ingestEvent(ev('now', 2, 1_000 + WINDOW_MS));
    expect(result.summary.sampleCount).toBe(2);
  });
});

describe('capacity and the dead-letter queue', () => {
  it('routes an event beyond buffer capacity to the dead-letter queue and throws', async () => {
    const pipeline = makePipeline({ maxBufferCapacity: 2 });
    await pipeline.ingestEvent(ev('a', 1, 1_000));
    await pipeline.ingestEvent(ev('b', 2, 1_001));

    await expect(pipeline.ingestEvent(ev('c', 3, 1_002))).rejects.toThrowError(/capacity exceeded/);

    expect(pipeline.getDeadLetterQueue().map((e) => e.eventId)).toEqual(['c']);
    expect(pipeline.getPipelineMetrics()).toEqual({ totalIngested: 2, totalDropped: 1, activeSources: 1 });
  });

  it('returns a copy of the dead-letter queue', async () => {
    const pipeline = makePipeline({ maxBufferCapacity: 1 });
    await pipeline.ingestEvent(ev('a', 1, 1_000));
    await expect(pipeline.ingestEvent(ev('b', 2, 1_001))).rejects.toThrow();

    pipeline.getDeadLetterQueue().length = 0;
    expect(pipeline.getDeadLetterQueue()).toHaveLength(1);
  });

  it('resetPipeline clears buffers, the dead-letter queue and the counters', async () => {
    const pipeline = makePipeline({ maxBufferCapacity: 1 });
    await pipeline.ingestEvent(ev('a', 1, 1_000));
    await expect(pipeline.ingestEvent(ev('b', 2, 1_001))).rejects.toThrow();

    pipeline.resetPipeline();

    expect(pipeline.getDeadLetterQueue()).toEqual([]);
    expect(pipeline.getPipelineMetrics()).toEqual({ totalIngested: 0, totalDropped: 0, activeSources: 0 });
    expect(pipeline.computeStatistics('edge-01', 'cpu').sampleCount).toBe(0);
  });

  it('counts distinct sources in the metrics', async () => {
    const pipeline = makePipeline();
    await pipeline.ingestEvent(ev('a', 1, 1_000, 'edge-a'));
    await pipeline.ingestEvent(ev('b', 1, 1_000, 'edge-b'));
    await pipeline.ingestEvent(ev('c', 1, 1_000, 'edge-b', 'mem'));
    expect(pipeline.getPipelineMetrics()).toEqual({ totalIngested: 3, totalDropped: 0, activeSources: 2 });
  });
});

describe('batch ingest', () => {
  it('reports processed, anomalous and dead-lettered events', async () => {
    const pipeline = makePipeline({ maxBufferCapacity: 2 });
    const result = await pipeline.ingestBatch([ev('a', 1, 1_000), ev('b', 2, 1_001), ev('c', 3, 1_002)]);
    expect(result).toEqual({ processed: 2, anomaliesDetected: 0, dlqRouted: 1 });
  });

  it('counts anomalies inside a batch', async () => {
    const pipeline = makePipeline();
    const batch: TelemetryEvent[] = [];
    for (let i = 0; i < 20; i++) batch.push(ev(`alt-${i}`, i % 2 === 0 ? 9 : 11, i * 10));
    batch.push(ev('spike', 100, 300));

    const result = await pipeline.ingestBatch(batch);
    expect(result).toEqual({ processed: 21, anomaliesDetected: 1, dlqRouted: 0 });
  });

  it('handles an empty batch', async () => {
    const result = await makePipeline().ingestBatch([]);
    expect(result).toEqual({ processed: 0, anomaliesDetected: 0, dlqRouted: 0 });
  });
});
