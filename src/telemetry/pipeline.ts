/**
 * NexusPulse Real-Time Telemetry Stream Ingestion & Sliding Window Anomaly Detection Pipeline
 * 
 * Implements asynchronous stream buffering, ring-buffer sliding window statistical analytics,
 * dynamic Z-score and moving-average anomaly detection, dead-letter queue (DLQ) backpressure management,
 * and automated alert dispatching.
 */

export interface TelemetryEvent {
  eventId: string;
  sourceId: string;
  metricKey: string;
  numericValue: number;
  metadata: Record<string, string | number | boolean>;
  timestamp: number;
}

export interface SlidingWindowConfig {
  windowDurationMs: number;
  maxBufferCapacity: number;
  zScoreAnomalyThreshold: number;
  minimumSampleSize: number;
}

export interface StatisticalSummary {
  metricKey: string;
  sampleCount: number;
  mean: number;
  standardDeviation: number;
  min: number;
  max: number;
  median: number;
  p95: number;
  p99: number;
}

export interface AnomalyAlert {
  alertId: string;
  sourceId: string;
  metricKey: string;
  observedValue: number;
  expectedMean: number;
  zScore: number;
  severity: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
  timestamp: number;
}

export type AlertSubscriber = (alert: AnomalyAlert) => Promise<void> | void;

export class TelemetryPipelineEngine {
  // SourceId -> MetricKey -> Ring buffer of events
  private streamBuffers: Map<string, Map<string, TelemetryEvent[]>> = new Map();
  private deadLetterQueue: TelemetryEvent[] = [];
  private alertSubscribers: AlertSubscriber[] = [];
  private config: SlidingWindowConfig;
  private totalIngestedCount: number = 0;
  private totalDroppedCount: number = 0;

  constructor(config: SlidingWindowConfig = {
    windowDurationMs: 60 * 1000, // 1 minute sliding window
    maxBufferCapacity: 10000,
    zScoreAnomalyThreshold: 3.0,  // 3 standard deviations
    minimumSampleSize: 15,
  }) {
    this.config = config;
  }

  /**
   * Register an async event handler for real-time anomaly alerts.
   */
  public subscribeAlerts(subscriber: AlertSubscriber): void {
    this.alertSubscribers.push(subscriber);
  }

  /**
   * Ingest a single telemetry event into the sliding window buffer.
   * Triggers real-time statistical evaluation and anomaly detection.
   */
  public async ingestEvent(event: TelemetryEvent): Promise<{ isAnomaly: boolean; summary: StatisticalSummary }> {
    this.pruneExpiredEvents(event.timestamp);

    let sourceMap = this.streamBuffers.get(event.sourceId);
    if (!sourceMap) {
      sourceMap = new Map();
      this.streamBuffers.set(event.sourceId, sourceMap);
    }

    let buffer = sourceMap.get(event.metricKey);
    if (!buffer) {
      buffer = [];
      sourceMap.set(event.metricKey, buffer);
    }

    // Handle backpressure capacity limits
    if (buffer.length >= this.config.maxBufferCapacity) {
      this.totalDroppedCount++;
      this.deadLetterQueue.push(event);
      throw new Error(`Stream buffer capacity exceeded for source '${event.sourceId}', metric '${event.metricKey}'. Event routed to DLQ.`);
    }

    buffer.push(event);
    this.totalIngestedCount++;

    // Compute sliding window statistics
    const summary = this.computeStatistics(event.sourceId, event.metricKey);
    const isAnomaly = this.evaluateAnomaly(event, summary);

    if (isAnomaly) {
      await this.dispatchAnomalyAlert(event, summary);
    }

    return { isAnomaly, summary };
  }

  /**
   * Ingest a batch of events asynchronously with performance chunking.
   */
  public async ingestBatch(events: TelemetryEvent[]): Promise<{ processed: number; anomaliesDetected: number; dlqRouted: number }> {
    let processed = 0;
    let anomaliesDetected = 0;
    let dlqRouted = 0;

    for (const event of events) {
      try {
        const result = await this.ingestEvent(event);
        processed++;
        if (result.isAnomaly) {
          anomaliesDetected++;
        }
      } catch {
        dlqRouted++;
      }
    }

    return { processed, anomaliesDetected, dlqRouted };
  }

  /**
   * Compute statistical distribution for a metric stream within the current sliding window.
   */
  public computeStatistics(sourceId: string, metricKey: string): StatisticalSummary {
    const buffer = this.streamBuffers.get(sourceId)?.get(metricKey) || [];
    const count = buffer.length;

    if (count === 0) {
      return {
        metricKey,
        sampleCount: 0,
        mean: 0,
        standardDeviation: 0,
        min: 0,
        max: 0,
        median: 0,
        p95: 0,
        p99: 0,
      };
    }

    const values = buffer.map((e) => e.numericValue).sort((a, b) => a - b);
    const sum = values.reduce((acc, v) => acc + v, 0);
    const mean = sum / count;

    const variance = values.reduce((acc, v) => acc + Math.pow(v - mean, 2), 0) / count;
    const standardDeviation = Math.sqrt(variance);

    const min = values[0];
    const max = values[count - 1];
    if (min === undefined || max === undefined) throw new Error('statistics buffer is unexpectedly empty');
    const median = this.calculatePercentile(values, 50);
    const p95 = this.calculatePercentile(values, 95);
    const p99 = this.calculatePercentile(values, 99);

    return {
      metricKey,
      sampleCount: count,
      mean: Number(mean.toFixed(4)),
      standardDeviation: Number(standardDeviation.toFixed(4)),
      min,
      max,
      median,
      p95,
      p99,
    };
  }

  /**
   * Retrieve current dead-letter queue contents.
   */
  public getDeadLetterQueue(): TelemetryEvent[] {
    return [...this.deadLetterQueue];
  }

  /**
   * Clear pipeline buffer and reset metrics.
   */
  public resetPipeline(): void {
    this.streamBuffers.clear();
    this.deadLetterQueue = [];
    this.totalIngestedCount = 0;
    this.totalDroppedCount = 0;
  }

  public getPipelineMetrics(): { totalIngested: number; totalDropped: number; activeSources: number } {
    return {
      totalIngested: this.totalIngestedCount,
      totalDropped: this.totalDroppedCount,
      activeSources: this.streamBuffers.size,
    };
  }

  private pruneExpiredEvents(currentTime: number): void {
    const minTimestamp = currentTime - this.config.windowDurationMs;

    for (const [, sourceMap] of this.streamBuffers.entries()) {
      for (const [mKey, buffer] of sourceMap.entries()) {
        const filtered = buffer.filter((e) => e.timestamp >= minTimestamp);
        sourceMap.set(mKey, filtered);
      }
    }
  }

  private evaluateAnomaly(event: TelemetryEvent, summary: StatisticalSummary): boolean {
    if (summary.sampleCount < this.config.minimumSampleSize) {
      return false; // Insufficient statistical sample size
    }

    if (summary.standardDeviation === 0) {
      return event.numericValue !== summary.mean;
    }

    const zScore = Math.abs((event.numericValue - summary.mean) / summary.standardDeviation);
    return zScore >= this.config.zScoreAnomalyThreshold;
  }

  private async dispatchAnomalyAlert(event: TelemetryEvent, summary: StatisticalSummary): Promise<void> {
    const zScore = Number(Math.abs((event.numericValue - summary.mean) / (summary.standardDeviation || 1)).toFixed(2));
    
    let severity: AnomalyAlert['severity'] = 'LOW';
    if (zScore >= 5.0) severity = 'CRITICAL';
    else if (zScore >= 4.0) severity = 'HIGH';
    else if (zScore >= 3.5) severity = 'MEDIUM';

    const alert: AnomalyAlert = {
      alertId: `alt_${Math.random().toString(36).substring(2, 9)}`,
      sourceId: event.sourceId,
      metricKey: event.metricKey,
      observedValue: event.numericValue,
      expectedMean: summary.mean,
      zScore,
      severity,
      timestamp: Date.now(),
    };

    for (const subscriber of this.alertSubscribers) {
      try {
        await subscriber(alert);
      } catch {
        // Suppress subscriber callback errors
      }
    }
  }

  private calculatePercentile(sortedValues: number[], percentile: number): number {
    if (sortedValues.length === 0) return 0;
    const index = (percentile / 100) * (sortedValues.length - 1);
    const lower = Math.floor(index);
    const upper = Math.ceil(index);
    const lowValue = sortedValues[lower];
    const highValue = sortedValues[upper];
    if (lowValue === undefined || highValue === undefined) {
      throw new RangeError('percentile index is outside the sorted values');
    }
    if (lower === upper) {
      return lowValue;
    }
    const weight = index - lower;
    return lowValue * (1 - weight) + highValue * weight;
  }
}
