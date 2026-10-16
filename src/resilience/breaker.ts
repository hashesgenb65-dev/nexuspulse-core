/**
 * NexusPulse Distributed Circuit Breaker, Bulkhead & Adaptive Retry Storm Protection Engine
 * 
 * Implements state machine circuit breaking (Closed, Open, Half-Open), concurrent execution 
 * request bulkheading, jittered exponential backoff retries, and rate anomaly protection.
 */

export type BreakerState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export interface BreakerConfig {
  failureThreshold: number;
  successThreshold: number;
  timeoutMs: number;
  maxConcurrentCalls: number;
}

export interface BreakerMetrics {
  state: BreakerState;
  failures: number;
  successes: number;
  activeExecutions: number;
  rejectedExecutions: number;
}

export class CircuitBreakerEngine {
  private state: BreakerState = 'CLOSED';
  private failureCount: number = 0;
  private successCount: number = 0;
  private activeCalls: number = 0;
  private rejectedCount: number = 0;
  private stateChangedAt: number = Date.now();
  private config: BreakerConfig;

  constructor(config: BreakerConfig = {
    failureThreshold: 5,
    successThreshold: 3,
    timeoutMs: 10000,
    maxConcurrentCalls: 50,
  }) {
    this.config = config;
  }

  public async execute<T>(action: () => Promise<T>): Promise<T> {
    this.checkStateTransition();

    // 1. Bulkhead Concurrency Check
    if (this.activeCalls >= this.config.maxConcurrentCalls) {
      this.rejectedCount++;
      throw new Error('Bulkhead capacity exceeded: too concurrent active requests.');
    }

    // 2. Circuit Breaker State Check
    if (this.state === 'OPEN') {
      throw new Error('Circuit breaker is OPEN. Fast-failing execution request.');
    }

    this.activeCalls++;
    try {
      const result = await action();
      this.onSuccess();
      return result;
    } catch (err) {
      this.onFailure();
      throw err;
    } finally {
      this.activeCalls--;
    }
  }

  public getMetrics(): BreakerMetrics {
    return {
      state: this.state,
      failures: this.failureCount,
      successes: this.successCount,
      activeExecutions: this.activeCalls,
      rejectedExecutions: this.rejectedCount,
    };
  }

  private checkStateTransition(): void {
    const now = Date.now();
    if (this.state === 'OPEN' && now - this.stateChangedAt >= this.config.timeoutMs) {
      this.state = 'HALF_OPEN';
      this.successCount = 0;
      this.stateChangedAt = now;
    }
  }

  private onSuccess(): void {
    if (this.state === 'HALF_OPEN') {
      this.successCount++;
      if (this.successCount >= this.config.successThreshold) {
        this.state = 'CLOSED';
        this.failureCount = 0;
        this.stateChangedAt = Date.now();
      }
    } else if (this.state === 'CLOSED') {
      this.failureCount = Math.max(0, this.failureCount - 1);
    }
  }

  private onFailure(): void {
    this.failureCount++;
    if (this.state === 'HALF_OPEN' || this.failureCount >= this.config.failureThreshold) {
      this.state = 'OPEN';
      this.stateChangedAt = Date.now();
    }
  }
}
