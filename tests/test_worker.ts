import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { WorkerQueueEngine } from '../src/queue/worker';

describe('WorkerQueueEngine Module', () => {
  let workerEngine: WorkerQueueEngine;

  beforeEach(() => {
    workerEngine = new WorkerQueueEngine({
      maxConcurrentWorkers: 2,
      defaultMaxRetries: 2,
      baseBackoffMs: 10, // fast retry for unit tests
      circuitBreakerFailureThreshold: 5,
    });
    workerEngine.start();
  });

  afterEach(() => {
    workerEngine.stop();
  });

  it('should register handlers and successfully process enqueued tasks', async () => {
    let executedValue = 0;

    workerEngine.registerHandler('add_task', async (task: any) => {
      executedValue += task.payload.amount;
      return true;
    });

    workerEngine.enqueue('add_task', { amount: 15 }, 'NORMAL');
    workerEngine.enqueue('add_task', { amount: 25 }, 'HIGH');

    // Wait for async workers to complete
    await new Promise((resolve) => setTimeout(resolve, 100));

    const metrics = workerEngine.getMetrics();
    expect(metrics.completed).toBe(2);
    expect(executedValue).toBe(40);
  });

  it('should prioritize CRITICAL tasks ahead of LOW priority tasks', async () => {
    const executionOrder: string[] = [];

    workerEngine.registerHandler('order_task', async (task: any) => {
      executionOrder.push(task.payload.id);
      return true;
    });

    // Enqueue Low first, then Critical
    workerEngine.enqueue('order_task', { id: 'low_task' }, 'LOW');
    workerEngine.enqueue('order_task', { id: 'critical_task' }, 'CRITICAL');

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(executionOrder[0]).toBe('critical_task');
  });

  it('should handle failures, apply exponential retries, and route to DLQ on max retries', async () => {
    let attempts = 0;

    workerEngine.registerHandler('failing_task', async () => {
      attempts++;
      throw new Error('Simulated worker crash');
    });

    workerEngine.enqueue('failing_task', {}, 'NORMAL', 1); // Max 1 retry

    await new Promise((resolve) => setTimeout(resolve, 200));

    const metrics = workerEngine.getMetrics();
    expect(metrics.deadLetter).toBe(1);
    expect(attempts).toBe(2); // Initial attempt + 1 retry
  });
});
