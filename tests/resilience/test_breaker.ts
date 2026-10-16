import { describe, it, expect, beforeEach } from 'vitest';
import { CircuitBreakerEngine } from '../../src/resilience/breaker';

describe('CircuitBreakerEngine Module', () => {
  let breaker: CircuitBreakerEngine;

  beforeEach(() => {
    breaker = new CircuitBreakerEngine({
      failureThreshold: 2,
      successThreshold: 2,
      timeoutMs: 50,
      maxConcurrentCalls: 5,
    });
  });

  it('should execute successfully while in CLOSED state', async () => {
    const res = await breaker.execute(async () => 'success_payload');
    expect(res).toBe('success_payload');
    expect(breaker.getMetrics().state).toBe('CLOSED');
  });

  it('should trip to OPEN state upon reaching failure threshold', async () => {
    const failingAction = async () => {
      throw new Error('Downstream failure');
    };

    await expect(breaker.execute(failingAction)).rejects.toThrow();
    await expect(breaker.execute(failingAction)).rejects.toThrow();

    const metrics = breaker.getMetrics();
    expect(metrics.state).toBe('OPEN');

    // Subsequent call should fast-fail
    await expect(breaker.execute(async () => 'ok')).rejects.toThrow(/Circuit breaker is OPEN/);
  });
});
