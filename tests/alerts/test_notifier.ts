/**
 * Tests for the alert notifier: ordering, dedup, backpressure, retries,
 * dead letters, timeouts, shutdown, and the sink helpers.
 */

import { describe, it, expect } from 'vitest';
import {
  AlertError,
  AlertNotifier,
  callbackSink,
  minSeverityFilter,
  NotifierConfigError,
  webhookSink,
  type Alert,
  type AlertSink,
  type FetchLike,
} from '../../src/alerts/notifier.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function alert(key: string, severity: Alert['severity'] = 'warning', summary = `alert ${key}`): Alert {
  return { key, severity, summary, firedAt: 1_000, labels: { source: 'edge-01' } };
}

/** A controllable clock. */
function fakeClock(start = 10_000): { now: () => number; advance: (ms: number) => void } {
  let t = start;
  return { now: () => t, advance: (ms: number) => void (t += ms) };
}

/** Sleeps are recorded, never waited on. */
function recordingSleep(): { sleep: (ms: number) => Promise<void>; waits: number[] } {
  const waits: number[] = [];
  return {
    waits,
    sleep: (ms: number): Promise<void> => {
      waits.push(ms);
      return Promise.resolve();
    },
  };
}

/** A sink that records every alert it receives. */
function recordingSink(name: string): AlertSink & { received: Alert[] } {
  const received: Alert[] = [];
  return {
    name,
    received,
    send(a: Alert): void {
      received.push(a);
    },
  };
}

/** A sink that fails the first `failures` sends, then succeeds. */
function flakySink(name: string, failures: number): AlertSink & { calls: number } {
  const sink = {
    name,
    calls: 0,
    send(): void {
      sink.calls++;
      if (sink.calls <= failures) throw new Error(`boom ${sink.calls}`);
    },
  };
  return sink;
}

function errorOf(fn: () => unknown): Error {
  try {
    fn();
  } catch (err) {
    if (err instanceof Error) return err;
    throw err;
  }
  throw new Error('expected an error');
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

describe('AlertNotifier configuration', () => {
  it('requires at least one sink', () => {
    const err = errorOf(() => new AlertNotifier({ sinks: [] }));
    expect(err).toBeInstanceOf(NotifierConfigError);
    expect((err as NotifierConfigError).issues).toEqual(['at least one sink is required']);
  });

  it('rejects duplicate and unnamed sinks', () => {
    const err = errorOf(() =>
      new AlertNotifier({
        sinks: [recordingSink('a'), recordingSink('a'), { name: '', send: () => undefined }],
      }),
    ) as NotifierConfigError;
    expect(err.issues).toContain('sink name a is used more than once');
    expect(err.issues).toContain('every sink needs a non-empty name');
  });

  it('rejects out-of-range numbers and reports them together', () => {
    const err = errorOf(
      () => new AlertNotifier({ sinks: [recordingSink('a')], queueCapacity: 0, maxAttempts: 11, attemptTimeoutMs: 0 }),
    ) as NotifierConfigError;
    expect(err.issues).toHaveLength(3);
    expect(err.code).toBe('NOTIFIER_CONFIG');
  });

  it('requires backoffMaxMs to be at least backoffBaseMs', () => {
    const err = errorOf(
      () => new AlertNotifier({ sinks: [recordingSink('a')], backoffBaseMs: 1000, backoffMaxMs: 500 }),
    ) as NotifierConfigError;
    expect(err.issues).toContain('backoffMaxMs must be at least backoffBaseMs');
  });

  it('rejects non-integer numbers', () => {
    const err = errorOf(() => new AlertNotifier({ sinks: [recordingSink('a')], queueCapacity: 2.5 })) as NotifierConfigError;
    expect(err.issues[0]).toContain('queueCapacity must be an integer');
  });
});

// ---------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------

describe('AlertNotifier delivery', () => {
  it('queues an alert and delivers it to every sink', async () => {
    const a = recordingSink('a');
    const b = recordingSink('b');
    const notifier = new AlertNotifier({ sinks: [a, b] });
    expect(notifier.notify(alert('k1'))).toBe('queued');
    await notifier.flush();
    expect(a.received.map((x) => x.key)).toEqual(['k1']);
    expect(b.received.map((x) => x.key)).toEqual(['k1']);
  });

  it('delivers alerts in the order they were queued', async () => {
    const sink = recordingSink('s');
    const notifier = new AlertNotifier({ sinks: [sink], dedupWindowMs: 0 });
    for (const key of ['one', 'two', 'three']) notifier.notify(alert(key));
    await notifier.flush();
    expect(sink.received.map((x) => x.key)).toEqual(['one', 'two', 'three']);
  });

  it('passes the alert through unchanged', async () => {
    const sink = recordingSink('s');
    const notifier = new AlertNotifier({ sinks: [sink] });
    const original = alert('exact', 'critical', 'disk full');
    notifier.notify(original);
    await notifier.flush();
    expect(sink.received[0]).toEqual(original);
  });

  it('counts processed alerts and deliveries', async () => {
    const notifier = new AlertNotifier({ sinks: [recordingSink('a'), recordingSink('b')] });
    notifier.notify(alert('x'));
    await notifier.flush();
    const stats = notifier.stats();
    expect(stats.processed).toBe(1);
    expect(stats.deliveries).toBe(2);
    expect(stats.pending).toBe(0);
    expect(stats.closed).toBe(false);
  });

  it('keeps delivering to other sinks when one sink throws synchronously', async () => {
    const bad = { name: 'bad', send(): never { throw new Error('sync failure'); } };
    const good = recordingSink('good');
    const { sleep } = recordingSleep();
    const notifier = new AlertNotifier({ sinks: [bad, good], maxAttempts: 1, sleep });
    notifier.notify(alert('x'));
    await notifier.flush();
    expect(good.received).toHaveLength(1);
    expect(notifier.deadLetterList()[0]?.error).toBe('sync failure');
  });

  it('flush resolves immediately when nothing is queued', async () => {
    const notifier = new AlertNotifier({ sinks: [recordingSink('a')] });
    await expect(notifier.flush()).resolves.toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Dedup
// ---------------------------------------------------------------------------

describe('AlertNotifier dedup', () => {
  it('suppresses a repeat of the same key within the window', async () => {
    const clock = fakeClock();
    const sink = recordingSink('s');
    const notifier = new AlertNotifier({ sinks: [sink], clock: clock.now, dedupWindowMs: 60_000 });
    expect(notifier.notify(alert('cpu'))).toBe('queued');
    expect(notifier.notify(alert('cpu'))).toBe('deduplicated');
    await notifier.flush();
    expect(sink.received).toHaveLength(1);
    expect(notifier.stats().deduplicated).toBe(1);
  });

  it('allows the key again once the window has passed', async () => {
    const clock = fakeClock();
    const sink = recordingSink('s');
    const notifier = new AlertNotifier({ sinks: [sink], clock: clock.now, dedupWindowMs: 1_000 });
    notifier.notify(alert('cpu'));
    clock.advance(1_000);
    expect(notifier.notify(alert('cpu'))).toBe('queued');
    await notifier.flush();
    expect(sink.received).toHaveLength(2);
  });

  it('treats different keys independently', () => {
    const clock = fakeClock();
    const notifier = new AlertNotifier({ sinks: [recordingSink('s')], clock: clock.now });
    expect(notifier.notify(alert('a'))).toBe('queued');
    expect(notifier.notify(alert('b'))).toBe('queued');
  });

  it('does not dedupe when the window is zero', async () => {
    const clock = fakeClock();
    const sink = recordingSink('s');
    const notifier = new AlertNotifier({ sinks: [sink], clock: clock.now, dedupWindowMs: 0 });
    notifier.notify(alert('x'));
    notifier.notify(alert('x'));
    await notifier.flush();
    expect(sink.received).toHaveLength(2);
  });

  it('bounds the dedup table under many distinct keys', async () => {
    const clock = fakeClock();
    const notifier = new AlertNotifier({
      sinks: [recordingSink('s')],
      clock: clock.now,
      dedupWindowMs: 86_400_000,
      queueCapacity: 100_000,
    });
    for (let i = 0; i < 10_050; i++) {
      notifier.notify(alert(`key-${i}`));
    }
    // Keys inside the window are not evicted by age, so the cap evicts the oldest ones.
    // A key evicted this way is accepted again, which is the documented trade-off.
    expect(notifier.notify(alert('key-0'))).toBe('queued');
    await notifier.flush();
  });
});

// ---------------------------------------------------------------------------
// Backpressure
// ---------------------------------------------------------------------------

describe('AlertNotifier backpressure', () => {
  it('drops alerts when the queue is full, without rejecting', async () => {
    // A sink that holds the first delivery open so later alerts stay queued.
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const blocking: AlertSink = { name: 'slow', send: () => gate };
    const notifier = new AlertNotifier({ sinks: [blocking], queueCapacity: 2, dedupWindowMs: 0 });

    expect(notifier.notify(alert('a'))).toBe('queued'); // taken by the loop right away
    expect(notifier.notify(alert('b'))).toBe('queued');
    expect(notifier.notify(alert('c'))).toBe('queued');
    expect(notifier.notify(alert('d'))).toBe('dropped');
    expect(notifier.stats().droppedQueueFull).toBe(1);

    release();
    await notifier.flush();
    expect(notifier.stats().processed).toBe(3);
  });

  it('does not record a dropped alert in the dedup table', async () => {
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const sink: AlertSink = { name: 'slow', send: () => gate };
    const clock = fakeClock();
    const notifier = new AlertNotifier({ sinks: [sink], queueCapacity: 1, dedupWindowMs: 60_000, clock: clock.now });

    notifier.notify(alert('first'));
    notifier.notify(alert('second'));
    expect(notifier.notify(alert('third'))).toBe('dropped');
    release();
    await notifier.flush();
    // 'third' was dropped, so it is not suppressed when it arrives again.
    expect(notifier.notify(alert('third'))).toBe('queued');
    await notifier.flush();
  });
});

// ---------------------------------------------------------------------------
// Retries and dead letters
// ---------------------------------------------------------------------------

describe('AlertNotifier retries', () => {
  it('retries a failing sink with exponential backoff and then succeeds', async () => {
    const sleeper = recordingSleep();
    const sink = flakySink('flaky', 2);
    const notifier = new AlertNotifier({
      sinks: [sink],
      maxAttempts: 3,
      backoffBaseMs: 500,
      backoffMaxMs: 10_000,
      sleep: sleeper.sleep,
    });
    notifier.notify(alert('x'));
    await notifier.flush();

    expect(sink.calls).toBe(3);
    expect(sleeper.waits).toEqual([500, 1000]);
    expect(notifier.stats().retries).toBe(2);
    expect(notifier.stats().deliveries).toBe(1);
    expect(notifier.stats().deadLettered).toBe(0);
  });

  it('caps the backoff delay at backoffMaxMs', () => {
    const notifier = new AlertNotifier({
      sinks: [recordingSink('a')],
      backoffBaseMs: 100,
      backoffMaxMs: 250,
    });
    expect(notifier.backoffFor(1)).toBe(100);
    expect(notifier.backoffFor(2)).toBe(200);
    expect(notifier.backoffFor(3)).toBe(250);
    expect(notifier.backoffFor(10)).toBe(250);
  });

  it('records a dead letter after the last attempt fails, and keeps other sinks working', async () => {
    const sleeper = recordingSleep();
    const broken = flakySink('broken', 99);
    const healthy = recordingSink('healthy');
    const clock = fakeClock(5_000);
    const notifier = new AlertNotifier({
      sinks: [broken, healthy],
      maxAttempts: 2,
      sleep: sleeper.sleep,
      clock: clock.now,
    });
    notifier.notify(alert('x'));
    await notifier.flush();

    expect(broken.calls).toBe(2);
    expect(healthy.received).toHaveLength(1);
    const dead = notifier.deadLetterList();
    expect(dead).toEqual([{ sink: 'broken', alertKey: 'x', attempts: 2, error: 'boom 2', at: 5_000 }]);
    expect(notifier.stats().deadLettered).toBe(1);
  });

  it('keeps only the most recent 100 dead letters', async () => {
    const notifier = new AlertNotifier({
      sinks: [flakySink('broken', 1_000)],
      maxAttempts: 1,
      dedupWindowMs: 0,
      queueCapacity: 200,
    });
    for (let i = 0; i < 105; i++) notifier.notify(alert(`k${i}`));
    await notifier.flush();
    const dead = notifier.deadLetterList();
    expect(dead).toHaveLength(100);
    expect(dead[0]?.alertKey).toBe('k5');
    expect(dead[99]?.alertKey).toBe('k104');
  });

  it('times out a send that never settles', async () => {
    const hang: AlertSink = { name: 'hang', send: () => new Promise<void>(() => undefined) };
    const notifier = new AlertNotifier({ sinks: [hang], maxAttempts: 1, attemptTimeoutMs: 10 });
    notifier.notify(alert('x'));
    await notifier.flush();
    expect(notifier.deadLetterList()[0]?.error).toBe('timed out after 10 ms');
  });

  it('does not wait after the final attempt', async () => {
    const sleeper = recordingSleep();
    const notifier = new AlertNotifier({ sinks: [flakySink('x', 9)], maxAttempts: 3, sleep: sleeper.sleep });
    notifier.notify(alert('k'));
    await notifier.flush();
    expect(sleeper.waits).toHaveLength(2);
  });
});

// ---------------------------------------------------------------------------
// Validation of alerts
// ---------------------------------------------------------------------------

describe('AlertNotifier alert validation', () => {
  const notifier = new AlertNotifier({ sinks: [recordingSink('a')] });

  it('rejects an empty or overlong key', () => {
    expect(() => notifier.notify(alert(''))).toThrow(AlertError);
    expect(() => notifier.notify(alert('k'.repeat(257)))).toThrow('alert key must be 1-256 characters');
  });

  it('rejects an unknown severity', () => {
    const bad = { ...alert('x'), severity: 'page' as never };
    expect(() => notifier.notify(bad)).toThrow('alert severity must be info, warning or critical');
  });

  it('rejects an empty or overlong summary', () => {
    expect(() => notifier.notify({ ...alert('x'), summary: '' })).toThrow(AlertError);
    expect(() => notifier.notify({ ...alert('x'), summary: 's'.repeat(1025) })).toThrow(AlertError);
  });

  it('rejects a non-finite firedAt', () => {
    expect(() => notifier.notify({ ...alert('x'), firedAt: Number.NaN })).toThrow('firedAt must be a finite number');
  });

  it('rejects non-string label values and too many labels', () => {
    expect(() => notifier.notify({ ...alert('x'), labels: { a: 1 as never } })).toThrow(
      'alert label values must be strings',
    );
    const many: Record<string, string> = {};
    for (let i = 0; i < 17; i++) many[`k${i}`] = 'v';
    expect(() => notifier.notify({ ...alert('x'), labels: many })).toThrow('at most 16 alert labels');
  });

  it('rejects a non-object alert', () => {
    expect(() => notifier.notify(null as unknown as Alert)).toThrow('alert must be an object');
  });
});

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

describe('AlertNotifier shutdown', () => {
  it('refuses new alerts after close', async () => {
    const notifier = new AlertNotifier({ sinks: [recordingSink('a')] });
    await notifier.close();
    expect(notifier.notify(alert('late'))).toBe('closed');
    expect(notifier.stats().closed).toBe(true);
  });

  it('drains queued alerts on a default close', async () => {
    const sink = recordingSink('s');
    const notifier = new AlertNotifier({ sinks: [sink], dedupWindowMs: 0 });
    notifier.notify(alert('a'));
    notifier.notify(alert('b'));
    const result = await notifier.close();
    expect(result.discarded).toBe(0);
    expect(sink.received.map((x) => x.key)).toEqual(['a', 'b']);
  });

  it('discards queued alerts on close with drain disabled', async () => {
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const sink: AlertSink & { received: Alert[] } = {
      name: 'slow',
      received: [],
      send(a: Alert): Promise<void> {
        this.received.push(a);
        return gate;
      },
    };
    const notifier = new AlertNotifier({ sinks: [sink], dedupWindowMs: 0 });
    notifier.notify(alert('in-flight'));
    notifier.notify(alert('queued-1'));
    notifier.notify(alert('queued-2'));

    const closing = notifier.close({ drain: false });
    release();
    const result = await closing;
    await notifier.flush();

    expect(result.discarded).toBe(2);
    expect(sink.received.map((x) => x.key)).toEqual(['in-flight']);
    expect(notifier.stats().pending).toBe(0);
  });

  it('can be closed twice', async () => {
    const notifier = new AlertNotifier({ sinks: [recordingSink('a')] });
    await notifier.close();
    await expect(notifier.close()).resolves.toEqual({ discarded: 0 });
  });
});

// ---------------------------------------------------------------------------
// Sink helpers
// ---------------------------------------------------------------------------

describe('minSeverityFilter', () => {
  it('delivers only alerts at or above the minimum', async () => {
    const inner = recordingSink('inner');
    const filtered = minSeverityFilter(inner, 'warning');
    const notifier = new AlertNotifier({ sinks: [filtered], dedupWindowMs: 0 });
    notifier.notify(alert('i', 'info'));
    notifier.notify(alert('w', 'warning'));
    notifier.notify(alert('c', 'critical'));
    await notifier.flush();
    expect(inner.received.map((x) => x.key)).toEqual(['w', 'c']);
  });

  it('keeps the wrapped sink name', () => {
    expect(minSeverityFilter(recordingSink('pager'), 'critical').name).toBe('pager');
  });
});

describe('callbackSink', () => {
  it('forwards each alert to the function', async () => {
    const seen: string[] = [];
    const notifier = new AlertNotifier({ sinks: [callbackSink('cb', (a) => void seen.push(a.key))] });
    notifier.notify(alert('via-callback'));
    await notifier.flush();
    expect(seen).toEqual(['via-callback']);
  });
});

describe('webhookSink', () => {
  interface Call {
    url: string;
    method: string;
    headers: Record<string, string>;
    body: string;
  }

  function fakeFetch(status: number): { fetch: FetchLike; calls: Call[] } {
    const calls: Call[] = [];
    const fetchImpl: FetchLike = (url, init) => {
      calls.push({ url, method: init.method, headers: init.headers, body: init.body });
      return Promise.resolve({ ok: status >= 200 && status < 300, status });
    };
    return { fetch: fetchImpl, calls };
  }

  it('posts the alert as JSON with the configured headers', async () => {
    const { fetch, calls } = fakeFetch(204);
    const sink = webhookSink({
      name: 'hook',
      url: 'https://hooks.example.internal/nexuspulse',
      headers: { authorization: 'Bearer test-token' },
      fetch,
    });
    await sink.send(alert('k'));
    expect(calls).toHaveLength(1);
    expect(calls[0]?.method).toBe('POST');
    expect(calls[0]?.url).toBe('https://hooks.example.internal/nexuspulse');
    expect(calls[0]?.headers).toEqual({ 'content-type': 'application/json', authorization: 'Bearer test-token' });
    expect(JSON.parse(calls[0]?.body ?? '{}')).toMatchObject({ key: 'k', severity: 'warning' });
  });

  it('throws when the endpoint responds with an error status', async () => {
    const { fetch } = fakeFetch(500);
    const sink = webhookSink({ name: 'hook', url: 'http://localhost:9999/x', fetch });
    await expect(sink.send(alert('k'))).rejects.toThrow('webhook responded with status 500');
  });

  it('rejects URLs that are not http or https', () => {
    expect(() => webhookSink({ name: 'hook', url: 'ftp://example.com/x' })).toThrow(NotifierConfigError);
    expect(() => webhookSink({ name: 'hook', url: 'not a url' })).toThrow('url is not a valid URL');
  });

  it('counts a failing webhook as a failed delivery through the notifier', async () => {
    const { fetch } = fakeFetch(503);
    const sleeper = recordingSleep();
    const notifier = new AlertNotifier({
      sinks: [webhookSink({ name: 'hook', url: 'https://example.com/h', fetch })],
      maxAttempts: 2,
      sleep: sleeper.sleep,
    });
    notifier.notify(alert('k'));
    await notifier.flush();
    expect(notifier.deadLetterList()[0]?.error).toBe('webhook responded with status 503');
    expect(sleeper.waits).toEqual([500]);
  });
});
