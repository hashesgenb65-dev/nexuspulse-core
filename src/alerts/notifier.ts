/**
 * Alert notifier: a bounded, deduplicated delivery queue with per-sink retries.
 *
 * Flow. {@link AlertNotifier.notify} checks the dedup window for the alert key,
 * then places the alert in a bounded queue. A single delivery loop takes alerts
 * in order and sends each one to every sink. A failed send is retried with
 * exponential backoff, up to `maxAttempts`. A sink that still fails is recorded
 * as a dead letter, and the other sinks still receive the alert.
 *
 * Guarantees.
 * - At most one delivery loop runs at a time, so alerts reach sinks in order.
 * - Memory is bounded: the queue, the dedup table and the dead-letter list all have caps.
 * - Sink failures never reject `notify`; they show up in {@link NotifierStats}.
 *
 * Not guaranteed. Delivery is at-most-once per process. Queued alerts are lost on
 * a crash, and no persistence is provided.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type AlertSeverity = 'info' | 'warning' | 'critical';

const SEVERITY_RANK: Readonly<Record<AlertSeverity, number>> = Object.freeze({ info: 0, warning: 1, critical: 2 });

export interface Alert {
  /** Dedup key. Alerts with the same key within the dedup window are suppressed. */
  readonly key: string;
  readonly severity: AlertSeverity;
  readonly summary: string;
  /** Epoch milliseconds when the condition was detected. */
  readonly firedAt: number;
  readonly labels: Readonly<Record<string, string>>;
}

export interface AlertSink {
  readonly name: string;
  send(alert: Alert): Promise<void> | void;
}

export type NotifyOutcome = 'queued' | 'deduplicated' | 'dropped' | 'closed';

export interface DeadLetter {
  readonly sink: string;
  readonly alertKey: string;
  readonly attempts: number;
  readonly error: string;
  readonly at: number;
}

export interface NotifierStats {
  readonly queued: number;
  readonly processed: number;
  readonly deliveries: number;
  readonly retries: number;
  readonly deadLettered: number;
  readonly deduplicated: number;
  readonly droppedQueueFull: number;
  readonly pending: number;
  readonly closed: boolean;
}

export interface NotifierOptions {
  readonly sinks: readonly AlertSink[];
  /** Maximum alerts waiting for delivery. Default 1000. */
  readonly queueCapacity?: number;
  /** Suppress repeats of a key for this long. Default 60 000. 0 disables dedup. */
  readonly dedupWindowMs?: number;
  /** Attempts per sink per alert, including the first. Default 3. */
  readonly maxAttempts?: number;
  /** Delay before the first retry; doubles on each further retry. Default 500. */
  readonly backoffBaseMs?: number;
  /** Upper bound on the retry delay. Default 10 000. */
  readonly backoffMaxMs?: number;
  /** A single send that takes longer is treated as failed. Default 5000. */
  readonly attemptTimeoutMs?: number;
  /** Time source in epoch ms. Default Date.now. */
  readonly clock?: () => number;
  /** Waits between retries. Default setTimeout. Injected by tests. */
  readonly sleep?: (ms: number) => Promise<void>;
}

/** Raised for invalid notifier configuration. Code: NOTIFIER_CONFIG. */
export class NotifierConfigError extends Error {
  public readonly code = 'NOTIFIER_CONFIG';
  public readonly issues: readonly string[];

  constructor(issues: readonly string[]) {
    super(`invalid notifier config: ${issues.join('; ')}`);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'NotifierConfigError';
    this.issues = Object.freeze([...issues]);
  }
}

/** Raised for an alert that cannot be queued. Code: ALERT_INVALID. */
export class AlertError extends Error {
  public readonly code = 'ALERT_INVALID';

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'AlertError';
  }
}

// ---------------------------------------------------------------------------
// Limits and defaults
// ---------------------------------------------------------------------------

export const DEFAULT_QUEUE_CAPACITY = 1000;
export const DEFAULT_DEDUP_WINDOW_MS = 60_000;
export const DEFAULT_MAX_ATTEMPTS = 3;
export const DEFAULT_BACKOFF_BASE_MS = 500;
export const DEFAULT_BACKOFF_MAX_MS = 10_000;
export const DEFAULT_ATTEMPT_TIMEOUT_MS = 5_000;

const MAX_QUEUE_CAPACITY = 100_000;
const MAX_DEDUP_WINDOW_MS = 86_400_000;
const MAX_ATTEMPTS = 10;
const MAX_BACKOFF_MS = 600_000;
const MAX_ATTEMPT_TIMEOUT_MS = 60_000;
const MAX_SINKS = 32;
const MAX_DEDUP_KEYS = 10_000;
const DEAD_LETTER_LIMIT = 100;
const MAX_KEY_LENGTH = 256;
const MAX_SUMMARY_LENGTH = 1024;
const MAX_LABELS = 16;

function checkInteger(issues: string[], name: string, value: number, min: number, max: number): void {
  if (!Number.isInteger(value) || value < min || value > max) {
    issues.push(`${name} must be an integer from ${min} to ${max}`);
  }
}

// ---------------------------------------------------------------------------
// Sink helpers
// ---------------------------------------------------------------------------

/** Wraps a sink so it only receives alerts at or above a severity. */
export function minSeverityFilter(sink: AlertSink, minimum: AlertSeverity): AlertSink {
  return {
    name: sink.name,
    send(alert: Alert): Promise<void> | void {
      if (SEVERITY_RANK[alert.severity] < SEVERITY_RANK[minimum]) return;
      return sink.send(alert);
    },
  };
}

/** Builds a sink from a plain function, for example a logger. */
export function callbackSink(name: string, fn: (alert: Alert) => void | Promise<void>): AlertSink {
  return { name, send: fn };
}

/** Minimal fetch signature used by {@link webhookSink}. Tests pass a fake. */
export type FetchLike = (
  url: string,
  init: { method: string; headers: Record<string, string>; body: string },
) => Promise<{ readonly ok: boolean; readonly status: number }>;

export interface WebhookOptions {
  readonly name: string;
  /** http or https URL that receives a JSON POST per alert. */
  readonly url: string;
  /** Extra headers, for example an authorization header. */
  readonly headers?: Readonly<Record<string, string>>;
  readonly fetch?: FetchLike;
}

/**
 * Creates a sink that POSTs each alert as JSON.
 *
 * @throws {NotifierConfigError} If the URL is not an http or https URL.
 */
export function webhookSink(options: WebhookOptions): AlertSink {
  let target: URL;
  try {
    target = new URL(options.url);
  } catch {
    throw new NotifierConfigError([`webhook ${options.name}: url is not a valid URL`]);
  }
  if (target.protocol !== 'http:' && target.protocol !== 'https:') {
    throw new NotifierConfigError([`webhook ${options.name}: url must use http or https`]);
  }
  const extra = { ...(options.headers ?? {}) };
  const doFetch: FetchLike = options.fetch ?? ((url, init) => fetch(url, init));
  const url = target.toString();

  return {
    name: options.name,
    async send(alert: Alert): Promise<void> {
      const response = await doFetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...extra },
        body: JSON.stringify(alert),
      });
      if (!response.ok) throw new Error(`webhook responded with status ${response.status}`);
    },
  };
}

// ---------------------------------------------------------------------------
// Notifier
// ---------------------------------------------------------------------------

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(task: Promise<void>, ms: number): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timed out after ${ms} ms`)), ms);
    task.then(
      () => {
        clearTimeout(timer);
        resolve();
      },
      (err: unknown) => {
        clearTimeout(timer);
        reject(err);
      },
    );
  });
}

function describeError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export class AlertNotifier {
  private readonly sinks: readonly AlertSink[];
  private readonly queueCapacity: number;
  private readonly dedupWindowMs: number;
  private readonly maxAttempts: number;
  private readonly backoffBaseMs: number;
  private readonly backoffMaxMs: number;
  private readonly attemptTimeoutMs: number;
  private readonly clock: () => number;
  private readonly sleep: (ms: number) => Promise<void>;

  private readonly items: Alert[] = [];
  private head = 0;
  private readonly lastSent = new Map<string, number>();
  private readonly deadLetters: DeadLetter[] = [];
  private loop: Promise<void> | undefined;
  private closed = false;
  private readonly counters = {
    queued: 0,
    processed: 0,
    deliveries: 0,
    retries: 0,
    deadLettered: 0,
    deduplicated: 0,
    droppedQueueFull: 0,
  };

  /**
   * @throws {NotifierConfigError} If any option is out of range or sinks are duplicated.
   */
  constructor(options: NotifierOptions) {
    const issues: string[] = [];
    const sinks = options.sinks;
    if (!Array.isArray(sinks) || sinks.length === 0) {
      issues.push('at least one sink is required');
    } else {
      if (sinks.length > MAX_SINKS) issues.push(`at most ${MAX_SINKS} sinks are allowed`);
      const names = new Set<string>();
      for (const sink of sinks) {
        if (typeof sink.name !== 'string' || sink.name.length === 0) {
          issues.push('every sink needs a non-empty name');
        } else if (names.has(sink.name)) {
          issues.push(`sink name ${sink.name} is used more than once`);
        } else {
          names.add(sink.name);
        }
        if (typeof sink.send !== 'function') issues.push(`sink ${sink.name} has no send function`);
      }
    }

    this.queueCapacity = options.queueCapacity ?? DEFAULT_QUEUE_CAPACITY;
    this.dedupWindowMs = options.dedupWindowMs ?? DEFAULT_DEDUP_WINDOW_MS;
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    this.backoffBaseMs = options.backoffBaseMs ?? DEFAULT_BACKOFF_BASE_MS;
    this.backoffMaxMs = options.backoffMaxMs ?? DEFAULT_BACKOFF_MAX_MS;
    this.attemptTimeoutMs = options.attemptTimeoutMs ?? DEFAULT_ATTEMPT_TIMEOUT_MS;
    checkInteger(issues, 'queueCapacity', this.queueCapacity, 1, MAX_QUEUE_CAPACITY);
    checkInteger(issues, 'dedupWindowMs', this.dedupWindowMs, 0, MAX_DEDUP_WINDOW_MS);
    checkInteger(issues, 'maxAttempts', this.maxAttempts, 1, MAX_ATTEMPTS);
    checkInteger(issues, 'backoffBaseMs', this.backoffBaseMs, 0, MAX_BACKOFF_MS);
    checkInteger(issues, 'backoffMaxMs', this.backoffMaxMs, 0, MAX_BACKOFF_MS);
    checkInteger(issues, 'attemptTimeoutMs', this.attemptTimeoutMs, 1, MAX_ATTEMPT_TIMEOUT_MS);
    if (this.backoffMaxMs < this.backoffBaseMs) issues.push('backoffMaxMs must be at least backoffBaseMs');
    if (issues.length > 0) throw new NotifierConfigError(issues);

    this.sinks = Object.freeze([...sinks]);
    this.clock = options.clock ?? Date.now;
    this.sleep = options.sleep ?? defaultSleep;
  }

  /**
   * Queues an alert for delivery. Never rejects for sink problems.
   *
   * @throws {AlertError} If the alert is malformed.
   */
  notify(alert: Alert): NotifyOutcome {
    validateAlert(alert);
    if (this.closed) return 'closed';

    const now = this.clock();
    const last = this.lastSent.get(alert.key);
    if (last !== undefined && now - last < this.dedupWindowMs) {
      this.counters.deduplicated++;
      return 'deduplicated';
    }
    if (this.pending() >= this.queueCapacity) {
      this.counters.droppedQueueFull++;
      return 'dropped';
    }

    this.recordDedup(alert.key, now);
    this.items.push(alert);
    this.counters.queued++;
    this.start();
    return 'queued';
  }

  /** Resolves once the queue is empty and no delivery is in progress. */
  async flush(): Promise<void> {
    while (this.loop !== undefined) await this.loop;
  }

  /**
   * Stops accepting alerts. With `drain` (the default) it waits for queued
   * alerts to finish. Without it, queued alerts are discarded, and the one in
   * flight is still completed.
   *
   * @returns The number of queued alerts that were discarded.
   */
  async close(options: { readonly drain?: boolean } = {}): Promise<{ readonly discarded: number }> {
    this.closed = true;
    if (options.drain === false) {
      const discarded = this.pending();
      this.head = this.items.length;
      return { discarded };
    }
    await this.flush();
    return { discarded: 0 };
  }

  /** Dead letters recorded so far, oldest first. Bounded to the most recent 100. */
  deadLetterList(): readonly DeadLetter[] {
    return Object.freeze([...this.deadLetters]);
  }

  stats(): NotifierStats {
    return Object.freeze({
      ...this.counters,
      pending: this.pending(),
      closed: this.closed,
    });
  }

  // -- internals -------------------------------------------------------------

  private pending(): number {
    return this.items.length - this.head;
  }

  private start(): void {
    if (this.loop !== undefined) return;
    this.loop = this.drain().finally(() => {
      this.loop = undefined;
    });
  }

  private async drain(): Promise<void> {
    while (this.head < this.items.length) {
      const alert = this.items[this.head];
      this.head++;
      if (this.head > 1024 && this.head * 2 > this.items.length) {
        this.items.splice(0, this.head);
        this.head = 0;
      }
      if (alert === undefined) continue;
      for (const sink of this.sinks) {
        await this.deliverToSink(sink, alert);
      }
      this.counters.processed++;
    }
  }

  private async deliverToSink(sink: AlertSink, alert: Alert): Promise<void> {
    let lastError: unknown;
    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      try {
        await withTimeout(Promise.resolve().then(() => sink.send(alert)), this.attemptTimeoutMs);
        this.counters.deliveries++;
        return;
      } catch (err) {
        lastError = err;
        if (attempt < this.maxAttempts) {
          this.counters.retries++;
          await this.sleep(this.backoffFor(attempt));
        }
      }
    }
    this.counters.deadLettered++;
    this.deadLetters.push({
      sink: sink.name,
      alertKey: alert.key,
      attempts: this.maxAttempts,
      error: describeError(lastError),
      at: this.clock(),
    });
    if (this.deadLetters.length > DEAD_LETTER_LIMIT) this.deadLetters.shift();
  }

  /** Delay before retry number `attempt`, doubling from the base and capped at the maximum. */
  backoffFor(attempt: number): number {
    const delay = this.backoffBaseMs * 2 ** (attempt - 1);
    return Math.min(this.backoffMaxMs, delay);
  }

  private recordDedup(key: string, now: number): void {
    this.lastSent.set(key, now);
    if (this.lastSent.size <= MAX_DEDUP_KEYS) return;
    // Drop expired entries first, then the oldest, until back under the cap.
    for (const [existing, at] of this.lastSent) {
      if (now - at >= this.dedupWindowMs) this.lastSent.delete(existing);
    }
    for (const existing of this.lastSent.keys()) {
      if (this.lastSent.size <= MAX_DEDUP_KEYS) break;
      this.lastSent.delete(existing);
    }
  }
}

function validateAlert(alert: Alert): void {
  if (typeof alert !== 'object' || alert === null) throw new AlertError('alert must be an object');
  if (typeof alert.key !== 'string' || alert.key.length === 0 || alert.key.length > MAX_KEY_LENGTH) {
    throw new AlertError(`alert key must be 1-${MAX_KEY_LENGTH} characters`);
  }
  if (!Object.hasOwn(SEVERITY_RANK, alert.severity)) {
    throw new AlertError('alert severity must be info, warning or critical');
  }
  if (typeof alert.summary !== 'string' || alert.summary.length === 0 || alert.summary.length > MAX_SUMMARY_LENGTH) {
    throw new AlertError(`alert summary must be 1-${MAX_SUMMARY_LENGTH} characters`);
  }
  if (typeof alert.firedAt !== 'number' || !Number.isFinite(alert.firedAt)) {
    throw new AlertError('alert firedAt must be a finite number');
  }
  const labels = alert.labels ?? {};
  if (Object.keys(labels).length > MAX_LABELS) throw new AlertError(`at most ${MAX_LABELS} alert labels`);
  for (const value of Object.values(labels)) {
    if (typeof value !== 'string') throw new AlertError('alert label values must be strings');
  }
}
