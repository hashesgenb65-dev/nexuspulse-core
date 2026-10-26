/**
 * Limits failed login attempts per key id. After too many wrong secrets for one key id inside the
 * window, further attempts for that key id are refused before the secret is checked, until the window
 * passes. A correct login clears the record.
 *
 * Keyed by key id, not by client address, because the request does not carry a reliable client address.
 * The record count is bounded, so a flood of guessed key ids cannot grow memory without limit.
 */

export const DEFAULT_MAX_FAILED_LOGINS = 5;
export const DEFAULT_LOGIN_WINDOW_MS = 15 * 60 * 1000;
export const DEFAULT_MAX_LOGIN_RECORDS = 10_000;

export interface LoginLimiterOptions {
  /** Failures allowed per key id inside the window. Defaults to 5. */
  readonly maxFailures?: number;
  /** How long failures are remembered, and how long a block lasts. Defaults to 15 minutes. */
  readonly windowMs?: number;
  /** Most key ids tracked at once. The oldest record is dropped when the table is full. */
  readonly maxRecords?: number;
  /** Clock in milliseconds. Defaults to Date.now. */
  readonly now?: () => number;
}

export type LoginCheck =
  | { readonly allowed: true }
  | { readonly allowed: false; readonly retryAfterMs: number };

interface FailureRecord {
  count: number;
  windowStart: number;
}

export class LoginFailureLimiter {
  private readonly maxFailures: number;
  private readonly windowMs: number;
  private readonly maxRecords: number;
  private readonly now: () => number;
  private readonly records = new Map<string, FailureRecord>();

  constructor(options: LoginLimiterOptions = {}) {
    this.maxFailures = options.maxFailures ?? DEFAULT_MAX_FAILED_LOGINS;
    this.windowMs = options.windowMs ?? DEFAULT_LOGIN_WINDOW_MS;
    this.maxRecords = options.maxRecords ?? DEFAULT_MAX_LOGIN_RECORDS;
    this.now = options.now ?? Date.now;
    if (!Number.isInteger(this.maxFailures) || this.maxFailures < 1) {
      throw new RangeError('maxFailures must be a positive integer');
    }
    if (!Number.isInteger(this.windowMs) || this.windowMs < 1) {
      throw new RangeError('windowMs must be a positive integer');
    }
    if (!Number.isInteger(this.maxRecords) || this.maxRecords < 1) {
      throw new RangeError('maxRecords must be a positive integer');
    }
  }

  /** Says whether an attempt for this key id may be checked now. Does not count as a failure. */
  public check(keyId: string): LoginCheck {
    const record = this.live(keyId);
    if (record === undefined || record.count < this.maxFailures) return { allowed: true };
    return { allowed: false, retryAfterMs: record.windowStart + this.windowMs - this.now() };
  }

  /** Counts one wrong secret for this key id. */
  public recordFailure(keyId: string): void {
    const now = this.now();
    const record = this.live(keyId);
    if (record === undefined) {
      this.makeRoom();
      this.records.set(keyId, { count: 1, windowStart: now });
      return;
    }
    record.count += 1;
  }

  /** Forgets the failures for this key id, after a correct login. */
  public clear(keyId: string): void {
    this.records.delete(keyId);
  }

  /** The record for this key id, or undefined when it is absent or its window has passed. */
  private live(keyId: string): FailureRecord | undefined {
    const record = this.records.get(keyId);
    if (record === undefined) return undefined;
    if (this.now() - record.windowStart >= this.windowMs) {
      this.records.delete(keyId);
      return undefined;
    }
    return record;
  }

  /** Drops expired records, then the oldest ones, until there is room for one more. */
  private makeRoom(): void {
    if (this.records.size < this.maxRecords) return;
    for (const [keyId, record] of this.records) {
      if (this.now() - record.windowStart >= this.windowMs) this.records.delete(keyId);
    }
    while (this.records.size >= this.maxRecords) {
      const oldest = this.records.keys().next();
      if (oldest.done) break;
      this.records.delete(oldest.value);
    }
  }
}
