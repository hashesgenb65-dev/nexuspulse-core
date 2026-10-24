/**
 * Token rotation without a restart.
 *
 * {@link RotatingTokenAuth.reload} swaps in a new token set atomically. Three cases:
 *
 * - A name whose secret changed keeps its old secret valid for a grace period, so clients can
 *   switch to the new secret without an outage. Once the grace period ends the old secret is refused.
 * - A name that was removed loses access immediately, and so does its old secret.
 * - Roles always come from the new set. A demotion applies at once, even to a secret still in its
 *   grace period.
 *
 * A reload that fails validation throws and leaves the current tokens in place.
 */

import { createHash, timingSafeEqual } from 'node:crypto';
import {
  AuthConfigError,
  authenticateWith,
  guardWith,
  TokenAuth,
  type Authenticator,
  type Principal,
  type Role,
  type TokenSpec,
} from './auth.js';
import type { Handler } from './http_router.js';

export const DEFAULT_ROTATION_GRACE_MS = 5 * 60_000;
export const MAX_ROTATION_GRACE_MS = 24 * 60 * 60_000;

export interface RotationOptions {
  /** How long a replaced secret stays valid. Defaults to 5 minutes; at most 24 hours. */
  readonly graceMs?: number;
  /** Clock in milliseconds. Defaults to Date.now. */
  readonly now?: () => number;
}

export interface RotationReport {
  /** Names that were not configured before this reload. */
  readonly added: readonly string[];
  /** Names that were configured before and are gone now. Their access ended immediately. */
  readonly removed: readonly string[];
  /** Names whose secret changed. Their old secret is in its grace period. */
  readonly rotated: readonly string[];
  /** Replaced secrets still inside their grace period after this reload. */
  readonly retiring: number;
}

interface Indexed {
  readonly digest: Buffer;
  readonly role: Role;
}

interface Retired {
  readonly name: string;
  readonly digest: Buffer;
  readonly until: number;
}

function digest(secret: string): Buffer {
  return createHash('sha256').update(secret, 'utf8').digest();
}

function indexSpecs(specs: readonly TokenSpec[]): Map<string, Indexed> {
  return new Map(specs.map((spec) => [spec.name, { digest: digest(spec.secret), role: spec.role }]));
}

export class RotatingTokenAuth implements Authenticator {
  private active: TokenAuth;
  private index: Map<string, Indexed>;
  private retired: Retired[] = [];
  private readonly graceMs: number;
  private readonly now: () => number;

  /**
   * @throws {AuthConfigError} If the tokens are invalid or the grace period is out of range.
   */
  constructor(specs: readonly TokenSpec[], options: RotationOptions = {}) {
    const graceMs = options.graceMs ?? DEFAULT_ROTATION_GRACE_MS;
    if (!Number.isInteger(graceMs) || graceMs < 0 || graceMs > MAX_ROTATION_GRACE_MS) {
      throw new AuthConfigError([
        { token: '<config>', message: `rotation grace period must be a whole number of ms from 0 to ${MAX_ROTATION_GRACE_MS}` },
      ]);
    }
    this.graceMs = graceMs;
    this.now = options.now ?? Date.now;
    this.active = new TokenAuth(specs);
    this.index = indexSpecs(specs);
  }

  get size(): number {
    return this.active.size;
  }

  describe(): readonly { readonly name: string; readonly role: Role }[] {
    return this.active.describe();
  }

  /**
   * Replaces the token set. Validation happens first, so an invalid set changes nothing.
   *
   * @throws {AuthConfigError} If the new set is invalid. The current tokens stay in place.
   */
  reload(specs: readonly TokenSpec[]): RotationReport {
    const next = new TokenAuth(specs);
    const now = this.now();
    const nextIndex = indexSpecs(specs);

    const added: string[] = [];
    const removed: string[] = [];
    const rotated: string[] = [];
    for (const [name, before] of this.index) {
      const after = nextIndex.get(name);
      if (after === undefined) {
        removed.push(name);
      } else if (!after.digest.equals(before.digest)) {
        rotated.push(name);
        this.retired.push({ name, digest: before.digest, until: now + this.graceMs });
      }
    }
    for (const name of nextIndex.keys()) {
      if (!this.index.has(name)) added.push(name);
    }

    // Drop retired secrets whose name is gone or whose grace period has ended.
    this.retired = this.retired.filter((entry) => entry.until > now && nextIndex.has(entry.name));

    this.active = next;
    this.index = nextIndex;
    return { added, removed, rotated, retiring: this.retired.length };
  }

  /**
   * Returns the principal that owns a secret, or undefined. The current set is checked first, then
   * secrets still in their grace period. A retired secret gets the role its name has now.
   */
  verifySecret(secret: string): Principal | undefined {
    const now = this.now();
    this.retired = this.retired.filter((entry) => entry.until > now);

    const current = this.active.verifySecret(secret);
    if (current !== undefined) return current;

    const candidate = digest(secret);
    let found: Principal | undefined;
    for (const entry of this.retired) {
      const same = timingSafeEqual(candidate, entry.digest);
      const role = this.index.get(entry.name)?.role;
      if (same && found === undefined && role !== undefined) found = { name: entry.name, role };
    }
    return found;
  }

  authenticateHeader(header: string | undefined): Principal {
    return authenticateWith((secret) => this.verifySecret(secret), header);
  }

  guard(minimum: Role, handler: Handler): Handler {
    return guardWith((header) => this.authenticateHeader(header), minimum, handler);
  }
}
