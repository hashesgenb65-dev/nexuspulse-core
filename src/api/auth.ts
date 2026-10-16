/**
 * Bearer-token authentication and role checks for the NexusPulse HTTP API.
 *
 * Tokens are configured as named secrets with a role. Only a SHA-256 digest of
 * each secret is kept in memory, and every presented token is compared against
 * every configured digest in constant time, so a response does not reveal which
 * token prefix matched or how many tokens exist.
 *
 * Roles, lowest to highest: `reader` may call read routes; `writer` may also
 * change state. A `writer` satisfies a `reader` requirement.
 *
 * This module does not handle TLS or token storage. Run the API behind TLS, and
 * load secrets from a file or secret manager rather than from source control.
 */

import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import type { IncomingMessage } from 'node:http';
import { ApiError, type ApiResponse, type Handler, type RequestContext } from './http_router.js';

// ---------------------------------------------------------------------------
// Constants and types
// ---------------------------------------------------------------------------

export type Role = 'reader' | 'writer';

const ROLE_RANK: Readonly<Record<Role, number>> = Object.freeze({ reader: 1, writer: 2 });

/** Every valid role, lowest first. */
export const ROLE_NAMES: readonly Role[] = Object.freeze(['reader', 'writer'] as const);

export const MIN_SECRET_LENGTH = 24;
export const MAX_SECRET_LENGTH = 512;
export const MAX_TOKENS = 100;
/** Length of a token produced by {@link generateSecret}. */
export const GENERATED_SECRET_LENGTH = 43;

const NAME_PATTERN = /^[a-z0-9][a-z0-9-]{0,62}$/;
const SECRET_PATTERN = /^[A-Za-z0-9._~+\/=_-]+$/;
const BEARER_PATTERN = /^Bearer ([A-Za-z0-9._~+\/=_-]+)$/i;
const CHALLENGE = 'Bearer realm="nexuspulse"';

export interface TokenSpec {
  /** Lowercase name used in logs and error details, for example `ci-reader`. */
  readonly name: string;
  readonly secret: string;
  readonly role: Role;
}

export interface Principal {
  readonly name: string;
  readonly role: Role;
}

export interface AuthIssue {
  /** The token name the problem belongs to, or `<config>` for problems with the list itself. */
  readonly token: string;
  readonly message: string;
}

/** Raised when the token configuration is invalid. Code: AUTH_CONFIG. */
export class AuthConfigError extends Error {
  public readonly code = 'AUTH_CONFIG';
  public readonly issues: readonly AuthIssue[];

  constructor(issues: readonly AuthIssue[]) {
    super(`invalid auth config: ${issues.map((i) => `${i.token}: ${i.message}`).join('; ')}`);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'AuthConfigError';
    this.issues = Object.freeze([...issues]);
  }
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

function digest(secret: string): Buffer {
  return createHash('sha256').update(secret, 'utf8').digest();
}

/**
 * Extracts the token from an `Authorization` header value. The scheme is
 * case-insensitive. Returns undefined when the header is absent or not a
 * well-formed bearer credential.
 */
export function parseBearer(header: string | undefined): string | undefined {
  if (header === undefined) return undefined;
  const match = BEARER_PATTERN.exec(header.trim());
  return match?.[1];
}

/** Creates a random token with 256 bits of entropy, encoded as base64url. */
export function generateSecret(): string {
  return randomBytes(32).toString('base64url');
}

/** Returns every problem in a token list, or an empty array when the list is valid. */
export function validateTokenSpecs(specs: unknown): AuthIssue[] {
  const issues: AuthIssue[] = [];
  if (!Array.isArray(specs)) {
    return [{ token: '<config>', message: 'must be an array of token specs' }];
  }
  if (specs.length === 0) {
    issues.push({ token: '<config>', message: 'at least one token is required' });
  }
  if (specs.length > MAX_TOKENS) {
    issues.push({ token: '<config>', message: `at most ${MAX_TOKENS} tokens are allowed` });
  }

  const names = new Set<string>();
  const secrets = new Map<string, string>();
  for (const [index, spec] of specs.entries()) {
    const label = typeof spec === 'object' && spec !== null && typeof (spec as { name?: unknown }).name === 'string'
      ? (spec as { name: string }).name
      : `#${index}`;
    if (typeof spec !== 'object' || spec === null) {
      issues.push({ token: label, message: 'must be an object' });
      continue;
    }
    const { name, secret, role } = spec as { name?: unknown; secret?: unknown; role?: unknown };

    if (typeof name !== 'string' || !NAME_PATTERN.test(name)) {
      issues.push({ token: label, message: 'name must be 1-63 lowercase letters, digits or hyphens' });
    } else if (names.has(name)) {
      issues.push({ token: label, message: 'name is used more than once' });
    } else {
      names.add(name);
    }

    if (typeof secret !== 'string') {
      issues.push({ token: label, message: 'secret must be a string' });
    } else if (secret.length < MIN_SECRET_LENGTH || secret.length > MAX_SECRET_LENGTH) {
      issues.push({ token: label, message: `secret must be ${MIN_SECRET_LENGTH}-${MAX_SECRET_LENGTH} characters` });
    } else if (!SECRET_PATTERN.test(secret)) {
      issues.push({ token: label, message: 'secret may only contain letters, digits and . _ ~ + / = -' });
    } else {
      const previous = secrets.get(secret);
      if (previous !== undefined) {
        issues.push({ token: label, message: `secret is the same as token ${previous}` });
      } else {
        secrets.set(secret, label);
      }
    }

    if (typeof role !== 'string' || !Object.hasOwn(ROLE_RANK, role)) {
      issues.push({ token: label, message: 'role must be reader or writer' });
    }
  }
  return issues;
}

function unauthorized(message: string): ApiError {
  return new ApiError(401, 'UNAUTHORIZED', message, {}, { 'www-authenticate': CHALLENGE });
}

/** The principal recorded for a request that passed through {@link TokenAuth.guard}. */
const principals = new WeakMap<IncomingMessage, Principal>();

/** Returns the authenticated principal for a request, or undefined if none was recorded. */
export function principalOf(ctx: RequestContext): Principal | undefined {
  return principals.get(ctx.req);
}

// ---------------------------------------------------------------------------
// Authenticator
// ---------------------------------------------------------------------------

interface Entry {
  readonly name: string;
  readonly role: Role;
  readonly digest: Buffer;
}

export class TokenAuth {
  private readonly entries: readonly Entry[];

  /**
   * @throws {AuthConfigError} If any token spec is invalid. All problems are reported together.
   */
  constructor(specs: readonly TokenSpec[]) {
    const issues = validateTokenSpecs(specs);
    if (issues.length > 0) throw new AuthConfigError(issues);
    this.entries = Object.freeze(
      specs.map((spec) => Object.freeze({ name: spec.name, role: spec.role, digest: digest(spec.secret) })),
    );
  }

  get size(): number {
    return this.entries.length;
  }

  /** Names and roles of the configured tokens. Secrets are never included. */
  describe(): readonly { readonly name: string; readonly role: Role }[] {
    return this.entries.map((entry) => ({ name: entry.name, role: entry.role }));
  }

  /**
   * Returns the principal that owns a secret, or undefined. Every configured
   * digest is compared even after a match, so timing does not depend on which
   * token matched.
   */
  verifySecret(secret: string): Principal | undefined {
    const candidate = digest(secret);
    let found: Principal | undefined;
    for (const entry of this.entries) {
      const same = timingSafeEqual(candidate, entry.digest);
      if (same && found === undefined) found = { name: entry.name, role: entry.role };
    }
    return found;
  }

  /**
   * Authenticates an `Authorization` header value.
   *
   * @throws {ApiError} 401 with a `WWW-Authenticate` challenge if the header is
   *   missing, malformed, or does not match any token.
   */
  authenticateHeader(header: string | undefined): Principal {
    const secret = parseBearer(header);
    if (secret === undefined) {
      throw unauthorized(header === undefined ? 'missing bearer token' : 'malformed authorization header');
    }
    const principal = this.verifySecret(secret);
    if (principal === undefined) throw unauthorized('invalid token');
    return principal;
  }

  /**
   * Wraps a handler so it runs only for a principal whose role is at least `minimum`.
   *
   * @throws {ApiError} 401 for missing or invalid credentials, 403 for insufficient role.
   */
  guard(minimum: Role, handler: Handler): Handler {
    return (ctx: RequestContext): ApiResponse | void | Promise<ApiResponse | void> => {
      const principal = this.authenticateHeader(headerOf(ctx.req));
      if (ROLE_RANK[principal.role] < ROLE_RANK[minimum]) {
        throw new ApiError(403, 'FORBIDDEN', `this route requires the ${minimum} role`, {
          role: principal.role,
          required: minimum,
        });
      }
      principals.set(ctx.req, principal);
      return handler(ctx);
    };
  }
}

function headerOf(req: IncomingMessage): string | undefined {
  const value = req.headers.authorization;
  return typeof value === 'string' ? value : undefined;
}

/** True when a role satisfies a required role. Exposed for callers that check roles themselves. */
export function roleSatisfies(actual: Role, required: Role): boolean {
  return ROLE_RANK[actual] >= ROLE_RANK[required];
}
