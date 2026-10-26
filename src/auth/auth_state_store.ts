/**
 * File-backed store for API keys and sessions, so both survive a restart.
 *
 * The file holds key records (the secret is kept only as a scrypt hash) and the live session ids.
 * A session id is a bearer credential, so the file is written with owner-only permissions and is
 * refused on load if group or others can read it. Each save writes a temporary file, flushes it,
 * renames it over the old file, and flushes the directory, so a crash leaves the previous complete
 * file in place.
 *
 * A file that cannot be parsed or holds a malformed record stops the load. Starting with records
 * silently dropped would let a damaged file change who can log in.
 */

import { createHash } from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import type { ApiKeyRecord, AuthSession, ScopePermission } from './manager.js';

export const AUTH_STATE_VERSION = 1;
const FILE_MODE = 0o600;
const KEY_ID_PATTERN = /^np_id_[0-9a-f]{16}$/;
const SESSION_ID_PATTERN = /^sess_[0-9a-f]{32}$/;
const ROLES: readonly string[] = ['ADMIN', 'ANALYST', 'INGEST_SERVICE', 'READ_ONLY'];
const ACTIONS: readonly string[] = ['read', 'write', 'delete', 'admin'];

/** Raised when the state file cannot be used. Code: AUTH_STATE_INVALID. */
export class AuthStateError extends Error {
  readonly code = 'AUTH_STATE_INVALID' as const;

  constructor(message: string) {
    super(message);
    this.name = 'AuthStateError';
  }
}

export interface PersistedAuthState {
  readonly apiKeys: readonly ApiKeyRecord[];
  readonly sessions: readonly AuthSession[];
}

/** Where keys and sessions are kept. The manager saves the full state after every change to either. */
export interface AuthStateStore {
  load(): PersistedAuthState;
  save(state: PersistedAuthState): void;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isScopes(value: unknown): value is ScopePermission[] {
  if (!Array.isArray(value)) return false;
  return value.every((permission) => {
    if (!isRecord(permission) || typeof permission.resource !== 'string') return false;
    if (!Array.isArray(permission.actions)) return false;
    return permission.actions.every((action) => typeof action === 'string' && ACTIONS.includes(action));
  });
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function isApiKeyRecord(value: unknown): value is ApiKeyRecord {
  if (!isRecord(value)) return false;
  return (
    typeof value.keyId === 'string' &&
    KEY_ID_PATTERN.test(value.keyId) &&
    typeof value.hashedSecret === 'string' &&
    value.hashedSecret.startsWith('scrypt$') &&
    typeof value.tenantId === 'string' &&
    value.tenantId.length > 0 &&
    typeof value.role === 'string' &&
    ROLES.includes(value.role) &&
    isScopes(value.scopes) &&
    isFiniteNumber(value.createdAt) &&
    (value.expiresAt === null || isFiniteNumber(value.expiresAt)) &&
    typeof value.isActive === 'boolean' &&
    isFiniteNumber(value.rateLimitPerMin)
  );
}

function isSession(value: unknown): value is AuthSession {
  if (!isRecord(value)) return false;
  return (
    typeof value.sessionId === 'string' &&
    SESSION_ID_PATTERN.test(value.sessionId) &&
    typeof value.keyId === 'string' &&
    KEY_ID_PATTERN.test(value.keyId) &&
    typeof value.tenantId === 'string' &&
    typeof value.role === 'string' &&
    ROLES.includes(value.role) &&
    isScopes(value.permissions) &&
    isFiniteNumber(value.authenticatedAt) &&
    isFiniteNumber(value.expiresAt)
  );
}

/** Hash of a file's exact bytes, or null when there is no file. */
function fingerprint(bytes: Buffer | null): string | null {
  return bytes === null ? null : createHash('sha256').update(bytes).digest('hex');
}

export class FileAuthStateStore implements AuthStateStore {
  private readonly file: string;
  /**
   * Fingerprint of the file as this store last read or wrote it. Undefined before the first read or write.
   * A save refuses to replace a file whose fingerprint differs, so a running member cannot silently undo
   * a change made to the file by someone else.
   */
  private known: string | null | undefined;

  constructor(file: string) {
    if (file.length === 0) throw new AuthStateError('the state file path must not be empty');
    this.file = file;
  }

  /**
   * Reads the state saved by the last successful save. Returns an empty state when no file exists.
   *
   * @throws {AuthStateError} If the file is readable by others, is not valid JSON, or holds a malformed record.
   */
  load(): PersistedAuthState {
    let stat: fs.Stats;
    try {
      stat = fs.statSync(this.file);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
        this.known = null;
        return { apiKeys: [], sessions: [] };
      }
      throw error;
    }
    if (!stat.isFile()) throw new AuthStateError(`${this.file} is not a regular file`);
    if (process.platform !== 'win32' && (stat.mode & 0o077) !== 0) {
      const mode = (stat.mode & 0o777).toString(8).padStart(3, '0');
      throw new AuthStateError(`${this.file} has mode ${mode}; group and others must have no access (chmod 600)`);
    }

    const bytes = fs.readFileSync(this.file);
    let parsed: unknown;
    try {
      parsed = JSON.parse(bytes.toString('utf8'));
    } catch {
      throw new AuthStateError(`${this.file} is not valid JSON`);
    }
    this.known = fingerprint(bytes);
    if (!isRecord(parsed) || parsed.version !== AUTH_STATE_VERSION) {
      throw new AuthStateError(`${this.file} is not a version ${AUTH_STATE_VERSION} state file`);
    }
    if (!Array.isArray(parsed.apiKeys) || !Array.isArray(parsed.sessions)) {
      throw new AuthStateError(`${this.file} must hold apiKeys and sessions arrays`);
    }
    parsed.apiKeys.forEach((entry, index) => {
      if (!isApiKeyRecord(entry)) throw new AuthStateError(`api key ${index} in ${this.file} is malformed`);
    });
    parsed.sessions.forEach((entry, index) => {
      if (!isSession(entry)) throw new AuthStateError(`session ${index} in ${this.file} is malformed`);
    });
    return { apiKeys: parsed.apiKeys as ApiKeyRecord[], sessions: parsed.sessions as AuthSession[] };
  }

  /**
   * Replaces the saved state. The previous file stays in place until the new one is fully written.
   *
   * @throws {AuthStateError} If the file changed since this store last read or wrote it. Load the state
   *   again before saving.
   * @throws {Error} If any write, flush or rename fails. The temporary file is removed first.
   */
  save(state: PersistedAuthState): void {
    if (this.known !== undefined && this.currentFingerprint() !== this.known) {
      throw new AuthStateError(
        `${this.file} changed since it was last read; it was not overwritten. Reload the state before saving.`,
      );
    }
    const tmp = `${this.file}.${process.pid}.tmp`;
    const body = `${JSON.stringify({ version: AUTH_STATE_VERSION, ...state }, null, 2)}\n`;

    let fd: number | undefined;
    try {
      fd = fs.openSync(tmp, 'w', FILE_MODE);
      const bytes = Buffer.from(body, 'utf8');
      let offset = 0;
      while (offset < bytes.length) offset += fs.writeSync(fd, bytes, offset, bytes.length - offset);
      fs.fsyncSync(fd);
      fs.closeSync(fd);
      fd = undefined;
      fs.renameSync(tmp, this.file);
    } catch (error) {
      if (fd !== undefined) fs.closeSync(fd);
      fs.rmSync(tmp, { force: true });
      throw error;
    }
    syncDirectory(path.dirname(this.file));
    this.known = fingerprint(Buffer.from(body, 'utf8'));
  }

  /** Fingerprint of the file as it is on disk now. */
  private currentFingerprint(): string | null {
    try {
      return fingerprint(fs.readFileSync(this.file));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
      throw error;
    }
  }
}

function syncDirectory(directory: string): void {
  let fd: number | undefined;
  try {
    fd = fs.openSync(directory, 'r');
    fs.fsyncSync(fd);
  } catch {
    // Not supported on every platform. The file itself was flushed before the rename.
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
}
