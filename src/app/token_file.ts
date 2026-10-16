/**
 * Token file: loads API bearer tokens from a JSON file on disk.
 *
 * File format: a JSON array of token specs, for example
 *
 *   [{ "name": "ci-reader", "secret": "...", "role": "reader" }]
 *
 * The loader refuses files that are too large, that are not regular files, that
 * are readable by group or others (on POSIX systems), that are not valid JSON,
 * or that fail token validation. Every validation problem is reported together.
 *
 * Keep the file mode at 0600. Secrets in a world-readable file defeat the API's
 * authentication, so the loader refuses such a file by default rather than
 * warning and continuing.
 */

import * as fs from 'node:fs';
import { generateSecret, TokenAuth, validateTokenSpecs, type Role, type TokenSpec } from '../api/auth.js';

export const MAX_TOKEN_FILE_BYTES = 64 * 1024;

export type TokenFileReason =
  | 'missing'
  | 'not-a-file'
  | 'too-large'
  | 'insecure-permissions'
  | 'malformed-json'
  | 'invalid-tokens';

/** Raised when a token file cannot be used. Code: TOKEN_FILE_INVALID. */
export class TokenFileError extends Error {
  public readonly code = 'TOKEN_FILE_INVALID';
  public readonly reason: TokenFileReason;
  public readonly issues: readonly string[];

  constructor(reason: TokenFileReason, message: string, issues: readonly string[] = []) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'TokenFileError';
    this.reason = reason;
    this.issues = Object.freeze([...issues]);
  }
}

export interface LoadTokenFileOptions {
  /** Accept a file that group or others can read. Off by default. */
  readonly allowInsecurePermissions?: boolean;
}

/**
 * Loads and validates a token file, returning a ready authenticator.
 *
 * @throws {TokenFileError} With a `reason` describing what failed.
 */
export function loadTokenFile(file: string, options: LoadTokenFileOptions = {}): TokenAuth {
  let stat: fs.Stats;
  try {
    stat = fs.statSync(file);
  } catch {
    throw new TokenFileError('missing', `token file ${file} does not exist or cannot be read`);
  }
  if (!stat.isFile()) throw new TokenFileError('not-a-file', `token file ${file} is not a regular file`);
  if (stat.size > MAX_TOKEN_FILE_BYTES) {
    throw new TokenFileError('too-large', `token file ${file} is larger than ${MAX_TOKEN_FILE_BYTES} bytes`);
  }
  if (process.platform !== 'win32' && (stat.mode & 0o077) !== 0 && options.allowInsecurePermissions !== true) {
    const mode = (stat.mode & 0o777).toString(8).padStart(3, '0');
    throw new TokenFileError(
      'insecure-permissions',
      `token file ${file} has mode ${mode}; group and others must have no access (chmod 600)`,
    );
  }

  let text: string;
  try {
    text = fs.readFileSync(file, 'utf8');
  } catch {
    throw new TokenFileError('missing', `token file ${file} cannot be read`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new TokenFileError('malformed-json', `token file ${file} is not valid JSON: ${(err as Error).message}`);
  }

  const issues = validateTokenSpecs(parsed);
  if (issues.length > 0) {
    const lines = issues.map((i) => `${i.token}: ${i.message}`);
    throw new TokenFileError('invalid-tokens', `token file ${file} is invalid: ${lines.join('; ')}`, lines);
  }
  return new TokenAuth(parsed as TokenSpec[]);
}

/** Creates a token spec with a freshly generated secret. */
export function newTokenSpec(name: string, role: Role): TokenSpec {
  return { name, role, secret: generateSecret() };
}

/**
 * Writes a new token file with mode 0600. Refuses to overwrite an existing file.
 *
 * @throws {TokenFileError} If the specs are invalid, or the file already exists.
 */
export function writeTokenFile(file: string, specs: readonly TokenSpec[]): void {
  const issues = validateTokenSpecs(specs);
  if (issues.length > 0) {
    const lines = issues.map((i) => `${i.token}: ${i.message}`);
    throw new TokenFileError('invalid-tokens', `refusing to write invalid tokens: ${lines.join('; ')}`, lines);
  }
  const body = `${JSON.stringify(specs, null, 2)}\n`;
  try {
    fs.writeFileSync(file, body, { flag: 'wx', mode: 0o600 });
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'EEXIST') throw new TokenFileError('invalid-tokens', `token file ${file} already exists`);
    throw err;
  }
}
