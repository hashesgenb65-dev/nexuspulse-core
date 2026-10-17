/**
 * Peer authentication for the mesh: every frame between members carries an HMAC-SHA256 tag made
 * with a secret the whole cluster shares. A frame without a valid tag, or with a timestamp outside
 * the allowed skew, is refused.
 *
 * The tag covers the whole frame, so neither the body nor the request id can be changed without
 * breaking it. Replies are tagged too, so a reply cannot be spoofed by someone who lacks the secret.
 *
 * Replay. A captured frame can be sent again within the skew window. Raft tolerates that: a repeated
 * vote request for an old term is refused, and a repeated AppendEntries or snapshot is either
 * idempotent or rejected by its consistency check. Keep the skew short.
 *
 * The secret is hex text of at least 32 bytes, stored in a file readable only by its owner.
 */

import * as crypto from 'node:crypto';
import * as fs from 'node:fs';

export const MIN_SECRET_BYTES = 32;
export const DEFAULT_SKEW_MS = 30_000;

/** The authentication fields added to every frame. */
export interface FrameAuth {
  readonly ts: number;
  readonly mac: string;
}

/** Raised when a secret is missing, too short, or readable by other users. Code: PEER_AUTH. */
export class PeerAuthError extends Error {
  public readonly code = 'PEER_AUTH';

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'PeerAuthError';
  }
}

/**
 * Serialises a value with object keys sorted at every level, so that the same frame always gives the
 * same bytes. Keys whose value is undefined are dropped, as in JSON.
 */
export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null';
  if (Array.isArray(value)) return `[${value.map((item) => canonicalJson(item)).join(',')}]`;

  const record = value as Record<string, unknown>;
  const parts: string[] = [];
  for (const key of Object.keys(record).sort()) {
    const item = record[key];
    if (item === undefined) continue;
    parts.push(`${JSON.stringify(key)}:${canonicalJson(item)}`);
  }
  return `{${parts.join(',')}}`;
}

/** Turns hex text into a secret. @throws {PeerAuthError} If it is not hex or is shorter than MIN_SECRET_BYTES. */
export function parseSecretHex(text: string): Buffer {
  const trimmed = text.trim();
  if (!/^[0-9a-fA-F]+$/.test(trimmed) || trimmed.length % 2 !== 0) {
    throw new PeerAuthError('peer secret must be hex text');
  }
  const bytes = Buffer.from(trimmed, 'hex');
  if (bytes.length < MIN_SECRET_BYTES) {
    throw new PeerAuthError(`peer secret must be at least ${MIN_SECRET_BYTES} bytes`);
  }
  return bytes;
}

/**
 * Reads a secret from a file. On POSIX the file must not be readable or writable by group or others.
 *
 * @throws {PeerAuthError} If the file is missing, too open, or does not hold a valid secret.
 */
export function loadPeerSecret(file: string): Buffer {
  let mode: number;
  try {
    mode = fs.statSync(file).mode;
  } catch {
    throw new PeerAuthError(`peer secret file ${file} cannot be read`);
  }
  if (process.platform !== 'win32' && (mode & 0o077) !== 0) {
    throw new PeerAuthError(`peer secret file ${file} must not be readable by group or others`);
  }
  return parseSecretHex(fs.readFileSync(file, 'utf8'));
}

/** The tag for a frame sent at `ts`. The frame must not already carry an `auth` field. */
export function tagFrame(secret: Buffer, frame: object, ts: number): string {
  return crypto.createHmac('sha256', secret).update(`${ts}\n${canonicalJson(frame)}`).digest('hex');
}

/** Returns the frame's auth fields with a fresh tag. */
export function authenticate(secret: Buffer, frame: object, now: number): FrameAuth {
  return { ts: now, mac: tagFrame(secret, frame, now) };
}

/**
 * Checks a received frame. `frame` is the whole frame as received, including its `auth` field.
 * Returns the reason for refusal, or undefined when the frame is genuine and current.
 */
export function refuseFrame(
  secret: Buffer,
  frame: Record<string, unknown>,
  now: number,
  skewMs: number = DEFAULT_SKEW_MS,
): string | undefined {
  const auth = frame.auth;
  if (typeof auth !== 'object' || auth === null) return 'missing authentication';
  const { ts, mac } = auth as { ts?: unknown; mac?: unknown };
  if (typeof ts !== 'number' || !Number.isInteger(ts) || typeof mac !== 'string') {
    return 'malformed authentication';
  }
  if (Math.abs(now - ts) > skewMs) return 'timestamp outside the allowed skew';

  const { auth: _auth, ...unsigned } = frame;
  const expected = Buffer.from(tagFrame(secret, unsigned, ts), 'hex');
  const received = Buffer.from(mac, 'hex');
  if (received.length !== expected.length || !crypto.timingSafeEqual(received, expected)) {
    return 'bad tag';
  }
  return undefined;
}
