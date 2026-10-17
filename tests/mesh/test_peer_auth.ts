/**
 * Tests for the peer authentication core: canonical serialisation, secret parsing and file checks,
 * tagging, and refusal of tampered, forged, stale or malformed frames.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  DEFAULT_SKEW_MS,
  MIN_SECRET_BYTES,
  PeerAuthError,
  authenticate,
  canonicalJson,
  loadPeerSecret,
  parseSecretHex,
  refuseFrame,
  tagFrame,
} from '../../src/mesh/peer_auth';

const SECRET = Buffer.alloc(MIN_SECRET_BYTES, 7);
const OTHER_SECRET = Buffer.alloc(MIN_SECRET_BYTES, 9);
const NOW = 1_700_000_000_000;
const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function signedRequest(secret = SECRET, ts = NOW): Record<string, unknown> {
  const frame = { type: 'request', id: 1, method: 'appendEntries', body: { term: 2, entries: [] } };
  return { ...frame, auth: authenticate(secret, frame, ts) };
}

describe('canonicalJson', () => {
  it('sorts object keys at every level, so key order does not matter', () => {
    expect(canonicalJson({ b: 1, a: { d: 2, c: [3, { z: 1, y: 2 }] } })).toBe(
      '{"a":{"c":[3,{"y":2,"z":1}],"d":2},"b":1}',
    );
    expect(canonicalJson({ a: 1, b: 2 })).toBe(canonicalJson({ b: 2, a: 1 }));
  });

  it('drops undefined fields, as JSON does', () => {
    expect(canonicalJson({ a: 1, b: undefined })).toBe('{"a":1}');
  });

  it('keeps array order', () => {
    expect(canonicalJson([2, 1])).toBe('[2,1]');
  });
});

describe('parseSecretHex', () => {
  it('accepts hex of at least the minimum length, ignoring surrounding whitespace', () => {
    expect(parseSecretHex(`  ${'ab'.repeat(MIN_SECRET_BYTES)}\n`)).toHaveLength(MIN_SECRET_BYTES);
  });

  it('rejects text that is not hex, odd-length hex, and short secrets', () => {
    expect(() => parseSecretHex('not hex!')).toThrowError(/must be hex/);
    expect(() => parseSecretHex('abc')).toThrowError(/must be hex/);
    expect(() => parseSecretHex('ab'.repeat(MIN_SECRET_BYTES - 1))).toThrowError(/at least 32 bytes/);
  });
});

describe('loadPeerSecret', () => {
  function secretFile(contents: string, mode: number): string {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'peer-secret-'));
    dirs.push(dir);
    const file = path.join(dir, 'peer.secret');
    fs.writeFileSync(file, contents, { mode });
    fs.chmodSync(file, mode);
    return file;
  }

  it('reads a secret that only its owner can read', () => {
    const file = secretFile('ab'.repeat(MIN_SECRET_BYTES), 0o600);
    expect(loadPeerSecret(file)).toHaveLength(MIN_SECRET_BYTES);
  });

  it('refuses a secret that group or others can read', () => {
    const file = secretFile('ab'.repeat(MIN_SECRET_BYTES), 0o640);
    if (process.platform === 'win32') return;
    expect(() => loadPeerSecret(file)).toThrowError(/must not be readable by group or others/);
  });

  it('refuses a missing file', () => {
    expect(() => loadPeerSecret(path.join(os.tmpdir(), 'no-such-peer-secret'))).toThrowError(PeerAuthError);
  });
});

describe('tagging and refusal', () => {
  it('accepts a frame signed with the cluster secret inside the skew window', () => {
    expect(refuseFrame(SECRET, signedRequest(), NOW + 1_000)).toBeUndefined();
  });

  it('refuses a frame signed with another secret', () => {
    expect(refuseFrame(SECRET, signedRequest(OTHER_SECRET), NOW)).toBe('bad tag');
  });

  it('refuses a frame whose body was changed after it was signed', () => {
    const frame = signedRequest();
    (frame.body as { term: number }).term = 99;
    expect(refuseFrame(SECRET, frame, NOW)).toBe('bad tag');
  });

  it('refuses a reply whose id was changed, so it cannot be moved to another request', () => {
    const frame = signedRequest();
    frame.id = 2;
    expect(refuseFrame(SECRET, frame, NOW)).toBe('bad tag');
  });

  it('refuses a frame with no authentication', () => {
    const { auth: _auth, ...bare } = signedRequest();
    expect(refuseFrame(SECRET, bare, NOW)).toBe('missing authentication');
  });

  it('refuses malformed authentication', () => {
    expect(refuseFrame(SECRET, { ...signedRequest(), auth: 'nope' }, NOW)).toBe('missing authentication');
    expect(refuseFrame(SECRET, { ...signedRequest(), auth: { ts: 'x', mac: 'aa' } }, NOW)).toBe(
      'malformed authentication',
    );
    expect(refuseFrame(SECRET, { ...signedRequest(), auth: { ts: NOW, mac: 5 } }, NOW)).toBe(
      'malformed authentication',
    );
  });

  it('refuses a frame older or newer than the skew allows', () => {
    const frame = signedRequest(SECRET, NOW);
    expect(refuseFrame(SECRET, frame, NOW + DEFAULT_SKEW_MS + 1)).toBe('timestamp outside the allowed skew');
    expect(refuseFrame(SECRET, frame, NOW - DEFAULT_SKEW_MS - 1)).toBe('timestamp outside the allowed skew');
  });

  it('refuses a tag of the wrong length without throwing', () => {
    const frame = signedRequest();
    frame.auth = { ts: NOW, mac: 'abcd' };
    expect(refuseFrame(SECRET, frame, NOW)).toBe('bad tag');
  });

  it('gives the same tag for the same frame regardless of key order', () => {
    const a = { type: 'reply', id: 3, ok: true, body: { x: 1, y: 2 } };
    const b = { body: { y: 2, x: 1 }, ok: true, id: 3, type: 'reply' };
    expect(tagFrame(SECRET, a, NOW)).toBe(tagFrame(SECRET, b, NOW));
  });
});
