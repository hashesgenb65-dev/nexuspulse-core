/**
 * Reloading a token file: a good file swaps the tokens, and a bad file (wrong mode, malformed,
 * invalid tokens) leaves the tokens that were in use untouched.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { generateSecret } from '../../src/api/auth';
import { loadRotatingTokenFile, reloadTokenFile, TokenFileError, writeTokenFile, newTokenSpec } from '../../src/app/token_file';

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function fileIn(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'token-rotation-'));
  dirs.push(dir);
  return path.join(dir, 'tokens.json');
}

describe('reloadTokenFile', () => {
  it('swaps in the tokens from a rewritten file, and reports the change', () => {
    const file = fileIn();
    const ops = newTokenSpec('ops', 'writer');
    writeTokenFile(file, [ops]);
    const auth = loadRotatingTokenFile(file, { graceMs: 60_000 });

    const rotated = { ...ops, secret: generateSecret() };
    fs.writeFileSync(file, JSON.stringify([rotated]), { mode: 0o600 });
    const report = reloadTokenFile(auth, file);

    expect(report.rotated).toEqual(['ops']);
    expect(auth.verifySecret(rotated.secret)).toEqual({ name: 'ops', role: 'writer' });
    expect(auth.verifySecret(ops.secret)).toEqual({ name: 'ops', role: 'writer' });
  });

  it('keeps the current tokens when the file has become world-readable', () => {
    const file = fileIn();
    const ops = newTokenSpec('ops', 'writer');
    writeTokenFile(file, [ops]);
    const auth = loadRotatingTokenFile(file);

    fs.writeFileSync(file, JSON.stringify([newTokenSpec('ops', 'reader')]));
    fs.chmodSync(file, 0o644);

    expect(() => reloadTokenFile(auth, file)).toThrow(TokenFileError);
    expect(auth.verifySecret(ops.secret)).toEqual({ name: 'ops', role: 'writer' });
  });

  it('keeps the current tokens when the file is not valid JSON', () => {
    const file = fileIn();
    const ops = newTokenSpec('ops', 'writer');
    writeTokenFile(file, [ops]);
    const auth = loadRotatingTokenFile(file);

    fs.writeFileSync(file, '[{"name": ', { mode: 0o600 });

    let reason: string | undefined;
    try {
      reloadTokenFile(auth, file);
    } catch (error) {
      reason = (error as TokenFileError).reason;
    }
    expect(reason).toBe('malformed-json');
    expect(auth.verifySecret(ops.secret)).toEqual({ name: 'ops', role: 'writer' });
  });

  it('keeps the current tokens when the file holds an invalid token list', () => {
    const file = fileIn();
    const ops = newTokenSpec('ops', 'writer');
    writeTokenFile(file, [ops]);
    const auth = loadRotatingTokenFile(file);

    fs.writeFileSync(file, JSON.stringify([{ name: 'ops', secret: 'short', role: 'writer' }]), { mode: 0o600 });

    expect(() => reloadTokenFile(auth, file)).toThrow(TokenFileError);
    expect(auth.size).toBe(1);
    expect(auth.verifySecret(ops.secret)).toEqual({ name: 'ops', role: 'writer' });
  });

  it('reports a missing file as missing and keeps the current tokens', () => {
    const file = fileIn();
    const ops = newTokenSpec('ops', 'writer');
    writeTokenFile(file, [ops]);
    const auth = loadRotatingTokenFile(file);

    fs.rmSync(file);
    let reason: string | undefined;
    try {
      reloadTokenFile(auth, file);
    } catch (error) {
      reason = (error as TokenFileError).reason;
    }
    expect(reason).toBe('missing');
    expect(auth.verifySecret(ops.secret)).toBeDefined();
  });

  it('refuses an empty token list and keeps the current tokens', () => {
    const file = fileIn();
    writeTokenFile(file, [newTokenSpec('ops', 'writer')]);
    const auth = loadRotatingTokenFile(file);

    fs.writeFileSync(file, '[]', { mode: 0o600 });
    expect(() => reloadTokenFile(auth, file)).toThrow(TokenFileError);
    expect(auth.size).toBe(1);
  });
});
