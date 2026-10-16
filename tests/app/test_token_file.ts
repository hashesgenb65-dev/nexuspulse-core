/**
 * Tests for the token file loader and writer.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { TokenAuth, generateSecret, roleSatisfies } from '../../src/api/auth.js';
import {
  loadTokenFile,
  MAX_TOKEN_FILE_BYTES,
  newTokenSpec,
  TokenFileError,
  writeTokenFile,
  type TokenFileReason,
} from '../../src/app/token_file.js';

const dirs: string[] = [];
const isPosix = process.platform !== 'win32';

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nexuspulse-tokens-'));
  dirs.push(dir);
  return dir;
}

function fileWith(content: string, mode = 0o600): string {
  const file = path.join(tempDir(), 'tokens.json');
  fs.writeFileSync(file, content, { mode });
  if (isPosix) fs.chmodSync(file, mode);
  return file;
}

function validSpecs(): { name: string; secret: string; role: 'reader' | 'writer' }[] {
  return [
    { name: 'ci-reader', secret: generateSecret(), role: 'reader' },
    { name: 'ops-writer', secret: generateSecret(), role: 'writer' },
  ];
}

function reasonOf(fn: () => unknown): TokenFileReason {
  try {
    fn();
  } catch (err) {
    if (err instanceof TokenFileError) return err.reason;
    throw err;
  }
  throw new Error('expected a TokenFileError');
}

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe('loadTokenFile', () => {
  it('loads a valid file into a working authenticator', () => {
    const specs = validSpecs();
    const file = fileWith(JSON.stringify(specs));
    const auth = loadTokenFile(file);
    expect(auth).toBeInstanceOf(TokenAuth);
    expect(auth.size).toBe(2);
    expect(auth.authenticateHeader(`Bearer ${specs[1]?.secret ?? ''}`)).toEqual({ name: 'ops-writer', role: 'writer' });
  });

  it('reports a missing file', () => {
    expect(reasonOf(() => loadTokenFile(path.join(tempDir(), 'absent.json')))).toBe('missing');
  });

  it('reports a directory as not a regular file', () => {
    expect(reasonOf(() => loadTokenFile(tempDir()))).toBe('not-a-file');
  });

  it('refuses a file over the size limit before reading it', () => {
    const file = fileWith('x'.repeat(MAX_TOKEN_FILE_BYTES + 1));
    expect(reasonOf(() => loadTokenFile(file))).toBe('too-large');
  });

  it('reports malformed JSON', () => {
    expect(reasonOf(() => loadTokenFile(fileWith('[not json')))).toBe('malformed-json');
  });

  it('reports every invalid token together', () => {
    const file = fileWith(JSON.stringify([{ name: 'Bad Name', secret: 'short', role: 'owner' }]));
    try {
      loadTokenFile(file);
      expect.unreachable('expected TokenFileError');
    } catch (err) {
      const e = err as TokenFileError;
      expect(e.reason).toBe('invalid-tokens');
      expect(e.issues.length).toBeGreaterThanOrEqual(3);
      expect(e.issues[0]).toContain('Bad Name');
    }
  });

  it('reports a file that is not an array', () => {
    expect(reasonOf(() => loadTokenFile(fileWith('{"name":"x"}')))).toBe('invalid-tokens');
  });

  it.skipIf(!isPosix)('refuses a file that group or others can read', () => {
    const file = fileWith(JSON.stringify(validSpecs()), 0o644);
    expect(reasonOf(() => loadTokenFile(file))).toBe('insecure-permissions');
  });

  it.skipIf(!isPosix)('names the mode in the insecure-permissions message', () => {
    const file = fileWith(JSON.stringify(validSpecs()), 0o640);
    try {
      loadTokenFile(file);
      expect.unreachable('expected TokenFileError');
    } catch (err) {
      expect((err as Error).message).toContain('mode 640');
      expect((err as Error).message).toContain('chmod 600');
    }
  });

  it.skipIf(!isPosix)('accepts an insecure file when explicitly allowed', () => {
    const file = fileWith(JSON.stringify(validSpecs()), 0o644);
    expect(loadTokenFile(file, { allowInsecurePermissions: true }).size).toBe(2);
  });

  it.skipIf(!isPosix)('accepts owner-only permissions of 0400', () => {
    const file = fileWith(JSON.stringify(validSpecs()), 0o400);
    expect(loadTokenFile(file).size).toBe(2);
  });

  it('gives each TokenFileError a stable code', () => {
    try {
      loadTokenFile(path.join(tempDir(), 'absent.json'));
    } catch (err) {
      expect((err as TokenFileError).code).toBe('TOKEN_FILE_INVALID');
      expect((err as TokenFileError).name).toBe('TokenFileError');
    }
  });

  it('exposes roles that the loaded tokens can be checked against', () => {
    const file = fileWith(JSON.stringify(validSpecs()));
    const auth = loadTokenFile(file);
    const secret = (JSON.parse(fs.readFileSync(file, 'utf8')) as { secret: string }[])[0]?.secret ?? '';
    const principal = auth.verifySecret(secret);
    expect(principal?.role).toBe('reader');
    expect(roleSatisfies(principal?.role ?? 'reader', 'writer')).toBe(false);
  });
});

describe('newTokenSpec', () => {
  it('creates a spec with a generated secret and the requested role', () => {
    const spec = newTokenSpec('new-one', 'writer');
    expect(spec.name).toBe('new-one');
    expect(spec.role).toBe('writer');
    expect(spec.secret.length).toBeGreaterThanOrEqual(24);
  });

  it('creates a different secret each time', () => {
    expect(newTokenSpec('a', 'reader').secret).not.toBe(newTokenSpec('a', 'reader').secret);
  });
});

describe('writeTokenFile', () => {
  it('writes a file that loads back with the same tokens', () => {
    const dir = tempDir();
    const file = path.join(dir, 'tokens.json');
    const specs = [newTokenSpec('ci', 'reader'), newTokenSpec('ops', 'writer')];
    writeTokenFile(file, specs);
    const auth = loadTokenFile(file);
    expect(auth.size).toBe(2);
    expect(auth.verifySecret(specs[0]?.secret ?? '')?.name).toBe('ci');
  });

  it.skipIf(!isPosix)('creates the file with mode 0600', () => {
    const file = path.join(tempDir(), 'tokens.json');
    writeTokenFile(file, [newTokenSpec('ci', 'reader')]);
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
  });

  it('refuses to overwrite an existing file', () => {
    const file = fileWith('[]');
    expect(() => writeTokenFile(file, [newTokenSpec('ci', 'reader')])).toThrow('already exists');
    expect(fs.readFileSync(file, 'utf8')).toBe('[]');
  });

  it('refuses to write invalid tokens and writes nothing', () => {
    const file = path.join(tempDir(), 'tokens.json');
    expect(() => writeTokenFile(file, [{ name: 'Bad', secret: 'x', role: 'reader' }])).toThrow(TokenFileError);
    expect(fs.existsSync(file)).toBe(false);
  });

  it('refuses an empty token list', () => {
    const file = path.join(tempDir(), 'tokens.json');
    expect(() => writeTokenFile(file, [])).toThrow('at least one token is required');
    expect(fs.existsSync(file)).toBe(false);
  });

  it('writes a trailing newline and pretty-printed JSON', () => {
    const file = path.join(tempDir(), 'tokens.json');
    writeTokenFile(file, [newTokenSpec('ci', 'reader')]);
    const text = fs.readFileSync(file, 'utf8');
    expect(text.endsWith('\n')).toBe(true);
    expect(text).toContain('\n  ');
  });
});
