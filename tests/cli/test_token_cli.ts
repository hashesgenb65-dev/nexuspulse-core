/**
 * Tests for the token command-line helper.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  EXIT_FAILURE,
  EXIT_OK,
  EXIT_USAGE,
  parseTokenArg,
  runTokenCli,
  USAGE,
  type CliIo,
} from '../../src/cli/token_cli.js';
import { loadTokenFile } from '../../src/app/token_file.js';
import { ROLE_NAMES, TokenAuth } from '../../src/api/auth.js';

const isPosix = process.platform !== 'win32';
const dirs: string[] = [];

function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nexuspulse-cli-'));
  dirs.push(dir);
  return dir;
}

interface Captured {
  readonly io: CliIo;
  readonly out: string[];
  readonly err: string[];
}

function capture(): Captured {
  const out: string[] = [];
  const err: string[] = [];
  return {
    out,
    err,
    io: {
      write: (line: string) => void out.push(line),
      error: (line: string) => void err.push(line),
    },
  };
}

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe('parseTokenArg', () => {
  it('parses name=role', () => {
    expect(parseTokenArg('ci-bot=reader')).toEqual({ name: 'ci-bot', role: 'reader' });
  });

  it('splits on the first equals sign only', () => {
    expect(parseTokenArg('a=b=reader')).toBe('role must be one of reader, writer, got "b=reader"');
  });

  it('rejects an argument without an equals sign', () => {
    expect(parseTokenArg('ci-bot')).toBe('expected name=role, got "ci-bot"');
  });

  it('rejects an empty name or an empty role', () => {
    expect(parseTokenArg('=reader')).toBe('expected name=role, got "=reader"');
    expect(parseTokenArg('ci-bot=')).toBe('expected name=role, got "ci-bot="');
  });

  it('rejects an unknown role and names the valid ones', () => {
    expect(parseTokenArg('ci-bot=admin')).toBe('role must be one of reader, writer, got "admin"');
  });
});

describe('help and usage', () => {
  it('prints usage and exits 0 for help', () => {
    const c = capture();
    expect(runTokenCli(['help'], c.io)).toBe(EXIT_OK);
    expect(c.out).toEqual([USAGE]);
  });

  it('accepts --help and -h', () => {
    const c = capture();
    expect(runTokenCli(['--help'], c.io)).toBe(EXIT_OK);
    expect(runTokenCli(['-h'], capture().io)).toBe(EXIT_OK);
  });

  it('exits with a usage error and prints usage when no command is given', () => {
    const c = capture();
    expect(runTokenCli([], c.io)).toBe(EXIT_USAGE);
    expect(c.err).toEqual([USAGE]);
  });

  it('rejects an unknown command', () => {
    const c = capture();
    expect(runTokenCli(['rotate'], c.io)).toBe(EXIT_USAGE);
    expect(c.err[0]).toBe('unknown command "rotate"');
  });

  it('lists every role in the usage text', () => {
    for (const role of ROLE_NAMES) expect(USAGE).toContain(role);
  });
});

describe('token new', () => {
  it('writes a token file that loads back', () => {
    const file = path.join(tempDir(), 'tokens.json');
    const c = capture();
    expect(runTokenCli(['new', file, 'ci-reader=reader', 'ops-writer=writer'], c.io)).toBe(EXIT_OK);

    const auth = loadTokenFile(file);
    expect(auth.size).toBe(2);
    expect(c.out[0]).toBe(`wrote 2 tokens to ${file} (mode 0600)`);
  });

  it('never prints a secret', () => {
    const file = path.join(tempDir(), 'tokens.json');
    const c = capture();
    runTokenCli(['new', file, 'ci-reader=reader'], c.io);
    const secret = (JSON.parse(fs.readFileSync(file, 'utf8')) as { secret: string }[])[0]?.secret ?? 'none';
    expect(secret.length).toBeGreaterThan(20);
    expect([...c.out, ...c.err].join('\n')).not.toContain(secret);
  });

  it.skipIf(!isPosix)('creates the file with mode 0600', () => {
    const file = path.join(tempDir(), 'tokens.json');
    runTokenCli(['new', file, 'ci-reader=reader'], capture().io);
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
  });

  it('uses the singular word for one token', () => {
    const file = path.join(tempDir(), 'tokens.json');
    const c = capture();
    runTokenCli(['new', file, 'solo=reader'], c.io);
    expect(c.out[0]).toContain('wrote 1 token to');
  });

  it('needs a file and at least one token', () => {
    const c = capture();
    expect(runTokenCli(['new'], c.io)).toBe(EXIT_USAGE);
    expect(runTokenCli(['new', path.join(tempDir(), 'x.json')], capture().io)).toBe(EXIT_USAGE);
  });

  it('rejects a bad token argument without writing anything', () => {
    const file = path.join(tempDir(), 'tokens.json');
    const c = capture();
    expect(runTokenCli(['new', file, 'ci-reader=admin'], c.io)).toBe(EXIT_USAGE);
    expect(c.err[0]).toContain('role must be one of');
    expect(fs.existsSync(file)).toBe(false);
  });

  it('refuses to overwrite an existing file', () => {
    const file = path.join(tempDir(), 'tokens.json');
    fs.writeFileSync(file, '[]');
    const c = capture();
    expect(runTokenCli(['new', file, 'ci-reader=reader'], c.io)).toBe(EXIT_FAILURE);
    expect(c.err[0]).toContain('already exists');
    expect(fs.readFileSync(file, 'utf8')).toBe('[]');
  });

  it('reports every invalid name, not just the first', () => {
    const file = path.join(tempDir(), 'tokens.json');
    const c = capture();
    expect(runTokenCli(['new', file, 'Bad=reader', 'Worse=writer'], c.io)).toBe(EXIT_FAILURE);
    expect(c.err.filter((line) => line.startsWith('  ')).length).toBeGreaterThanOrEqual(2);
    expect(fs.existsSync(file)).toBe(false);
  });
});

describe('token check', () => {
  it('reports names and roles of a valid file, without secrets', () => {
    const file = path.join(tempDir(), 'tokens.json');
    runTokenCli(['new', file, 'ci-reader=reader', 'ops-writer=writer'], capture().io);
    const c = capture();
    expect(runTokenCli(['check', file], c.io)).toBe(EXIT_OK);
    expect(c.out[0]).toBe(`ok: 2 tokens in ${file}`);
    expect(c.out).toContain('  ci-reader (reader)');
    expect(c.out).toContain('  ops-writer (writer)');
    const secrets = (JSON.parse(fs.readFileSync(file, 'utf8')) as { secret: string }[]).map((s) => s.secret);
    for (const secret of secrets) expect(c.out.join('\n')).not.toContain(secret);
  });

  it('exits 1 for a missing file', () => {
    const c = capture();
    expect(runTokenCli(['check', path.join(tempDir(), 'absent.json')], c.io)).toBe(EXIT_FAILURE);
    expect(c.err[0]).toContain('does not exist');
  });

  it.skipIf(!isPosix)('exits 1 and explains the fix for a world-readable file', () => {
    const file = path.join(tempDir(), 'tokens.json');
    runTokenCli(['new', file, 'ci-reader=reader'], capture().io);
    fs.chmodSync(file, 0o644);
    const c = capture();
    expect(runTokenCli(['check', file], c.io)).toBe(EXIT_FAILURE);
    expect(c.err[0]).toContain('chmod 600');
  });

  it('needs exactly one file', () => {
    expect(runTokenCli(['check'], capture().io)).toBe(EXIT_USAGE);
    expect(runTokenCli(['check', 'a.json', 'b.json'], capture().io)).toBe(EXIT_USAGE);
  });
});

describe('TokenAuth.describe', () => {
  it('lists names and roles and never the secret', () => {
    const secret = 'describe-secret-0123456789abcdef';
    const auth = new TokenAuth([{ name: 'ci', secret, role: 'writer' }]);
    expect(auth.describe()).toEqual([{ name: 'ci', role: 'writer' }]);
    expect(JSON.stringify(auth.describe())).not.toContain(secret);
  });
});
