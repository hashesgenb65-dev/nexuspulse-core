/**
 * The apikey command: create shows a secret that really logs in, list never shows a secret, revoke ends
 * a key, and a state file locked by a running member is never written to.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { runApiKeyCli, parseScope, type ApiKeyCliIo, EXIT_OK, EXIT_FAILURE, EXIT_USAGE } from '../../src/cli/api_key_cli';
import { AuthManager } from '../../src/auth/manager';
import { FileAuthStateStore } from '../../src/auth/auth_state_store';
import { acquireStateLock, lockPathFor } from '../../src/auth/state_lock';

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function stateFile(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'apikey-cli-'));
  dirs.push(dir);
  return path.join(dir, 'auth-state.json');
}

function capture(): { io: ApiKeyCliIo; out: string[]; err: string[] } {
  const out: string[] = [];
  const err: string[] = [];
  return { io: { write: (line) => out.push(line), error: (line) => err.push(line) }, out, err };
}

function run(argv: string[]): { code: number; out: string[]; err: string[] } {
  const c = capture();
  const code = runApiKeyCli(argv, c.io);
  return { code, out: c.out, err: c.err };
}

describe('create', () => {
  it('prints a key that logs in, and the key id', () => {
    const file = stateFile();
    const result = run(['create', file, 'tenant-a', 'INGEST_SERVICE', 'metrics:read,write']);
    expect(result.code).toBe(EXIT_OK);

    const keyId = result.out.find((line) => line.startsWith('key id: '))!.slice('key id: '.length);
    const rawApiKey = result.out.find((line) => line.startsWith('api key: '))!.slice('api key: '.length);
    expect(rawApiKey.startsWith(`${keyId}.`)).toBe(true);

    const manager = new AuthManager({ stateStore: new FileAuthStateStore(file) });
    const session = manager.authenticateApiKey(rawApiKey);
    expect(session).toMatchObject({ tenantId: 'tenant-a', role: 'INGEST_SERVICE' });
  });

  it('writes the file owner-only and leaves no lock behind', () => {
    const file = stateFile();
    run(['create', file, 'tenant-a', 'READ_ONLY', 'metrics:read']);
    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    expect(fs.existsSync(lockPathFor(file))).toBe(false);
  });

  it('applies the rate limit and the expiry flags', () => {
    const file = stateFile();
    run(['create', file, 't', 'ANALYST', 'metrics:read', '--rate-limit', '50', '--ttl-seconds', '600']);
    const saved = new FileAuthStateStore(file).load();
    expect(saved.apiKeys[0]?.rateLimitPerMin).toBe(50);
    expect(saved.apiKeys[0]?.expiresAt).not.toBeNull();
  });

  it('refuses an unknown role, a bad scope and a bad flag with a usage exit code', () => {
    const file = stateFile();
    expect(run(['create', file, 't', 'ROOT', 'metrics:read']).code).toBe(EXIT_USAGE);
    expect(run(['create', file, 't', 'ANALYST', 'metrics']).code).toBe(EXIT_USAGE);
    expect(run(['create', file, 't', 'ANALYST', 'metrics:fly']).code).toBe(EXIT_USAGE);
    expect(run(['create', file, 't', 'ANALYST', 'metrics:read', '--rate-limit', '0']).code).toBe(EXIT_USAGE);
    expect(fs.existsSync(file)).toBe(false);
  });

  it('refuses to write while the state file is locked, and changes nothing', () => {
    const file = stateFile();
    const release = acquireStateLock(file);
    try {
      const result = run(['create', file, 't', 'ANALYST', 'metrics:read']);
      expect(result.code).toBe(EXIT_FAILURE);
      expect(result.err[0]).toMatch(/locked by another process/);
      expect(fs.existsSync(file)).toBe(false);
    } finally {
      release();
    }
  });

  it('fails cleanly when the directory does not exist', () => {
    const file = path.join(stateFile(), 'missing', 'auth-state.json');
    expect(run(['create', file, 't', 'ANALYST', 'metrics:read']).code).toBe(EXIT_FAILURE);
  });
});

describe('list', () => {
  it('shows keys without their secrets or hashes', () => {
    const file = stateFile();
    const created = run(['create', file, 'tenant-a', 'ANALYST', 'metrics:read']);
    const rawApiKey = created.out.find((line) => line.startsWith('api key: '))!.slice('api key: '.length);
    const secret = rawApiKey.split('.')[1]!;

    const listed = run(['list', file]);
    expect(listed.code).toBe(EXIT_OK);
    const text = listed.out.join('\n');
    expect(text).toContain('tenant=tenant-a');
    expect(text).toContain('role=ANALYST');
    expect(text).toContain('active');
    expect(text).not.toContain(secret);
    expect(text).not.toContain('scrypt$');
  });

  it('says so when there are no keys, including when the file does not exist yet', () => {
    expect(run(['list', stateFile()]).out).toEqual(['no keys']);
  });

  it('can be used while the state file is locked', () => {
    const file = stateFile();
    run(['create', file, 't', 'ANALYST', 'metrics:read']);
    const release = acquireStateLock(file);
    try {
      expect(run(['list', file]).code).toBe(EXIT_OK);
    } finally {
      release();
    }
  });
});

describe('revoke', () => {
  it('ends the key, so it no longer logs in', () => {
    const file = stateFile();
    const created = run(['create', file, 't', 'ANALYST', 'metrics:read']);
    const keyId = created.out.find((line) => line.startsWith('key id: '))!.slice('key id: '.length);
    const rawApiKey = created.out.find((line) => line.startsWith('api key: '))!.slice('api key: '.length);

    expect(run(['revoke', file, keyId]).code).toBe(EXIT_OK);
    const manager = new AuthManager({ stateStore: new FileAuthStateStore(file) });
    expect(manager.authenticateApiKey(rawApiKey)).toBeNull();
  });

  it('reports an unknown key id with exit code 1', () => {
    const file = stateFile();
    run(['create', file, 't', 'ANALYST', 'metrics:read']);
    const result = run(['revoke', file, 'np_id_0000000000000000']);
    expect(result.code).toBe(EXIT_FAILURE);
    expect(result.err[0]).toMatch(/no key with id/);
  });

  it('refuses while the state file is locked', () => {
    const file = stateFile();
    const created = run(['create', file, 't', 'ANALYST', 'metrics:read']);
    const keyId = created.out.find((line) => line.startsWith('key id: '))!.slice('key id: '.length);
    const release = acquireStateLock(file);
    try {
      expect(run(['revoke', file, keyId]).code).toBe(EXIT_FAILURE);
    } finally {
      release();
    }
  });
});

describe('usage', () => {
  it('prints usage for no arguments or an unknown command, with a usage exit code', () => {
    expect(run([]).code).toBe(EXIT_USAGE);
    expect(run(['sing']).code).toBe(EXIT_USAGE);
    expect(run(['help']).code).toBe(EXIT_OK);
  });
});

describe('parseScope', () => {
  it('reads resource:action,action and rejects anything else', () => {
    expect(parseScope('metrics:read,write')).toEqual({ resource: 'metrics', actions: ['read', 'write'] });
    expect(parseScope(':read')).toMatch(/resource:action/);
    expect(parseScope('metrics:')).toMatch(/resource:action/);
    expect(parseScope('metrics:read,jump')).toMatch(/unknown action "jump"/);
  });
});
