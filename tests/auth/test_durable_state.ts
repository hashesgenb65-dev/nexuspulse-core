/**
 * API keys and sessions kept in a file: they survive a restart, revocations and expiry still hold
 * after one, the file is owner-only, and a damaged or unwritable file stops the work it guards.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { AuthManager, type ScopePermission } from '../../src/auth/manager';
import { AuthStateError, FileAuthStateStore } from '../../src/auth/auth_state_store';

const HOUR = 60 * 60 * 1000;
const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];
const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function stateFile(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'auth-state-'));
  dirs.push(dir);
  return path.join(dir, 'auth-state.json');
}

function open(file: string, clock: { now: number }): AuthManager {
  return new AuthManager({ now: () => clock.now, stateStore: new FileAuthStateStore(file) });
}

describe('restart', () => {
  it('keeps an API key and its session usable after a restart', () => {
    const file = stateFile();
    const clock = { now: 1_700_000_000_000 };
    const first = open(file, clock);
    const { rawApiKey } = first.createApiKey('t1', 'ANALYST', scopes);
    const session = first.authenticateApiKey(rawApiKey);
    expect(session).not.toBeNull();

    const second = open(file, clock);
    expect(second.validateSession(session!.sessionId)).toMatchObject({ tenantId: 't1', role: 'ANALYST' });
    expect(second.authenticateApiKey(rawApiKey)).not.toBeNull();
  });

  it('keeps a revoked key revoked, and ends its sessions, after a restart', () => {
    const file = stateFile();
    const clock = { now: 1_700_000_000_000 };
    const first = open(file, clock);
    const { keyId, rawApiKey } = first.createApiKey('t1', 'ANALYST', scopes);
    const session = first.authenticateApiKey(rawApiKey);
    first.revokeApiKey(keyId);

    const second = open(file, clock);
    expect(second.authenticateApiKey(rawApiKey)).toBeNull();
    expect(second.validateSession(session!.sessionId)).toBeNull();
  });

  it('does not restore sessions that expired while the node was down', () => {
    const file = stateFile();
    const clock = { now: 1_700_000_000_000 };
    const first = open(file, clock);
    const { rawApiKey } = first.createApiKey('t1', 'ANALYST', scopes);
    const session = first.authenticateApiKey(rawApiKey);

    clock.now += HOUR + 1;
    const second = open(file, clock);
    expect(second.validateSession(session!.sessionId)).toBeNull();
    expect(second.sessionCount).toBe(0);
  });

  it('refuses a session whose API key is missing from the file', () => {
    const file = stateFile();
    const clock = { now: 1_700_000_000_000 };
    const first = open(file, clock);
    const { keyId, rawApiKey } = first.createApiKey('t1', 'ANALYST', scopes);
    const session = first.authenticateApiKey(rawApiKey);

    const saved = JSON.parse(fs.readFileSync(file, 'utf8')) as { apiKeys: Array<{ keyId: string }> };
    saved.apiKeys = saved.apiKeys.filter((key) => key.keyId !== keyId);
    fs.writeFileSync(file, JSON.stringify(saved), { mode: 0o600 });

    const second = open(file, clock);
    expect(second.validateSession(session!.sessionId)).toBeNull();
  });

  it('persists a session revocation', () => {
    const file = stateFile();
    const clock = { now: 1_700_000_000_000 };
    const first = open(file, clock);
    const { rawApiKey } = first.createApiKey('t1', 'ANALYST', scopes);
    const session = first.authenticateApiKey(rawApiKey);
    expect(first.revokeSession(session!.sessionId)).toBe(true);

    const second = open(file, clock);
    expect(second.validateSession(session!.sessionId)).toBeNull();
  });
});

describe('the file', () => {
  it('is owner-only and leaves no temporary file behind', () => {
    const file = stateFile();
    const manager = open(file, { now: 1_700_000_000_000 });
    manager.createApiKey('t1', 'ANALYST', scopes);

    expect(fs.statSync(file).mode & 0o777).toBe(0o600);
    const leftovers = fs.readdirSync(path.dirname(file)).filter((name) => name.endsWith('.tmp'));
    expect(leftovers).toEqual([]);
  });

  it('refuses a file that group or others can read', () => {
    const file = stateFile();
    open(file, { now: 1_700_000_000_000 }).createApiKey('t1', 'ANALYST', scopes);
    fs.chmodSync(file, 0o644);

    expect(() => open(file, { now: 1_700_000_000_000 })).toThrow(AuthStateError);
  });

  it('refuses a file that is not valid JSON', () => {
    const file = stateFile();
    fs.writeFileSync(file, '{"version": 1,', { mode: 0o600 });
    expect(() => open(file, { now: 1_700_000_000_000 })).toThrow(/not valid JSON/);
  });

  it('refuses a file with a malformed session, and names the session', () => {
    const file = stateFile();
    fs.writeFileSync(file, JSON.stringify({ version: 1, apiKeys: [], sessions: [{ sessionId: 'nope' }] }), {
      mode: 0o600,
    });
    expect(() => open(file, { now: 1_700_000_000_000 })).toThrow(/session 0 .* malformed/);
  });

  it('refuses a file with a key whose role is unknown', () => {
    const file = stateFile();
    const clock = { now: 1_700_000_000_000 };
    open(file, clock).createApiKey('t1', 'ANALYST', scopes);
    const saved = JSON.parse(fs.readFileSync(file, 'utf8')) as { apiKeys: Array<{ role: string }> };
    saved.apiKeys[0]!.role = 'SUPERUSER';
    fs.writeFileSync(file, JSON.stringify(saved), { mode: 0o600 });

    expect(() => open(file, clock)).toThrow(/api key 0 .* malformed/);
  });

  it('rejects an empty path', () => {
    expect(() => new FileAuthStateStore('')).toThrow(AuthStateError);
  });
});

describe('a failed write', () => {
  it('issues no key and no session when the state cannot be saved', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'auth-state-'));
    dirs.push(dir);
    const manager = new AuthManager({
      now: () => 1_700_000_000_000,
      stateStore: new FileAuthStateStore(path.join(dir, 'missing-dir', 'auth-state.json')),
    });

    expect(() => manager.createApiKey('t1', 'ANALYST', scopes)).toThrow();
    expect(manager.sessionCount).toBe(0);
  });
});
