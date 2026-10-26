/**
 * AuthManager.reload: keys and sessions are replaced with what the file holds now. A file that fails its
 * checks changes nothing, and a manager with no store cannot reload.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { AuthStateError, FileAuthStateStore } from '../../src/auth/auth_state_store';
import { AuthManager, type ScopePermission } from '../../src/auth/manager';

const scopes: ScopePermission[] = [{ resource: 'metrics', actions: ['read'] }];
const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function stateFile(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'session-reload-'));
  dirs.push(dir);
  return path.join(dir, 'auth-state.json');
}

describe('AuthManager.reload', () => {
  it('picks up a key revoked and a key created by another writer', () => {
    const file = stateFile();
    const member = new AuthManager({ stateStore: new FileAuthStateStore(file) });
    const old = member.createApiKey('t1', 'READ_ONLY', scopes);

    // Another writer, such as a restored backup, changes the file underneath the member.
    const other = new AuthManager({ stateStore: new FileAuthStateStore(file) });
    other.revokeApiKey(old.keyId);
    const fresh = other.createApiKey('t2', 'ANALYST', scopes);

    // Before the reload, the member refuses to save over the changed file, so it cannot undo the revocation.
    expect(() => member.authenticateApiKey(old.rawApiKey)).toThrow(AuthStateError);
    member.reload();
    expect(member.authenticateApiKey(old.rawApiKey)).toBeNull();
    expect(member.authenticateApiKey(fresh.rawApiKey)).toMatchObject({ tenantId: 't2' });
  });

  it('drops sessions that were revoked in the file, and keeps ones that are still there', () => {
    const file = stateFile();
    const member = new AuthManager({ stateStore: new FileAuthStateStore(file) });
    const key = member.createApiKey('t1', 'ANALYST', scopes);
    const kept = member.authenticateApiKey(key.rawApiKey)!;
    const dropped = member.authenticateApiKey(key.rawApiKey)!;

    const other = new AuthManager({ stateStore: new FileAuthStateStore(file) });
    other.revokeSession(dropped.sessionId);

    member.reload();
    expect(member.validateSession(kept.sessionId)).not.toBeNull();
    expect(member.validateSession(dropped.sessionId)).toBeNull();
  });

  it('keeps the current state when the file fails its checks', () => {
    const file = stateFile();
    const member = new AuthManager({ stateStore: new FileAuthStateStore(file) });
    const key = member.createApiKey('t1', 'ANALYST', scopes);

    fs.writeFileSync(file, '{ not json', { mode: 0o600 });
    expect(() => member.reload()).toThrow(AuthStateError);
    // The key is still held in memory: the save is refused, which only happens for a live key.
    expect(() => member.authenticateApiKey(key.rawApiKey)).toThrow(AuthStateError);
    expect(fs.readFileSync(file, 'utf8')).toBe('{ not json');
  });

  it('refuses to reload when there is no store', () => {
    expect(() => new AuthManager().reload()).toThrow(AuthStateError);
  });
});
