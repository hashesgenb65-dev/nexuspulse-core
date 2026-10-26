/**
 * Command-line helper for API keys stored in an auth state file.
 *
 *   apikey create <state-file> <tenant> <role> <resource>:<action>[,<action>...] [--rate-limit N] [--ttl-seconds N]
 *   apikey revoke <state-file> <key-id>
 *   apikey list <state-file>
 *   apikey help
 *
 * Roles: ADMIN, ANALYST, INGEST_SERVICE, READ_ONLY. Actions: read, write, delete, admin.
 *
 * `create` prints the secret once, and it is never shown again. `create` and `revoke` take the state
 * file's lock, so they refuse to run while a mesh member that uses the file is running.
 *
 * Exit codes: 0 success, 1 failure (locked file, unknown key, unreadable file), 2 bad usage.
 */

import { AuthManager, type ApiKeyRecord, type ScopePermission, type UserRole } from '../auth/manager.js';
import { AuthStateError, FileAuthStateStore } from '../auth/auth_state_store.js';
import { acquireStateLock, StateLockError } from '../auth/state_lock.js';

export interface ApiKeyCliIo {
  write(line: string): void;
  error(line: string): void;
}

export const EXIT_OK = 0;
export const EXIT_FAILURE = 1;
export const EXIT_USAGE = 2;

export const USER_ROLES: readonly UserRole[] = ['ADMIN', 'ANALYST', 'INGEST_SERVICE', 'READ_ONLY'];
const ACTIONS: readonly ScopePermission['actions'][number][] = ['read', 'write', 'delete', 'admin'];

export const USAGE = [
  'usage:',
  '  apikey create <state-file> <tenant> <role> <resource>:<action>[,<action>...] [--rate-limit N] [--ttl-seconds N]',
  '  apikey revoke <state-file> <key-id>',
  '  apikey list <state-file>',
  '  apikey help',
  `roles: ${USER_ROLES.join(', ')}`,
  `actions: ${ACTIONS.join(', ')}`,
].join('\n');

/** Parses `resource:action,action`. Returns an error message instead of throwing. */
export function parseScope(arg: string): ScopePermission | string {
  const separator = arg.indexOf(':');
  if (separator <= 0 || separator === arg.length - 1) return `scope "${arg}" must look like resource:action`;
  const resource = arg.slice(0, separator);
  const actions = arg.slice(separator + 1).split(',');
  for (const action of actions) {
    if (!ACTIONS.includes(action as ScopePermission['actions'][number])) {
      return `unknown action "${action}" in scope "${arg}"`;
    }
  }
  return { resource, actions: actions as ScopePermission['actions'] };
}

function isUserRole(value: string): value is UserRole {
  return (USER_ROLES as readonly string[]).includes(value);
}

function positiveInteger(value: string): number | undefined {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : undefined;
}

/** Runs `work` with the state file's lock held, and releases it afterwards. */
function withLock<T>(stateFile: string, work: () => T): T {
  const release = acquireStateLock(stateFile);
  try {
    return work();
  } finally {
    release();
  }
}

function commandCreate(args: readonly string[], io: ApiKeyCliIo): number {
  const flags: { rateLimit?: number; ttlSeconds?: number } = {};
  const positional: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg === '--rate-limit' || arg === '--ttl-seconds') {
      const value = positiveInteger(args[i + 1] ?? '');
      if (value === undefined) {
        io.error(`${arg} needs a positive whole number`);
        return EXIT_USAGE;
      }
      if (arg === '--rate-limit') flags.rateLimit = value;
      else flags.ttlSeconds = value;
      i++;
    } else {
      positional.push(arg);
    }
  }
  if (positional.length !== 4) {
    io.error(USAGE);
    return EXIT_USAGE;
  }
  const [stateFile, tenant, role, scopeArg] = positional as [string, string, string, string];
  if (!isUserRole(role)) {
    io.error(`role must be one of ${USER_ROLES.join(', ')}`);
    return EXIT_USAGE;
  }
  const scope = parseScope(scopeArg);
  if (typeof scope === 'string') {
    io.error(scope);
    return EXIT_USAGE;
  }

  try {
    const created = withLock(stateFile, () => {
      const manager = new AuthManager({ stateStore: new FileAuthStateStore(stateFile) });
      return manager.createApiKey(tenant, role, [scope], flags.rateLimit, flags.ttlSeconds);
    });
    io.write(`key id: ${created.keyId}`);
    io.write(`api key: ${created.rawApiKey}`);
    io.write('The api key is shown once. Store it now; it cannot be recovered later.');
    return EXIT_OK;
  } catch (error) {
    return reportFailure(error, io);
  }
}

function commandRevoke(args: readonly string[], io: ApiKeyCliIo): number {
  if (args.length !== 2) {
    io.error(USAGE);
    return EXIT_USAGE;
  }
  const [stateFile, keyId] = args as [string, string];
  try {
    const revoked = withLock(stateFile, () => {
      const manager = new AuthManager({ stateStore: new FileAuthStateStore(stateFile) });
      return manager.revokeApiKey(keyId);
    });
    if (!revoked) {
      io.error(`no key with id ${keyId} in ${stateFile}`);
      return EXIT_FAILURE;
    }
    io.write(`revoked ${keyId}; its sessions have ended`);
    return EXIT_OK;
  } catch (error) {
    return reportFailure(error, io);
  }
}

function commandList(args: readonly string[], io: ApiKeyCliIo): number {
  if (args.length !== 1) {
    io.error(USAGE);
    return EXIT_USAGE;
  }
  const stateFile = args[0]!;
  try {
    // Reading does not take the lock: the file is replaced atomically, so a read sees a whole file.
    const state = new FileAuthStateStore(stateFile).load();
    if (state.apiKeys.length === 0) {
      io.write('no keys');
      return EXIT_OK;
    }
    for (const key of state.apiKeys) io.write(describeKey(key));
    return EXIT_OK;
  } catch (error) {
    return reportFailure(error, io);
  }
}

function describeKey(key: ApiKeyRecord): string {
  const scopes = key.scopes.map((s) => `${s.resource}:${s.actions.join(',')}`).join(' ');
  const state = key.isActive ? 'active' : 'revoked';
  const expires = key.expiresAt === null ? 'no expiry' : `expires ${new Date(key.expiresAt).toISOString()}`;
  return `${key.keyId}  ${state}  tenant=${key.tenantId}  role=${key.role}  scopes=[${scopes}]  ${expires}`;
}

function reportFailure(error: unknown, io: ApiKeyCliIo): number {
  if (error instanceof StateLockError || error instanceof AuthStateError) {
    io.error(error.message);
    return EXIT_FAILURE;
  }
  if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
    io.error('the state file or its directory does not exist');
    return EXIT_FAILURE;
  }
  io.error(`unexpected error: ${error instanceof Error ? error.message : String(error)}`);
  return EXIT_FAILURE;
}

/** Runs one `apikey` command. `argv` excludes the program name. Returns the exit code. */
export function runApiKeyCli(argv: readonly string[], io: ApiKeyCliIo): number {
  const [command, ...rest] = argv;
  switch (command) {
    case 'create':
      return commandCreate(rest, io);
    case 'revoke':
      return commandRevoke(rest, io);
    case 'list':
      return commandList(rest, io);
    case 'help':
    case undefined:
      io.write(USAGE);
      return command === undefined ? EXIT_USAGE : EXIT_OK;
    default:
      io.error(`unknown command "${command}"`);
      io.error(USAGE);
      return EXIT_USAGE;
  }
}
