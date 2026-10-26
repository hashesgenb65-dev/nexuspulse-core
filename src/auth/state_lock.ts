/**
 * A lock file next to the auth state file, held while a mesh member runs and while the key command
 * writes. Only one of them may change the file at a time, so a key created while a member is running
 * cannot be lost when the member saves its own copy.
 *
 * The lock is created exclusively and holds the owner's process id and start time, so the message for
 * a held lock says what holds it. A lock left behind by a crash is not removed automatically; the
 * operator removes it after checking that no member is running.
 */

import * as fs from 'node:fs';

/** Raised when the state file is locked by another process. Code: AUTH_STATE_LOCKED. */
export class StateLockError extends Error {
  readonly code = 'AUTH_STATE_LOCKED' as const;

  constructor(message: string) {
    super(message);
    this.name = 'StateLockError';
  }
}

export function lockPathFor(stateFile: string): string {
  return `${stateFile}.lock`;
}

/**
 * Takes the lock for `stateFile` and returns the function that releases it. Releasing twice is safe.
 *
 * @throws {StateLockError} If the lock is already held.
 */
export function acquireStateLock(stateFile: string): () => void {
  const lockPath = lockPathFor(stateFile);
  let fd: number;
  try {
    fd = fs.openSync(lockPath, 'wx', 0o600);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
    throw new StateLockError(`${stateFile} is locked by another process (${describeHolder(lockPath)}); remove ${lockPath} only if no member is running`);
  }
  fs.writeSync(fd, `${JSON.stringify({ pid: process.pid, startedAt: new Date().toISOString() })}\n`);
  fs.closeSync(fd);

  let released = false;
  return (): void => {
    if (released) return;
    released = true;
    fs.rmSync(lockPath, { force: true });
  };
}

function describeHolder(lockPath: string): string {
  try {
    const holder = JSON.parse(fs.readFileSync(lockPath, 'utf8')) as { pid?: unknown; startedAt?: unknown };
    return `pid ${String(holder.pid)}, since ${String(holder.startedAt)}`;
  } catch {
    return 'unknown holder';
  }
}
