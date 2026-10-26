/**
 * Accepts either a token from the token file or a live session from the auth manager. A session is
 * issued by logging in with an API key, and its id is then sent as the bearer token.
 *
 * Roles: the API has two, reader and writer. An auth-manager role maps onto them as follows:
 * ADMIN and INGEST_SERVICE are writers; ANALYST and READ_ONLY are readers. The key's scopes are not
 * checked by the key-value API, so a session gives the same access as its role and nothing narrower.
 *
 * A token from the token file is checked first. Its name is used as the principal name; for a
 * session the principal name is the API key id.
 */

import { authenticateWith, guardWith, type Authenticator, type Principal, type Role } from './auth.js';
import type { AuthManager, UserRole } from '../auth/manager.js';
import type { Handler } from './http_router.js';

/** Path where a client logs in with an API key. */
export const SESSION_LOGIN_PATH = '/v1/sessions';

const WRITER_ROLES: readonly UserRole[] = ['ADMIN', 'INGEST_SERVICE'];

/** The API role for an auth-manager role. */
export function apiRoleOf(role: UserRole): Role {
  return WRITER_ROLES.includes(role) ? 'writer' : 'reader';
}

export class SessionAuthenticator implements Authenticator {
  private readonly tokens: Authenticator | undefined;
  private readonly sessions: AuthManager;

  constructor(tokens: Authenticator | undefined, sessions: AuthManager) {
    this.tokens = tokens;
    this.sessions = sessions;
  }

  get size(): number {
    return this.tokens?.size ?? 0;
  }

  describe(): readonly { readonly name: string; readonly role: Role }[] {
    return this.tokens?.describe() ?? [];
  }

  verifySecret(secret: string): Principal | undefined {
    const fromToken = this.tokens?.verifySecret(secret);
    if (fromToken !== undefined) return fromToken;
    const session = this.sessions.validateSession(secret);
    if (session === null) return undefined;
    return { name: session.keyId, role: apiRoleOf(session.role) };
  }

  authenticateHeader(header: string | undefined): Principal {
    return authenticateWith((secret) => this.verifySecret(secret), header);
  }

  guard(minimum: Role, handler: Handler): Handler {
    return guardWith((header) => this.authenticateHeader(header), minimum, handler);
  }
}
