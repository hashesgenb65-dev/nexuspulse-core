/**
 * TLS credentials for the TCP ingest port.
 *
 * The ingest accepts telemetry from edge devices over the network. Without TLS, tokens and events
 * cross the wire in clear text. This module loads a PEM certificate chain and private key from
 * disk, and refuses a private key that other users on the host can read.
 */

import * as fs from 'node:fs';

/** Paths to a PEM certificate chain and its private key. */
export interface TlsFiles {
  readonly certPath: string;
  readonly keyPath: string;
}

/** PEM contents, ready to pass to `tls.createServer`. */
export interface TlsCredentials {
  readonly cert: Buffer;
  readonly key: Buffer;
}

/** Oldest TLS version the ingest will negotiate. TLS 1.0 and 1.1 are refused. */
export const MIN_TLS_VERSION = 'TLSv1.2' as const;

/** Bits of a file mode that grant access to group or other users. */
const GROUP_OR_OTHER_BITS = 0o077;

/** Thrown when the TLS files are missing, unreadable, or unsafe. The message never includes key bytes. */
export class TlsConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TlsConfigError';
  }
}

function readPemFile(filePath: string, label: string): Buffer {
  let contents: Buffer;
  try {
    contents = fs.readFileSync(filePath);
  } catch {
    throw new TlsConfigError(`cannot read the ${label} file`);
  }
  const text = contents.toString('latin1');
  if (!text.includes('-----BEGIN ')) {
    throw new TlsConfigError(`the ${label} file is not PEM encoded`);
  }
  return contents;
}

/**
 * Loads the certificate and key. Throws {@link TlsConfigError} when a file is missing, is not PEM,
 * or when the key file's mode gives group or other users any access.
 */
export function loadTlsCredentials(files: TlsFiles): TlsCredentials {
  if (files.certPath.length === 0 || files.keyPath.length === 0) {
    throw new TlsConfigError('certPath and keyPath must be non-empty');
  }

  let keyStat: fs.Stats;
  try {
    keyStat = fs.statSync(files.keyPath);
  } catch {
    throw new TlsConfigError('cannot read the key file');
  }
  if ((keyStat.mode & GROUP_OR_OTHER_BITS) !== 0) {
    throw new TlsConfigError('the key file must not be readable by group or other users (use mode 0600)');
  }

  const cert = readPemFile(files.certPath, 'certificate');
  const key = readPemFile(files.keyPath, 'key');
  return { cert, key };
}

/** Options for `tls.createServer` built from loaded credentials. */
export function tlsServerOptions(credentials: TlsCredentials): {
  readonly cert: Buffer;
  readonly key: Buffer;
  readonly minVersion: typeof MIN_TLS_VERSION;
} {
  return { cert: credentials.cert, key: credentials.key, minVersion: MIN_TLS_VERSION };
}
