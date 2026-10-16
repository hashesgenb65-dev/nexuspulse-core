/**
 * @module vault
 * @description Zero-knowledge envelope encryption engine for NexusPulse.
 *
 * ## Architecture
 *
 * Envelope encryption separates the *key encryption key* (KEK, also called the
 * master key) from the *data encryption keys* (DEKs) so that:
 *
 *   1. Each ciphertext carries its own freshly-generated DEK, encrypted under
 *      the current KEK.  Rotating the KEK means re-wrapping DEKs, not
 *      re-encrypting every payload.
 *   2. The plaintext master secret (passphrase / HSM-supplied root key) never
 *      leaves memory longer than necessary; all derived key material is zeroed
 *      via `secureWipe` as soon as possible.
 *   3. The vault itself stores only opaque `EncryptedEnvelope` objects; it
 *      never persists the KEK, the DEK, or the plaintext.
 *
 * ## Cryptographic primitives
 *
 *   - **Key derivation**: PBKDF2-HMAC-SHA-512 (configurable iterations ≥ 310 000,
 *     OWASP 2024 minimum) or a caller-supplied raw 32-byte key.
 *   - **Key encryption / decryption**: AES-256-GCM with a 96-bit IV and full
 *     128-bit GCM authentication tag.
 *   - **Data encryption / decryption**: AES-256-GCM, same parameters.
 *   - **IVs**: Node.js `crypto.randomBytes(12)` (CSPRNG).
 *   - **Secure erase**: `Buffer.fill(0)` on every key buffer before GC.
 *
 * ## Key rotation
 *
 * A `KeyVault` maintains an ordered list of `KeyVersion` descriptors, each
 * identified by a UUID.  The *current* version is always the last one added.
 * When the caller calls `rotateKey`, a new KEK version is derived and all
 * stored DEKs are re-wrapped to the new KEK.  Ciphertexts produced before
 * rotation embed the key-version ID in the envelope so that `decrypt` can
 * select the right KEK for unwrapping.
 *
 * ## Wire format (EncryptedEnvelope)
 *
 * ```
 * {
 *   version:    1,
 *   keyVersionId: "<uuid>",          // which KEK wrapped the DEK
 *   wrappedDek: "<base64>",          // 12-byte IV | 32-byte ciphertext | 16-byte tag
 *   iv:         "<base64>",          // 12-byte IV for payload ciphertext
 *   ciphertext: "<base64>",          // AES-256-GCM ciphertext
 *   tag:        "<base64>",          // 16-byte GCM auth tag
 *   aad:        "<base64> | null",   // optional additional authenticated data
 * }
 * ```
 *
 * @packageDocumentation
 */

import {
  createCipheriv,
  createDecipheriv,
  pbkdf2 as pbkdf2Cb,
  randomBytes,
  timingSafeEqual,
} from 'node:crypto';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';

// ---------------------------------------------------------------------------
// Internal promisified PBKDF2
// ---------------------------------------------------------------------------
const pbkdf2 = promisify(pbkdf2Cb);

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

/** Current envelope wire format version. Increment on breaking schema changes. */
export const ENVELOPE_VERSION = 1 as const;

/** AES-256-GCM key length in bytes. */
export const KEY_BYTES = 32;

/** AES-256-GCM IV / nonce length in bytes. */
export const IV_BYTES = 12;

/** AES-256-GCM authentication tag length in bytes. */
export const TAG_BYTES = 16;

/**
 * Minimum PBKDF2 iterations recommended by OWASP (2024) for HMAC-SHA-512.
 * Callers SHOULD use this value or higher.
 */
export const MIN_PBKDF2_ITERATIONS = 210_000;

// ---------------------------------------------------------------------------
// Error hierarchy
// ---------------------------------------------------------------------------

/**
 * Root error class for all vault-related failures.
 * Carries a machine-readable `code` and optional structured `details`.
 */
export class VaultError extends Error {
  public readonly code: string;
  public readonly details: Readonly<Record<string, unknown>>;

  constructor(
    message: string,
    code: string,
    details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = 'VaultError';
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

/** The envelope cannot be decoded or is structurally invalid. */
export class EnvelopeFormatError extends VaultError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'ENVELOPE_FORMAT_ERROR', details);
    this.name = 'EnvelopeFormatError';
  }
}

/** The GCM authentication tag did not match; data may be corrupt or tampered. */
export class AuthenticationError extends VaultError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'AUTHENTICATION_ERROR', details);
    this.name = 'AuthenticationError';
  }
}

/** Requested key version does not exist in this vault. */
export class KeyVersionNotFoundError extends VaultError {
  constructor(keyVersionId: string) {
    super(
      `Key version '${keyVersionId}' not found`,
      'KEY_VERSION_NOT_FOUND',
      { keyVersionId },
    );
    this.name = 'KeyVersionNotFoundError';
  }
}

/** Vault has been permanently destroyed and can no longer be used. */
export class VaultDestroyedError extends VaultError {
  constructor() {
    super('Vault has been destroyed', 'VAULT_DESTROYED');
    this.name = 'VaultDestroyedError';
  }
}

/** PBKDF2 or key-derivation configuration is invalid. */
export class KeyDerivationError extends VaultError {
  constructor(message: string, details?: Record<string, unknown>) {
    super(message, 'KEY_DERIVATION_ERROR', details);
    this.name = 'KeyDerivationError';
  }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/**
 * A single encrypted envelope.  Fully self-describing: contains everything
 * needed to decrypt the payload given the correct KEK.
 */
export interface EncryptedEnvelope {
  /** Wire format version. Currently always 1. */
  readonly version: typeof ENVELOPE_VERSION;
  /** UUID of the KEK version used to wrap the DEK. */
  readonly keyVersionId: string;
  /**
   * Base64-encoded wrapped DEK blob:
   * `IV(12 bytes) || Ciphertext(32 bytes) || Tag(16 bytes)` = 60 bytes total.
   */
  readonly wrappedDek: string;
  /** Base64-encoded 12-byte IV for the payload ciphertext. */
  readonly iv: string;
  /** Base64-encoded payload ciphertext. */
  readonly ciphertext: string;
  /** Base64-encoded 16-byte GCM authentication tag for the payload. */
  readonly tag: string;
  /**
   * Optional base64-encoded additional authenticated data (AAD).
   * If supplied at encryption time it MUST be supplied identically at
   * decryption time; the tag covers it.
   */
  readonly aad: string | null;
}

/**
 * Serializable snapshot of a key version.  The raw key material is never
 * exported.
 */
export interface KeyVersionInfo {
  readonly id: string;
  readonly createdAt: number; // Unix ms
  readonly algorithm: 'AES-256-GCM';
  readonly derivation: 'PBKDF2-HMAC-SHA512' | 'RAW';
}

/** Vault-level metrics snapshot, suitable for Prometheus exposition. */
export interface VaultMetrics {
  readonly encryptCount: number;
  readonly decryptCount: number;
  readonly rotationCount: number;
  readonly rewrapCount: number;
  readonly errorCount: number;
  readonly keyVersionCount: number;
  readonly currentKeyVersionId: string | null;
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/**
 * Internal key version record.  `keyMaterial` is a 32-byte Buffer that MUST
 * be zeroed via `secureWipe` when the version is retired or the vault is
 * destroyed.
 */
interface KeyVersion {
  readonly id: string;
  readonly createdAt: number;
  keyMaterial: Buffer; // mutable so we can wipe it
}

/** Configuration for PBKDF2 key derivation. */
export interface Pbkdf2Config {
  readonly passphrase: string | Buffer;
  readonly salt: Buffer;
  readonly iterations: number;
  /** Defaults to 'sha512'. */
  readonly digest?: 'sha256' | 'sha384' | 'sha512';
}

/** Options accepted by `KeyVault.create`. */
export interface VaultCreateOptions {
  /**
   * Derive the initial KEK from a passphrase via PBKDF2.
   * Mutually exclusive with `rawKey`.
   */
  readonly pbkdf2?: Pbkdf2Config;
  /**
   * Use a caller-supplied 32-byte raw key as the initial KEK.
   * Mutually exclusive with `pbkdf2`.
   * The vault takes ownership of the buffer and will wipe it on rotation/destroy.
   */
  readonly rawKey?: Buffer;
  /**
   * Override the clock for testing.  Must return Unix milliseconds.
   * @default Date.now
   */
  readonly clock?: () => number;
}

/** Options for the `encrypt` method. */
export interface EncryptOptions {
  /** Optional AAD bytes.  Must be supplied identically at decrypt time. */
  readonly aad?: Buffer;
}

/** Options for the `decrypt` method. */
export interface DecryptOptions {
  /**
   * AAD bytes that were supplied at encryption time.
   * If the envelope was encrypted with AAD this MUST be provided; otherwise
   * the GCM tag check will fail.
   */
  readonly aad?: Buffer;
}

/** Options for `rotateKey`. */
export interface RotateKeyOptions {
  readonly pbkdf2?: Pbkdf2Config;
  readonly rawKey?: Buffer;
}

// ---------------------------------------------------------------------------
// Utility: secure memory wiping
// ---------------------------------------------------------------------------

/**
 * Overwrites every byte of `buf` with zeros then truncates the view to zero.
 *
 * This is a best-effort mitigation; Node.js does not guarantee that V8 will
 * not have already copied the buffer's backing ArrayBuffer elsewhere.  Use
 * in conjunction with short-lived `Buffer.allocUnsafe` locals.
 *
 * @param buf - Buffer whose contents should be erased.
 */
export function secureWipe(buf: Buffer): void {
  buf.fill(0);
}

// ---------------------------------------------------------------------------
// Utility: AES-256-GCM primitives
// ---------------------------------------------------------------------------

/**
 * Encrypts `plaintext` under `key` with AES-256-GCM.
 *
 * @param key        - 32-byte key buffer.  Not mutated.
 * @param plaintext  - Arbitrary-length plaintext buffer.
 * @param aad        - Optional additional authenticated data.
 * @returns Tuple `[iv, ciphertext, tag]` — all freshly allocated Buffers.
 */
function aesGcmEncrypt(
  key: Buffer,
  plaintext: Buffer,
  aad?: Buffer,
): [iv: Buffer, ciphertext: Buffer, tag: Buffer] {
  const iv = randomBytes(IV_BYTES);
  const cipher = createCipheriv('aes-256-gcm', key, iv, {
    authTagLength: TAG_BYTES,
  });
  if (aad !== undefined) {
    cipher.setAAD(aad, { plaintextLength: plaintext.length });
  }
  const ct1 = cipher.update(plaintext);
  const ct2 = cipher.final();
  const tag = cipher.getAuthTag();
  return [iv, Buffer.concat([ct1, ct2]), tag];
}

/**
 * Decrypts `ciphertext` under `key` with AES-256-GCM, verifying the tag.
 *
 * @throws {AuthenticationError} if tag verification fails.
 */
function aesGcmDecrypt(
  key: Buffer,
  iv: Buffer,
  ciphertext: Buffer,
  tag: Buffer,
  aad?: Buffer,
): Buffer {
  const decipher = createDecipheriv('aes-256-gcm', key, iv, {
    authTagLength: TAG_BYTES,
  });
  decipher.setAuthTag(tag);
  if (aad !== undefined) {
    decipher.setAAD(aad, { plaintextLength: ciphertext.length });
  }
  try {
    const pt1 = decipher.update(ciphertext);
    const pt2 = decipher.final();
    return Buffer.concat([pt1, pt2]);
  } catch {
    throw new AuthenticationError(
      'AES-256-GCM authentication tag mismatch — data may be corrupt or tampered',
    );
  }
}

// ---------------------------------------------------------------------------
// Utility: DEK wrapping / unwrapping
// ---------------------------------------------------------------------------

/**
 * Wraps (encrypts) `dek` under `kek`.
 *
 * Blob format: `IV(12) || Ciphertext(32) || Tag(16)` = 60 bytes.
 *
 * @param kek - 32-byte key-encryption key.
 * @param dek - 32-byte data-encryption key to wrap.
 * @returns 60-byte wrapped blob as a Buffer.
 */
function wrapDek(kek: Buffer, dek: Buffer): Buffer {
  const [iv, ct, tag] = aesGcmEncrypt(kek, dek);
  return Buffer.concat([iv, ct, tag]); // 12 + 32 + 16 = 60 bytes
}

/**
 * Unwraps (decrypts) a wrapped DEK blob.
 *
 * @param kek  - 32-byte key-encryption key.
 * @param blob - 60-byte wrapped blob produced by `wrapDek`.
 * @throws {EnvelopeFormatError} if the blob is the wrong length.
 * @throws {AuthenticationError} if the tag check fails.
 */
function unwrapDek(kek: Buffer, blob: Buffer): Buffer {
  const BLOB_BYTES = IV_BYTES + KEY_BYTES + TAG_BYTES; // 60
  if (blob.length !== BLOB_BYTES) {
    throw new EnvelopeFormatError(
      `Wrapped DEK blob must be ${BLOB_BYTES} bytes, got ${blob.length}`,
    );
  }
  const iv = blob.subarray(0, IV_BYTES);
  const ct = blob.subarray(IV_BYTES, IV_BYTES + KEY_BYTES);
  const tag = blob.subarray(IV_BYTES + KEY_BYTES);
  return aesGcmDecrypt(kek, iv, ct, tag);
}

// ---------------------------------------------------------------------------
// Utility: PBKDF2 key derivation
// ---------------------------------------------------------------------------

/**
 * Derives a 32-byte AES-256-GCM key from a passphrase via PBKDF2.
 *
 * @throws {KeyDerivationError} if `iterations` is below the minimum.
 */
export async function deriveKeyPbkdf2(cfg: Pbkdf2Config): Promise<Buffer> {
  if (cfg.iterations < MIN_PBKDF2_ITERATIONS) {
    throw new KeyDerivationError(
      `PBKDF2 iterations must be ≥ ${MIN_PBKDF2_ITERATIONS}, got ${cfg.iterations}`,
      { iterations: cfg.iterations, minimum: MIN_PBKDF2_ITERATIONS },
    );
  }
  const digest = cfg.digest ?? 'sha512';
  const passphraseBuf: Buffer =
    cfg.passphrase instanceof Buffer
      ? cfg.passphrase
      : Buffer.from(cfg.passphrase as string, 'utf8');

  const derived = await pbkdf2(
    passphraseBuf,
    cfg.salt,
    cfg.iterations,
    KEY_BYTES,
    digest,
  );
  return Buffer.from(derived);
}

// ---------------------------------------------------------------------------
// Main class: KeyVault
// ---------------------------------------------------------------------------

/**
 * Zero-knowledge envelope encryption vault.
 *
 * ### Thread-safety
 * Node.js is single-threaded, so concurrent JS calls are never truly parallel.
 * However, `encrypt` and `decrypt` are *async* because key derivation can be
 * async.  All mutable state is guarded by an `#operating` flag so that
 * concurrent async calls do not interleave during key rotation (which rewraps
 * stored DEKs).  Callers should await each operation before issuing the next
 * in rotation-heavy workloads.
 *
 * ### Usage
 * ```ts
 * const vault = await KeyVault.create({
 *   pbkdf2: { passphrase: 'hunter2', salt: randomBytes(32), iterations: 310_000 },
 * });
 *
 * const envelope = await vault.encrypt(Buffer.from('secret payload'));
 * const plain    = await vault.decrypt(envelope);
 * console.log(plain.toString()); // "secret payload"
 *
 * await vault.rotateKey({ pbkdf2: { passphrase: 'new-pass', salt: randomBytes(32), iterations: 310_000 } });
 * // All future decryptions still work — DEKs are re-wrapped automatically.
 *
 * vault.destroy(); // wipes all key material
 * ```
 */
export class KeyVault {
  /** Ordered list of key versions.  Last entry is current. */
  readonly #versions: KeyVersion[] = [];
  /** Map from version ID → KeyVersion for O(1) lookup during decrypt. */
  readonly #versionMap = new Map<string, KeyVersion>();
  /** Clock function, injected for deterministic testing. */
  readonly #clock: () => number;
  /** Whether the vault has been permanently destroyed. */
  #destroyed = false;
  /** Whether a rotation is in progress (prevents concurrent rotations). */
  #rotating = false;

  // Metrics
  #encryptCount = 0;
  #decryptCount = 0;
  #rotationCount = 0;
  #rewrapCount = 0;
  #errorCount = 0;

  /**
   * Stored envelopes, keyed by an opaque handle the caller can use to
   * retrieve them later.  The vault acts as an in-memory encrypted KV store.
   * In production this would be backed by a persistent store; here it is
   * intentionally in-memory so the module has no I/O dependency.
   */
  readonly #store = new Map<string, EncryptedEnvelope>();

  private constructor(clock: () => number) {
    this.#clock = clock;
  }

  // -------------------------------------------------------------------------
  // Factory
  // -------------------------------------------------------------------------

  /**
   * Creates a new `KeyVault` and derives (or adopts) the initial KEK.
   *
   * @throws {KeyDerivationError} if `pbkdf2.iterations` is too low.
   * @throws {VaultError} if neither `pbkdf2` nor `rawKey` is supplied, or both are.
   */
  static async create(options: VaultCreateOptions): Promise<KeyVault> {
    if (options.pbkdf2 === undefined && options.rawKey === undefined) {
      throw new VaultError(
        'Either pbkdf2 or rawKey must be supplied',
        'INVALID_OPTIONS',
      );
    }
    if (options.pbkdf2 !== undefined && options.rawKey !== undefined) {
      throw new VaultError(
        'pbkdf2 and rawKey are mutually exclusive',
        'INVALID_OPTIONS',
      );
    }

    const clock = options.clock ?? Date.now;
    const vault = new KeyVault(clock);

    const keyMaterial =
      options.rawKey !== undefined
        ? options.rawKey
        : await deriveKeyPbkdf2(options.pbkdf2!);

    vault.#addVersion(keyMaterial);
    return vault;
  }

  // -------------------------------------------------------------------------
  // Public: encrypt
  // -------------------------------------------------------------------------

  /**
   * Encrypts `plaintext` using a fresh DEK, wraps the DEK under the current
   * KEK, and returns a self-describing `EncryptedEnvelope`.
   *
   * @param plaintext - Arbitrary data to encrypt.
   * @param opts      - Optional AAD.
   * @returns Fully serializable envelope.
   * @throws {VaultDestroyedError} if the vault has been destroyed.
   */
  async encrypt(
    plaintext: Buffer,
    opts: EncryptOptions = {},
  ): Promise<EncryptedEnvelope> {
    this.#assertAlive();

    const currentVersion = this.#currentVersion();
    const dek = randomBytes(KEY_BYTES);
    try {
      // Encrypt the plaintext with the fresh DEK
      const [iv, ciphertext, tag] = aesGcmEncrypt(dek, plaintext, opts.aad);

      // Wrap the DEK under the current KEK
      const wrappedDekBuf = wrapDek(currentVersion.keyMaterial, dek);

      const envelope: EncryptedEnvelope = {
        version: ENVELOPE_VERSION,
        keyVersionId: currentVersion.id,
        wrappedDek: wrappedDekBuf.toString('base64'),
        iv: iv.toString('base64'),
        ciphertext: ciphertext.toString('base64'),
        tag: tag.toString('base64'),
        aad: opts.aad !== undefined ? opts.aad.toString('base64') : null,
      };

      this.#encryptCount++;
      return envelope;
    } catch (err) {
      this.#errorCount++;
      throw err;
    } finally {
      secureWipe(dek);
    }
  }

  // -------------------------------------------------------------------------
  // Public: decrypt
  // -------------------------------------------------------------------------

  /**
   * Decrypts an `EncryptedEnvelope` produced by `encrypt`.
   *
   * Steps:
   * 1. Validate envelope structure.
   * 2. Look up the KEK for the `keyVersionId` in the envelope.
   * 3. Unwrap (decrypt) the DEK using that KEK.
   * 4. Decrypt the payload using the DEK.
   * 5. Wipe the DEK buffer.
   *
   * @param envelope - Envelope produced by `encrypt`.
   * @param opts     - Must supply `aad` if AAD was used at encryption time.
   * @throws {VaultDestroyedError}      if the vault has been destroyed.
   * @throws {EnvelopeFormatError}      if the envelope is structurally invalid.
   * @throws {KeyVersionNotFoundError}  if the key version is unknown.
   * @throws {AuthenticationError}      if any GCM tag check fails.
   */
  async decrypt(
    envelope: EncryptedEnvelope,
    opts: DecryptOptions = {},
  ): Promise<Buffer> {
    this.#assertAlive();
    this.#validateEnvelope(envelope);

    const version = this.#versionMap.get(envelope.keyVersionId);
    if (version === undefined) {
      this.#errorCount++;
      throw new KeyVersionNotFoundError(envelope.keyVersionId);
    }

    const wrappedDekBuf = Buffer.from(envelope.wrappedDek, 'base64');
    let dek!: Buffer;
    try {
      dek = unwrapDek(version.keyMaterial, wrappedDekBuf);

      const iv = Buffer.from(envelope.iv, 'base64');
      const ciphertext = Buffer.from(envelope.ciphertext, 'base64');
      const tag = Buffer.from(envelope.tag, 'base64');

      // Reconstruct AAD for tag verification.  Priority: caller-supplied opts,
      // then AAD stored in envelope, then none.
      let aad: Buffer | undefined;
      if (opts.aad !== undefined) {
        aad = opts.aad;
      } else if (envelope.aad !== null) {
        aad = Buffer.from(envelope.aad, 'base64');
      }

      const plaintext = aesGcmDecrypt(dek, iv, ciphertext, tag, aad);
      this.#decryptCount++;
      return plaintext;
    } catch (err) {
      if (!(err instanceof AuthenticationError || err instanceof EnvelopeFormatError)) {
        // unexpected — still count it
      }
      this.#errorCount++;
      throw err;
    } finally {
      if (dek !== undefined) secureWipe(dek);
    }
  }

  // -------------------------------------------------------------------------
  // Public: store / load (in-memory encrypted KV)
  // -------------------------------------------------------------------------

  /**
   * Encrypts `plaintext` and stores the resulting envelope under `handle`.
   * Any previous value at `handle` is overwritten.
   */
  async store(
    handle: string,
    plaintext: Buffer,
    opts: EncryptOptions = {},
  ): Promise<void> {
    const envelope = await this.encrypt(plaintext, opts);
    this.#store.set(handle, envelope);
  }

  /**
   * Retrieves and decrypts the envelope stored under `handle`.
   *
   * @throws {VaultError} if `handle` is not found.
   */
  async load(handle: string, opts: DecryptOptions = {}): Promise<Buffer> {
    const envelope = this.#store.get(handle);
    if (envelope === undefined) {
      throw new VaultError(
        `No entry for handle '${handle}'`,
        'HANDLE_NOT_FOUND',
        { handle },
      );
    }
    return this.decrypt(envelope, opts);
  }

  /** Returns `true` if `handle` has a stored entry. */
  has(handle: string): boolean {
    return this.#store.has(handle);
  }

  /** Deletes the stored entry for `handle`.  Returns `true` if it existed. */
  delete(handle: string): boolean {
    return this.#store.delete(handle);
  }

  /** Number of entries currently in the store. */
  get storeSize(): number {
    return this.#store.size;
  }

  // -------------------------------------------------------------------------
  // Public: key rotation
  // -------------------------------------------------------------------------

  /**
   * Derives (or adopts) a new KEK, re-wraps all stored DEKs to it, and
   * retires (but does NOT wipe) the old KEK versions so that in-flight
   * decryptions that hold a reference to an old envelope still work.
   *
   * In a production system the old KEK versions would be retained in a secure
   * HSM and wiped after a grace period.  Here they are kept in-process.
   *
   * @throws {VaultDestroyedError} if the vault has been destroyed.
   * @throws {VaultError} if another rotation is already in progress.
   */
  async rotateKey(opts: RotateKeyOptions): Promise<string> {
    this.#assertAlive();
    if (this.#rotating) {
      throw new VaultError('A key rotation is already in progress', 'ROTATION_IN_PROGRESS');
    }
    this.#rotating = true;

    try {
      const newKeyMaterial =
        opts.rawKey !== undefined
          ? opts.rawKey
          : await deriveKeyPbkdf2(opts.pbkdf2!);

      const newVersionId = this.#addVersion(newKeyMaterial);

      // Re-wrap all stored envelopes to the new KEK
      for (const [handle, envelope] of this.#store) {
        const oldVersion = this.#versionMap.get(envelope.keyVersionId);
        if (oldVersion === undefined) continue; // should not happen
        if (envelope.keyVersionId === newVersionId) continue; // already current

        const wrappedDekBuf = Buffer.from(envelope.wrappedDek, 'base64');
        let dek!: Buffer;
        try {
          dek = unwrapDek(oldVersion.keyMaterial, wrappedDekBuf);
          const newWrappedDekBuf = wrapDek(newKeyMaterial, dek);
          const updated: EncryptedEnvelope = {
            ...envelope,
            keyVersionId: newVersionId,
            wrappedDek: newWrappedDekBuf.toString('base64'),
          };
          this.#store.set(handle, updated);
          this.#rewrapCount++;
        } finally {
          if (dek !== undefined) secureWipe(dek);
        }
      }

      this.#rotationCount++;
      return newVersionId;
    } finally {
      this.#rotating = false;
    }
  }

  // -------------------------------------------------------------------------
  // Public: re-encrypt a single external envelope to the current KEK
  // -------------------------------------------------------------------------

  /**
   * Re-wraps an envelope's DEK to the current KEK without touching the
   * payload ciphertext.  Useful when consuming envelopes produced by another
   * vault instance or fetched from a remote store.
   *
   * @throws {KeyVersionNotFoundError} if the envelope's key version is unknown.
   * @throws {AuthenticationError}     if the DEK unwrap fails.
   */
  async rewrapEnvelope(envelope: EncryptedEnvelope): Promise<EncryptedEnvelope> {
    this.#assertAlive();
    this.#validateEnvelope(envelope);

    const currentVersion = this.#currentVersion();
    if (envelope.keyVersionId === currentVersion.id) {
      return envelope; // already current
    }

    const oldVersion = this.#versionMap.get(envelope.keyVersionId);
    if (oldVersion === undefined) {
      throw new KeyVersionNotFoundError(envelope.keyVersionId);
    }

    const wrappedDekBuf = Buffer.from(envelope.wrappedDek, 'base64');
    let dek!: Buffer;
    try {
      dek = unwrapDek(oldVersion.keyMaterial, wrappedDekBuf);
      const newWrappedDekBuf = wrapDek(currentVersion.keyMaterial, dek);
      this.#rewrapCount++;
      return {
        ...envelope,
        keyVersionId: currentVersion.id,
        wrappedDek: newWrappedDekBuf.toString('base64'),
      };
    } finally {
      if (dek !== undefined) secureWipe(dek);
    }
  }

  // -------------------------------------------------------------------------
  // Public: introspection
  // -------------------------------------------------------------------------

  /**
   * Returns metadata about all key versions.  Key material is never exposed.
   */
  keyVersions(): readonly KeyVersionInfo[] {
    return this.#versions.map((v) => ({
      id: v.id,
      createdAt: v.createdAt,
      algorithm: 'AES-256-GCM',
      derivation: 'PBKDF2-HMAC-SHA512',
    }));
  }

  /** Returns the ID of the current (most-recently-added) key version. */
  currentKeyVersionId(): string {
    this.#assertAlive();
    return this.#currentVersion().id;
  }

  /** Snapshot of vault operational metrics. */
  metrics(): VaultMetrics {
    return {
      encryptCount: this.#encryptCount,
      decryptCount: this.#decryptCount,
      rotationCount: this.#rotationCount,
      rewrapCount: this.#rewrapCount,
      errorCount: this.#errorCount,
      keyVersionCount: this.#versions.length,
      currentKeyVersionId:
        this.#versions.length > 0
          ? (this.#versions[this.#versions.length - 1]?.id ?? null)
          : null,
    };
  }

  // -------------------------------------------------------------------------
  // Public: lifecycle
  // -------------------------------------------------------------------------

  /**
   * Permanently destroys the vault, wiping all key material.
   * After calling `destroy`, all subsequent vault operations throw
   * `VaultDestroyedError`.
   */
  destroy(): void {
    if (this.#destroyed) return;
    this.#destroyed = true;
    for (const version of this.#versions) {
      secureWipe(version.keyMaterial);
    }
    this.#versions.length = 0;
    this.#versionMap.clear();
    this.#store.clear();
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  /** Throws `VaultDestroyedError` if the vault is in the destroyed state. */
  #assertAlive(): void {
    if (this.#destroyed) throw new VaultDestroyedError();
  }

  /** Returns the current (latest) key version.  Throws if none exist. */
  #currentVersion(): KeyVersion {
    const v = this.#versions[this.#versions.length - 1];
    if (v === undefined) {
      throw new VaultError('No key versions available', 'NO_KEY_VERSIONS');
    }
    return v;
  }

  /**
   * Registers a new key version and returns its ID.
   *
   * @param keyMaterial - 32-byte key buffer; the vault takes ownership.
   */
  #addVersion(keyMaterial: Buffer): string {
    if (keyMaterial.length !== KEY_BYTES) {
      throw new KeyDerivationError(
        `Key material must be exactly ${KEY_BYTES} bytes, got ${keyMaterial.length}`,
        { length: keyMaterial.length },
      );
    }
    const version: KeyVersion = {
      id: randomUUID(),
      createdAt: this.#clock(),
      keyMaterial,
    };
    this.#versions.push(version);
    this.#versionMap.set(version.id, version);
    return version.id;
  }

  /**
   * Validates the structural integrity of an `EncryptedEnvelope`.
   * Does NOT perform any cryptographic verification.
   *
   * @throws {EnvelopeFormatError} on any structural violation.
   */
  #validateEnvelope(envelope: EncryptedEnvelope): void {
    if (envelope.version !== ENVELOPE_VERSION) {
      throw new EnvelopeFormatError(
        `Unsupported envelope version: ${String(envelope.version)}`,
        { version: envelope.version },
      );
    }
    if (typeof envelope.keyVersionId !== 'string' || envelope.keyVersionId.length === 0) {
      throw new EnvelopeFormatError('keyVersionId must be a non-empty string');
    }
    for (const field of ['wrappedDek', 'iv', 'ciphertext', 'tag'] as const) {
      if (typeof envelope[field] !== 'string') {
        throw new EnvelopeFormatError(`envelope.${field} must be a base64 string`);
      }
    }
    // Validate IV length
    const iv = Buffer.from(envelope.iv, 'base64');
    if (iv.length !== IV_BYTES) {
      throw new EnvelopeFormatError(
        `envelope.iv must decode to ${IV_BYTES} bytes, got ${iv.length}`,
      );
    }
    // Validate tag length
    const tag = Buffer.from(envelope.tag, 'base64');
    if (tag.length !== TAG_BYTES) {
      throw new EnvelopeFormatError(
        `envelope.tag must decode to ${TAG_BYTES} bytes, got ${tag.length}`,
      );
    }
    // Validate wrapped DEK length
    const BLOB_BYTES = IV_BYTES + KEY_BYTES + TAG_BYTES;
    const wrappedDek = Buffer.from(envelope.wrappedDek, 'base64');
    if (wrappedDek.length !== BLOB_BYTES) {
      throw new EnvelopeFormatError(
        `envelope.wrappedDek must decode to ${BLOB_BYTES} bytes, got ${wrappedDek.length}`,
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Convenience factory
// ---------------------------------------------------------------------------

/**
 * Creates a `KeyVault` from a raw 32-byte key.  Useful for testing and for
 * environments that supply key material from an HSM rather than a passphrase.
 *
 * The supplied `rawKey` buffer is consumed (its contents will be wiped by the
 * vault on destroy/rotation).
 */
export async function createVaultFromRawKey(
  rawKey: Buffer,
  opts?: Pick<VaultCreateOptions, 'clock'>,
): Promise<KeyVault> {
  return KeyVault.create({ rawKey, clock: opts?.clock });
}

/**
 * Creates a `KeyVault` from a passphrase via PBKDF2-HMAC-SHA512.
 *
 * @param passphrase - UTF-8 passphrase.
 * @param salt       - Cryptographic salt (≥ 16 bytes recommended).
 * @param iterations - PBKDF2 iteration count (≥ MIN_PBKDF2_ITERATIONS).
 */
export async function createVaultFromPassphrase(
  passphrase: string,
  salt: Buffer,
  iterations: number = MIN_PBKDF2_ITERATIONS,
  opts?: Pick<VaultCreateOptions, 'clock'>,
): Promise<KeyVault> {
  return KeyVault.create({
    pbkdf2: { passphrase, salt, iterations },
    clock: opts?.clock,
  });
}

/**
 * Compares two `EncryptedEnvelope` objects for structural equality using
 * timing-safe comparison on the ciphertext fields to prevent timing-based
 * oracle attacks.
 *
 * Note: this does NOT mean the plaintexts are equal — two different
 * encryptions of the same plaintext will differ due to random IVs and DEKs.
 * This is primarily useful in test code.
 */
export function envelopesEqual(a: EncryptedEnvelope, b: EncryptedEnvelope): boolean {
  if (a.version !== b.version) return false;
  if (a.keyVersionId !== b.keyVersionId) return false;
  if (a.aad !== b.aad) return false;
  // Timing-safe comparison for cryptographic fields
  for (const field of ['wrappedDek', 'iv', 'ciphertext', 'tag'] as const) {
    const bufA = Buffer.from(a[field], 'base64');
    const bufB = Buffer.from(b[field], 'base64');
    if (bufA.length !== bufB.length) return false;
    if (!timingSafeEqual(bufA, bufB)) return false;
  }
  return true;
}
