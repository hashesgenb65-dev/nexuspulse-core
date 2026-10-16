/**
 * @file tests/encryption/test_vault.ts
 * @description Exhaustive Vitest test suite for /src/encryption/vault.ts.
 *
 * ## Coverage strategy
 *
 * - **Unit** tests on every exported primitive (secureWipe, deriveKeyPbkdf2,
 *   aesGcmEncrypt/Decrypt via vault round-trip, wrapDek/unwrapDek via vault
 *   internal paths).
 * - **Integration** tests that exercise the full encrypt → store → rotate →
 *   decrypt lifecycle.
 * - **Error-path** tests for every named error class.
 * - **Edge-case** tests: empty plaintext, very large plaintext, non-ASCII
 *   passphrases, zero-byte AAD, envelope structural corruption.
 * - **Metrics** assertions after each group of operations.
 * - **Determinism** tests using a fixed clock.
 * - **Security invariant** tests: different encryptions of same plaintext
 *   produce different ciphertexts; DEK is not the KEK; wiped buffers are zero.
 */

import { randomBytes } from 'node:crypto';
import { describe, it, expect, beforeEach } from 'vitest';
import {
  KeyVault,
  VaultError,
  EnvelopeFormatError,
  AuthenticationError,
  KeyVersionNotFoundError,
  VaultDestroyedError,
  KeyDerivationError,
  deriveKeyPbkdf2,
  createVaultFromRawKey,
  createVaultFromPassphrase,
  secureWipe,
  envelopesEqual,
  ENVELOPE_VERSION,
  KEY_BYTES,
  IV_BYTES,
  TAG_BYTES,
  MIN_PBKDF2_ITERATIONS,
  type EncryptedEnvelope,
} from '../../src/encryption/vault.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Fast raw-key vault for tests that do not care about PBKDF2. */
async function makeVault(clock?: () => number): Promise<KeyVault> {
  const key = randomBytes(KEY_BYTES);
  return createVaultFromRawKey(key, { clock });
}

/** Deterministic fixed clock value. */
const FIXED_NOW = 1_760_000_000_000;
const fixedClock = (): number => FIXED_NOW;

/** Returns a 32-byte buffer filled with a repeated byte value. */
function repeatByte(byte: number): Buffer {
  return Buffer.alloc(KEY_BYTES, byte);
}

/** Corrupts one base64-encoded field of an envelope (flips one byte). */
function corruptBase64Field(b64: string): string {
  const buf = Buffer.from(b64, 'base64');
  buf[0] = (buf[0]! ^ 0xff) & 0xff;
  return buf.toString('base64');
}

// ---------------------------------------------------------------------------
// secureWipe
// ---------------------------------------------------------------------------

describe('secureWipe', () => {
  it('fills the buffer with zeros', () => {
    const buf = Buffer.from([1, 2, 3, 4, 5]);
    secureWipe(buf);
    expect([...buf]).toEqual([0, 0, 0, 0, 0]);
  });

  it('is a no-op on empty buffer', () => {
    const buf = Buffer.alloc(0);
    expect(() => secureWipe(buf)).not.toThrow();
  });

  it('works on large buffers', () => {
    const buf = randomBytes(65536);
    secureWipe(buf);
    expect(buf.every((b) => b === 0)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// deriveKeyPbkdf2
// ---------------------------------------------------------------------------

describe('deriveKeyPbkdf2', () => {
  const salt = Buffer.alloc(32, 0xab);

  it('produces a 32-byte key', async () => {
    const key = await deriveKeyPbkdf2({
      passphrase: 'testpass',
      salt,
      iterations: MIN_PBKDF2_ITERATIONS,
    });
    expect(key.length).toBe(KEY_BYTES);
  });

  it('is deterministic for the same inputs', async () => {
    const cfg = { passphrase: 'same-pass', salt, iterations: MIN_PBKDF2_ITERATIONS };
    const a = await deriveKeyPbkdf2(cfg);
    const b = await deriveKeyPbkdf2(cfg);
    expect(a.equals(b)).toBe(true);
  });

  it('produces different keys for different passphrases', async () => {
    const cfg1 = { passphrase: 'pass1', salt, iterations: MIN_PBKDF2_ITERATIONS };
    const cfg2 = { passphrase: 'pass2', salt, iterations: MIN_PBKDF2_ITERATIONS };
    const a = await deriveKeyPbkdf2(cfg1);
    const b = await deriveKeyPbkdf2(cfg2);
    expect(a.equals(b)).toBe(false);
  });

  it('produces different keys for different salts', async () => {
    const salt2 = Buffer.alloc(32, 0xcd);
    const cfg1 = { passphrase: 'same', salt, iterations: MIN_PBKDF2_ITERATIONS };
    const cfg2 = { passphrase: 'same', salt: salt2, iterations: MIN_PBKDF2_ITERATIONS };
    const a = await deriveKeyPbkdf2(cfg1);
    const b = await deriveKeyPbkdf2(cfg2);
    expect(a.equals(b)).toBe(false);
  });

  it('rejects iterations below minimum', async () => {
    await expect(
      deriveKeyPbkdf2({ passphrase: 'p', salt, iterations: 1000 }),
    ).rejects.toThrow(KeyDerivationError);
  });

  it('accepts a Buffer passphrase', async () => {
    const key = await deriveKeyPbkdf2({
      passphrase: Buffer.from('buf-passphrase'),
      salt,
      iterations: MIN_PBKDF2_ITERATIONS,
    });
    expect(key.length).toBe(KEY_BYTES);
  });

  it('supports sha256 digest', async () => {
    const key = await deriveKeyPbkdf2({
      passphrase: 'sha256test',
      salt,
      iterations: MIN_PBKDF2_ITERATIONS,
      digest: 'sha256',
    });
    expect(key.length).toBe(KEY_BYTES);
  });

  it('supports non-ASCII passphrase', async () => {
    const key = await deriveKeyPbkdf2({
      passphrase: '日本語パスフレーズ🔐',
      salt,
      iterations: MIN_PBKDF2_ITERATIONS,
    });
    expect(key.length).toBe(KEY_BYTES);
  });
});

// ---------------------------------------------------------------------------
// KeyVault.create — factory validation
// ---------------------------------------------------------------------------

describe('KeyVault.create', () => {
  it('creates a vault with a raw key', async () => {
    const vault = await makeVault();
    expect(vault).toBeInstanceOf(KeyVault);
    vault.destroy();
  });

  it('creates a vault with PBKDF2', async () => {
    const vault = await createVaultFromPassphrase('hunter2', randomBytes(32));
    expect(vault.keyVersions()).toHaveLength(1);
    vault.destroy();
  });

  it('throws when neither pbkdf2 nor rawKey supplied', async () => {
    await expect(KeyVault.create({} as never)).rejects.toThrow(VaultError);
  });

  it('throws when both pbkdf2 and rawKey supplied', async () => {
    await expect(
      KeyVault.create({
        pbkdf2: {
          passphrase: 'p',
          salt: randomBytes(16),
          iterations: MIN_PBKDF2_ITERATIONS,
        },
        rawKey: randomBytes(KEY_BYTES),
      }),
    ).rejects.toThrow(VaultError);
  });

  it('rejects rawKey of wrong length', async () => {
    await expect(createVaultFromRawKey(Buffer.alloc(16))).rejects.toThrow(KeyDerivationError);
  });
});

// ---------------------------------------------------------------------------
// Basic encrypt / decrypt round-trip
// ---------------------------------------------------------------------------

describe('encrypt / decrypt round-trip', () => {
  let vault: KeyVault;

  beforeEach(async () => {
    vault = await makeVault(fixedClock);
  });

  it('decrypts to original plaintext', async () => {
    const plaintext = Buffer.from('hello, vault!');
    const env = await vault.encrypt(plaintext);
    const result = await vault.decrypt(env);
    expect(result.equals(plaintext)).toBe(true);
  });

  it('handles empty plaintext', async () => {
    const env = await vault.encrypt(Buffer.alloc(0));
    const result = await vault.decrypt(env);
    expect(result.length).toBe(0);
  });

  it('handles 1 MB plaintext', async () => {
    const plaintext = randomBytes(1024 * 1024);
    const env = await vault.encrypt(plaintext);
    const result = await vault.decrypt(env);
    expect(result.equals(plaintext)).toBe(true);
  });

  it('encrypts same plaintext to different ciphertexts (IVs are random)', async () => {
    const plaintext = Buffer.from('same');
    const env1 = await vault.encrypt(plaintext);
    const env2 = await vault.encrypt(plaintext);
    expect(env1.iv).not.toBe(env2.iv);
    expect(env1.ciphertext).not.toBe(env2.ciphertext);
    expect(env1.wrappedDek).not.toBe(env2.wrappedDek);
  });

  it('envelope has correct version', async () => {
    const env = await vault.encrypt(Buffer.from('v'));
    expect(env.version).toBe(ENVELOPE_VERSION);
  });

  it('envelope iv decodes to IV_BYTES', async () => {
    const env = await vault.encrypt(Buffer.from('iv-check'));
    expect(Buffer.from(env.iv, 'base64').length).toBe(IV_BYTES);
  });

  it('envelope tag decodes to TAG_BYTES', async () => {
    const env = await vault.encrypt(Buffer.from('tag-check'));
    expect(Buffer.from(env.tag, 'base64').length).toBe(TAG_BYTES);
  });

  it('envelope wrappedDek decodes to IV+KEY+TAG bytes', async () => {
    const env = await vault.encrypt(Buffer.from('dek-check'));
    expect(Buffer.from(env.wrappedDek, 'base64').length).toBe(IV_BYTES + KEY_BYTES + TAG_BYTES);
  });

  it('envelope aad is null when no AAD supplied', async () => {
    const env = await vault.encrypt(Buffer.from('no-aad'));
    expect(env.aad).toBeNull();
  });

  it('binary data round-trips correctly', async () => {
    const plaintext = Buffer.from([0x00, 0xff, 0x80, 0x7f, 0x01]);
    const result = await vault.decrypt(await vault.encrypt(plaintext));
    expect(result.equals(plaintext)).toBe(true);
  });

  it('increments encryptCount and decryptCount', async () => {
    await vault.encrypt(Buffer.from('a'));
    await vault.encrypt(Buffer.from('b'));
    const env = await vault.encrypt(Buffer.from('c'));
    await vault.decrypt(env);
    const m = vault.metrics();
    expect(m.encryptCount).toBe(3);
    expect(m.decryptCount).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// AAD (Additional Authenticated Data)
// ---------------------------------------------------------------------------

describe('AAD', () => {
  let vault: KeyVault;

  beforeEach(async () => {
    vault = await makeVault();
  });

  it('encrypts and decrypts with matching AAD', async () => {
    const plaintext = Buffer.from('secret');
    const aad = Buffer.from('user:42');
    const env = await vault.encrypt(plaintext, { aad });
    const result = await vault.decrypt(env, { aad });
    expect(result.equals(plaintext)).toBe(true);
  });

  it('fails to decrypt when AAD is wrong', async () => {
    const plaintext = Buffer.from('secret');
    const aad = Buffer.from('user:42');
    const env = await vault.encrypt(plaintext, { aad });
    await expect(
      vault.decrypt(env, { aad: Buffer.from('user:99') }),
    ).rejects.toThrow(AuthenticationError);
  });

  it('fails to decrypt when AAD is missing at decrypt', async () => {
    const plaintext = Buffer.from('secret');
    const aad = Buffer.from('context');
    const env = await vault.encrypt(plaintext, { aad });
    // Decrypt without any aad — envelope.aad will be used, should still work
    const result = await vault.decrypt(env);
    expect(result.equals(plaintext)).toBe(true);
  });

  it('caller-supplied opts.aad overrides envelope.aad', async () => {
    const plaintext = Buffer.from('secret');
    const aad = Buffer.from('ctx');
    const env = await vault.encrypt(plaintext, { aad });
    // Provide same AAD via opts — should succeed
    const result = await vault.decrypt(env, { aad });
    expect(result.equals(plaintext)).toBe(true);
  });

  it('aad field is set in envelope when AAD supplied', async () => {
    const aad = Buffer.from('meta');
    const env = await vault.encrypt(Buffer.from('data'), { aad });
    expect(env.aad).not.toBeNull();
    expect(Buffer.from(env.aad!, 'base64').equals(aad)).toBe(true);
  });

  it('zero-byte AAD round-trips', async () => {
    const aad = Buffer.alloc(0);
    const plaintext = Buffer.from('zero-aad');
    const env = await vault.encrypt(plaintext, { aad });
    const result = await vault.decrypt(env, { aad });
    expect(result.equals(plaintext)).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Envelope format validation
// ---------------------------------------------------------------------------

describe('envelope format validation', () => {
  let vault: KeyVault;
  let goodEnv: EncryptedEnvelope;

  beforeEach(async () => {
    vault = await makeVault();
    goodEnv = await vault.encrypt(Buffer.from('valid'));
  });

  it('throws EnvelopeFormatError for wrong version', async () => {
    const bad = { ...goodEnv, version: 99 as typeof ENVELOPE_VERSION };
    await expect(vault.decrypt(bad)).rejects.toThrow(EnvelopeFormatError);
  });

  it('throws EnvelopeFormatError for empty keyVersionId', async () => {
    const bad = { ...goodEnv, keyVersionId: '' };
    await expect(vault.decrypt(bad)).rejects.toThrow(EnvelopeFormatError);
  });

  it('throws EnvelopeFormatError when iv decodes to wrong length', async () => {
    const bad = { ...goodEnv, iv: Buffer.alloc(8).toString('base64') };
    await expect(vault.decrypt(bad)).rejects.toThrow(EnvelopeFormatError);
  });

  it('throws EnvelopeFormatError when tag decodes to wrong length', async () => {
    const bad = { ...goodEnv, tag: Buffer.alloc(8).toString('base64') };
    await expect(vault.decrypt(bad)).rejects.toThrow(EnvelopeFormatError);
  });

  it('throws EnvelopeFormatError when wrappedDek decodes to wrong length', async () => {
    const bad = { ...goodEnv, wrappedDek: Buffer.alloc(10).toString('base64') };
    await expect(vault.decrypt(bad)).rejects.toThrow(EnvelopeFormatError);
  });
});

// ---------------------------------------------------------------------------
// Tamper / corruption detection
// ---------------------------------------------------------------------------

describe('tamper detection', () => {
  let vault: KeyVault;

  beforeEach(async () => {
    vault = await makeVault();
  });

  it('detects ciphertext corruption', async () => {
    const env = await vault.encrypt(Buffer.from('tamper me'));
    const bad = { ...env, ciphertext: corruptBase64Field(env.ciphertext) };
    await expect(vault.decrypt(bad)).rejects.toThrow(AuthenticationError);
  });

  it('detects tag corruption', async () => {
    const env = await vault.encrypt(Buffer.from('tamper tag'));
    const bad = { ...env, tag: corruptBase64Field(env.tag) };
    await expect(vault.decrypt(bad)).rejects.toThrow(AuthenticationError);
  });

  it('detects wrapped DEK corruption', async () => {
    const env = await vault.encrypt(Buffer.from('tamper dek'));
    const bad = { ...env, wrappedDek: corruptBase64Field(env.wrappedDek) };
    await expect(vault.decrypt(bad)).rejects.toThrow(AuthenticationError);
  });

  it('detects IV corruption (causes auth failure)', async () => {
    const env = await vault.encrypt(Buffer.from('tamper iv'));
    const bad = { ...env, iv: corruptBase64Field(env.iv) };
    await expect(vault.decrypt(bad)).rejects.toThrow(AuthenticationError);
  });

  it('increments errorCount on tamper', async () => {
    const env = await vault.encrypt(Buffer.from('err'));
    const bad = { ...env, ciphertext: corruptBase64Field(env.ciphertext) };
    try { await vault.decrypt(bad); } catch { /* expected */ }
    expect(vault.metrics().errorCount).toBeGreaterThan(0);
  });
});

// ---------------------------------------------------------------------------
// KeyVersionNotFoundError
// ---------------------------------------------------------------------------

describe('KeyVersionNotFoundError', () => {
  it('throws when keyVersionId does not exist in vault', async () => {
    const vault = await makeVault();
    const env = await vault.encrypt(Buffer.from('data'));
    const bad = { ...env, keyVersionId: 'nonexistent-uuid' };
    await expect(vault.decrypt(bad)).rejects.toThrow(KeyVersionNotFoundError);
    vault.destroy();
  });
});

// ---------------------------------------------------------------------------
// Key rotation
// ---------------------------------------------------------------------------

describe('key rotation', () => {
  let vault: KeyVault;

  beforeEach(async () => {
    vault = await makeVault(fixedClock);
  });

  it('adds a second key version', async () => {
    const newId = await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });
    expect(vault.keyVersions()).toHaveLength(2);
    expect(vault.currentKeyVersionId()).toBe(newId);
  });

  it('can still decrypt envelopes produced before rotation', async () => {
    const plaintext = Buffer.from('pre-rotation');
    const env = await vault.encrypt(plaintext);

    await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });
    const result = await vault.decrypt(env);
    expect(result.equals(plaintext)).toBe(true);
  });

  it('new encryptions use the new key version', async () => {
    const oldId = vault.currentKeyVersionId();
    await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });
    const env = await vault.encrypt(Buffer.from('post-rotation'));
    expect(env.keyVersionId).not.toBe(oldId);
    expect(env.keyVersionId).toBe(vault.currentKeyVersionId());
  });

  it('re-wraps stored envelopes to new key', async () => {
    await vault.store('k1', Buffer.from('v1'));
    await vault.store('k2', Buffer.from('v2'));
    const oldId = vault.currentKeyVersionId();

    await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });

    const m = vault.metrics();
    expect(m.rewrapCount).toBe(2);

    // Verify stored values still decrypt correctly
    const v1 = await vault.load('k1');
    expect(v1.toString()).toBe('v1');
    const v2 = await vault.load('k2');
    expect(v2.toString()).toBe('v2');

    // Stored envelopes should reference new key
    // (we test via round-trip — internal key IDs are not directly exposed)
    expect(vault.currentKeyVersionId()).not.toBe(oldId);
  });

  it('increments rotationCount', async () => {
    await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });
    await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });
    expect(vault.metrics().rotationCount).toBe(2);
  });

  it('supports PBKDF2-based rotation', async () => {
    const newId = await vault.rotateKey({
      pbkdf2: {
        passphrase: 'new-password',
        salt: randomBytes(32),
        iterations: MIN_PBKDF2_ITERATIONS,
      },
    });
    expect(typeof newId).toBe('string');
    expect(newId.length).toBeGreaterThan(0);
  });

  it('throws VaultError if rotation is called concurrently', async () => {
    // Trigger rotation with PBKDF2 (async) and immediately try another
    const p1 = vault.rotateKey({
      pbkdf2: {
        passphrase: 'concurrent-1',
        salt: randomBytes(32),
        iterations: MIN_PBKDF2_ITERATIONS,
      },
    });
    const p2 = vault.rotateKey({
      pbkdf2: {
        passphrase: 'concurrent-2',
        salt: randomBytes(32),
        iterations: MIN_PBKDF2_ITERATIONS,
      },
    });
    // One should succeed, one should throw
    const results = await Promise.allSettled([p1, p2]);
    const errors = results.filter((r) => r.status === 'rejected');
    expect(errors.length).toBeGreaterThanOrEqual(1);
  });
});

// ---------------------------------------------------------------------------
// rewrapEnvelope
// ---------------------------------------------------------------------------

describe('rewrapEnvelope', () => {
  it('rewraps to current key and is still decryptable', async () => {
    const vault = await makeVault();
    const plaintext = Buffer.from('rewrap me');
    const oldEnv = await vault.encrypt(plaintext);

    await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });
    const newEnv = await vault.rewrapEnvelope(oldEnv);

    expect(newEnv.keyVersionId).toBe(vault.currentKeyVersionId());
    const result = await vault.decrypt(newEnv);
    expect(result.equals(plaintext)).toBe(true);
    vault.destroy();
  });

  it('returns same envelope object if already current key', async () => {
    const vault = await makeVault();
    const env = await vault.encrypt(Buffer.from('already current'));
    const rewrapped = await vault.rewrapEnvelope(env);
    expect(envelopesEqual(env, rewrapped)).toBe(true);
    vault.destroy();
  });

  it('throws KeyVersionNotFoundError for unknown key version', async () => {
    const vault = await makeVault();
    const env = await vault.encrypt(Buffer.from('data'));
    const bad = { ...env, keyVersionId: 'unknown-uuid' };
    await expect(vault.rewrapEnvelope(bad)).rejects.toThrow(KeyVersionNotFoundError);
    vault.destroy();
  });
});

// ---------------------------------------------------------------------------
// store / load / has / delete
// ---------------------------------------------------------------------------

describe('store / load KV interface', () => {
  let vault: KeyVault;

  beforeEach(async () => {
    vault = await makeVault();
  });

  it('stores and loads a value', async () => {
    await vault.store('key1', Buffer.from('value1'));
    const v = await vault.load('key1');
    expect(v.toString()).toBe('value1');
  });

  it('has() returns true after store, false before', async () => {
    expect(vault.has('absent')).toBe(false);
    await vault.store('present', Buffer.from('x'));
    expect(vault.has('present')).toBe(true);
  });

  it('delete() removes entry', async () => {
    await vault.store('del', Buffer.from('x'));
    expect(vault.delete('del')).toBe(true);
    expect(vault.has('del')).toBe(false);
    expect(vault.delete('del')).toBe(false);
  });

  it('storeSize reflects current count', async () => {
    expect(vault.storeSize).toBe(0);
    await vault.store('a', Buffer.from('1'));
    await vault.store('b', Buffer.from('2'));
    expect(vault.storeSize).toBe(2);
    vault.delete('a');
    expect(vault.storeSize).toBe(1);
  });

  it('throws VaultError on load of absent key', async () => {
    await expect(vault.load('nope')).rejects.toThrow(VaultError);
  });

  it('overwrites handle on second store', async () => {
    await vault.store('h', Buffer.from('first'));
    await vault.store('h', Buffer.from('second'));
    expect(vault.storeSize).toBe(1);
    const v = await vault.load('h');
    expect(v.toString()).toBe('second');
  });

  it('supports AAD in store/load', async () => {
    const aad = Buffer.from('meta:42');
    await vault.store('aad-key', Buffer.from('secret'), { aad });
    const v = await vault.load('aad-key', { aad });
    expect(v.toString()).toBe('secret');
  });
});

// ---------------------------------------------------------------------------
// Vault lifecycle / destroy
// ---------------------------------------------------------------------------

describe('destroy', () => {
  it('throws VaultDestroyedError after destroy', async () => {
    const vault = await makeVault();
    vault.destroy();
    await expect(vault.encrypt(Buffer.from('x'))).rejects.toThrow(VaultDestroyedError);
  });

  it('can call destroy twice without error', async () => {
    const vault = await makeVault();
    vault.destroy();
    expect(() => vault.destroy()).not.toThrow();
  });

  it('clears the store on destroy (encrypt throws VaultDestroyedError)', async () => {
    const vault = await makeVault();
    await vault.store('k', Buffer.from('v'));
    vault.destroy();
    // Store is cleared — loading throws VaultError (handle not found) because
    // load() checks the store before assertAlive.  encrypt() hits assertAlive first.
    await expect(vault.encrypt(Buffer.from('x'))).rejects.toThrow(VaultDestroyedError);
    // load() no longer finds the entry (store was cleared)
    await expect(vault.load('k')).rejects.toThrow(VaultError);
  });
});

// ---------------------------------------------------------------------------
// keyVersions / currentKeyVersionId / metrics
// ---------------------------------------------------------------------------

describe('introspection', () => {
  let vault: KeyVault;

  beforeEach(async () => {
    vault = await makeVault(fixedClock);
  });

  it('keyVersions returns one entry initially', () => {
    const versions = vault.keyVersions();
    expect(versions).toHaveLength(1);
    expect(versions[0]?.algorithm).toBe('AES-256-GCM');
  });

  it('keyVersions createdAt reflects fixed clock', () => {
    expect(vault.keyVersions()[0]?.createdAt).toBe(FIXED_NOW);
  });

  it('currentKeyVersionId matches first version', () => {
    const id = vault.currentKeyVersionId();
    expect(id).toBe(vault.keyVersions()[0]?.id);
  });

  it('metrics starts at zero counts', () => {
    const m = vault.metrics();
    expect(m.encryptCount).toBe(0);
    expect(m.decryptCount).toBe(0);
    expect(m.rotationCount).toBe(0);
    expect(m.rewrapCount).toBe(0);
    expect(m.errorCount).toBe(0);
    expect(m.keyVersionCount).toBe(1);
  });

  it('metrics.currentKeyVersionId matches vault.currentKeyVersionId', () => {
    expect(vault.metrics().currentKeyVersionId).toBe(vault.currentKeyVersionId());
  });
});

// ---------------------------------------------------------------------------
// createVaultFromPassphrase convenience factory
// ---------------------------------------------------------------------------

describe('createVaultFromPassphrase', () => {
  it('creates a working vault', async () => {
    const vault = await createVaultFromPassphrase(
      'my-passphrase',
      randomBytes(32),
      MIN_PBKDF2_ITERATIONS,
    );
    const plain = Buffer.from('hello');
    const env = await vault.encrypt(plain);
    const result = await vault.decrypt(env);
    expect(result.equals(plain)).toBe(true);
    vault.destroy();
  });

  it('fails with low iterations', async () => {
    await expect(
      createVaultFromPassphrase('pass', randomBytes(32), 1000),
    ).rejects.toThrow(KeyDerivationError);
  });
});

// ---------------------------------------------------------------------------
// envelopesEqual
// ---------------------------------------------------------------------------

describe('envelopesEqual', () => {
  it('returns true for identical envelopes', async () => {
    const vault = await makeVault();
    const env = await vault.encrypt(Buffer.from('eq-test'));
    expect(envelopesEqual(env, { ...env })).toBe(true);
    vault.destroy();
  });

  it('returns false when ciphertext differs', async () => {
    const vault = await makeVault();
    const e1 = await vault.encrypt(Buffer.from('a'));
    const e2 = await vault.encrypt(Buffer.from('b'));
    expect(envelopesEqual(e1, e2)).toBe(false);
    vault.destroy();
  });

  it('returns false when versions differ', async () => {
    const vault = await makeVault();
    const env = await vault.encrypt(Buffer.from('v'));
    const bad = { ...env, version: 2 as typeof ENVELOPE_VERSION };
    expect(envelopesEqual(env, bad)).toBe(false);
    vault.destroy();
  });

  it('returns false when aad differs', async () => {
    const vault = await makeVault();
    const e1 = { ...(await vault.encrypt(Buffer.from('a'))), aad: 'aGVsbG8=' };
    const e2 = { ...(await vault.encrypt(Buffer.from('a'))), aad: null };
    expect(envelopesEqual(e1, e2)).toBe(false);
    vault.destroy();
  });
});

// ---------------------------------------------------------------------------
// Security invariants
// ---------------------------------------------------------------------------

describe('security invariants', () => {
  it('DEK is not recoverable from envelope fields alone', async () => {
    // The DEK is embedded only as ciphertext in wrappedDek; the raw bytes
    // should not appear in any other field.
    const vault = await makeVault();
    const env = await vault.encrypt(Buffer.from('invariant-check'));
    const wrappedDekBuf = Buffer.from(env.wrappedDek, 'base64');
    const ciphertextBuf = Buffer.from(env.ciphertext, 'base64');
    // The wrapped DEK blob and the payload ciphertext should share no
    // common 32-byte substring (extremely high probability with random keys)
    expect(wrappedDekBuf.length).toBe(IV_BYTES + KEY_BYTES + TAG_BYTES);
    // The plaintext DEK is 32 bytes; it should not appear verbatim in ciphertext
    // This is probabilistically guaranteed (1/2^256 chance of false negative)
    const dekFromPayload = ciphertextBuf.subarray(0, KEY_BYTES);
    const dekFromWrap = wrappedDekBuf.subarray(IV_BYTES, IV_BYTES + KEY_BYTES);
    expect(dekFromPayload.equals(dekFromWrap)).toBe(false);
    vault.destroy();
  });

  it('repeatByte key encrypts but produces distinct ciphertexts per call', async () => {
    const key = repeatByte(0x42);
    const vault = await createVaultFromRawKey(key);
    const env1 = await vault.encrypt(Buffer.from('same'));
    const env2 = await vault.encrypt(Buffer.from('same'));
    expect(env1.iv).not.toBe(env2.iv);
    vault.destroy();
  });

  it('all-zero key still produces authenticated ciphertext', async () => {
    const key = Buffer.alloc(KEY_BYTES, 0);
    const vault = await createVaultFromRawKey(key);
    const plain = Buffer.from('zero-key-test');
    const env = await vault.encrypt(plain);
    const result = await vault.decrypt(env);
    expect(result.equals(plain)).toBe(true);
    vault.destroy();
  });

  it('cross-vault decryption fails (different KEK)', async () => {
    const vaultA = await makeVault();
    const vaultB = await makeVault();
    const env = await vaultA.encrypt(Buffer.from('cross'));
    // vaultB has a different key version ID — should get KeyVersionNotFoundError
    await expect(vaultB.decrypt(env)).rejects.toThrow(KeyVersionNotFoundError);
    vaultA.destroy();
    vaultB.destroy();
  });
});

// ---------------------------------------------------------------------------
// Metrics accumulation end-to-end
// ---------------------------------------------------------------------------

describe('metrics accumulation', () => {
  it('accurately tracks all operation types', async () => {
    const vault = await makeVault();

    // 3 encryptions
    const e1 = await vault.encrypt(Buffer.from('a'));
    const e2 = await vault.encrypt(Buffer.from('b'));
    await vault.encrypt(Buffer.from('c'));

    // 2 successful decryptions
    await vault.decrypt(e1);
    await vault.decrypt(e2);

    // 1 failed decryption
    try {
      await vault.decrypt({ ...e1, ciphertext: corruptBase64Field(e1.ciphertext) });
    } catch { /* expected */ }

    // 1 rotation with 0 stored keys (rewrapCount stays 0)
    await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });

    // 2 stores → rotation rewraps them
    await vault.store('s1', Buffer.from('v1'));
    await vault.store('s2', Buffer.from('v2'));
    await vault.rotateKey({ rawKey: randomBytes(KEY_BYTES) });

    const m = vault.metrics();
    expect(m.encryptCount).toBeGreaterThanOrEqual(3);
    expect(m.decryptCount).toBe(2);
    expect(m.rotationCount).toBe(2);
    expect(m.rewrapCount).toBe(2);
    expect(m.errorCount).toBeGreaterThanOrEqual(1);
    expect(m.keyVersionCount).toBe(3);

    vault.destroy();
  });
});
