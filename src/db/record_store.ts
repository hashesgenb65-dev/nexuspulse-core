/**
 * NexusPulse persistence layer: a single-file, append-only record store.
 *
 * Layout. The file is a sequence of records. Each record is a 12-byte header
 * followed by a UTF-8 JSON body:
 *
 *   offset  size  field
 *   0       4     magic 0x4e504442 ("NPDB"), big-endian
 *   4       4     body length in bytes, big-endian
 *   8       4     CRC-32 of the body (see ./crc32), big-endian
 *   12      n     body: {"op":"put","key":"...","value":...} or {"op":"del","key":"..."}
 *
 * Guarantees.
 * - Every value is checked against the store's schema before it is written, and
 *   again when the file is replayed, so a schema change cannot silently load
 *   bad data.
 * - A crash in the middle of an append leaves at most one damaged record at the
 *   end of the file. Opening the store drops that tail and reports how many bytes
 *   were removed. Damage anywhere else is reported as corruption and the store
 *   refuses to open, because silently skipping it could lose later writes.
 * - Compaction rewrites the live state to a temporary file, syncs it, and renames
 *   it over the log, so readers see either the old file or the new one.
 * - An exclusive lock file prevents two processes from writing the same store.
 *
 * Limits. The whole file is read into memory when the store opens, and the live
 * state is kept in memory too. This store suits configuration, metadata and
 * bounded working sets, not large datasets.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { crc32 } from './crc32.js';
import { defineSchema, validate, type Schema } from './schema.js';

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Base class for errors raised by the storage layer. Each subclass has a stable `code`. */
export class StorageError extends Error {
  public readonly code: string;
  public readonly details: Readonly<Record<string, unknown>>;

  constructor(code: string, message: string, details: Record<string, unknown> = {}) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = new.target.name;
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

/** Invalid store options. Code: STORAGE_INVALID_OPTION. */
export class StorageOptionError extends StorageError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('STORAGE_INVALID_OPTION', message, details);
  }
}

/** A key is not a non-empty string within the byte limit. Code: STORAGE_INVALID_KEY. */
export class StorageKeyError extends StorageError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('STORAGE_INVALID_KEY', message, details);
  }
}

/** A value does not match the schema. Code: STORAGE_VALIDATION_FAILED. */
export class StorageValidationError extends StorageError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('STORAGE_VALIDATION_FAILED', message, details);
  }
}

/** A record would exceed the payload limit. Code: STORAGE_RECORD_TOO_LARGE. */
export class StorageRecordTooLargeError extends StorageError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('STORAGE_RECORD_TOO_LARGE', message, details);
  }
}

/** Another process holds the store's lock file. Code: STORAGE_LOCKED. */
export class StorageLockedError extends StorageError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('STORAGE_LOCKED', message, details);
  }
}

/** The store was used after close(). Code: STORAGE_CLOSED. */
export class StorageClosedError extends StorageError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('STORAGE_CLOSED', message, details);
  }
}

/** The file contains damage that is not a torn tail, or records that fail the schema. Code: STORAGE_CORRUPT. */
export class StorageCorruptionError extends StorageError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('STORAGE_CORRUPT', message, details);
  }
}

// ---------------------------------------------------------------------------
// Constants and public types
// ---------------------------------------------------------------------------

/** Record magic, "NPDB" in ASCII. */
export const RECORD_MAGIC = 0x4e504442;
/** Bytes in a record header. */
export const HEADER_BYTES = 12;
/** Maximum key length in UTF-8 bytes. */
export const MAX_KEY_BYTES = 256;
/** Maximum record body length in bytes. */
export const MAX_PAYLOAD_BYTES = 1024 * 1024;

/** Options for {@link RecordStore.open}. */
export interface StoreOptions {
  /** Path of the log file. Its directory must exist. The file is created if absent. */
  readonly file: string;
  /** Schema every stored value must satisfy. Raw definitions are accepted and normalised. */
  readonly schema: Schema;
  /** Whether each append is flushed to disk with fsync. Default true. */
  readonly fsync?: boolean;
}

/** Counters describing the store since it was opened. */
export interface StoreStats {
  /** Live keys. */
  readonly entries: number;
  /** Put records written since open. */
  readonly appends: number;
  /** Delete records written since open. */
  readonly deletes: number;
  /** Successful compactions since open. */
  readonly compactions: number;
  /** Bytes removed from a torn tail when the store was opened. */
  readonly recoveredTornBytes: number;
  /** Current size of the log file in bytes. */
  readonly fileBytes: number;
}

/** Result of {@link RecordStore.compact}. */
export interface CompactResult {
  readonly bytesBefore: number;
  readonly bytesAfter: number;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

interface ResolvedOptions {
  readonly file: string;
  readonly dir: string;
  readonly lockFile: string;
  readonly schema: Schema;
  readonly fsync: boolean;
}

interface Replay {
  readonly entries: Map<string, string>;
  readonly goodBytes: number;
  readonly tornBytes: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isValidKey(key: string): boolean {
  return key.length > 0 && Buffer.byteLength(key, 'utf8') <= MAX_KEY_BYTES;
}

function assertKey(key: unknown): asserts key is string {
  if (typeof key !== 'string') {
    throw new StorageKeyError('key must be a string', { type: typeof key });
  }
  if (key.length === 0) throw new StorageKeyError('key must not be empty');
  const bytes = Buffer.byteLength(key, 'utf8');
  if (bytes > MAX_KEY_BYTES) {
    throw new StorageKeyError(`key is ${bytes} bytes, above the ${MAX_KEY_BYTES}-byte limit`, { bytes });
  }
}

function resolveOptions(options: unknown): ResolvedOptions {
  if (!isRecord(options)) throw new StorageOptionError('options must be an object');
  const file = options['file'];
  if (typeof file !== 'string' || file.length === 0) {
    throw new StorageOptionError('file must be a non-empty string');
  }
  const fsyncOption = options['fsync'];
  if (fsyncOption !== undefined && typeof fsyncOption !== 'boolean') {
    throw new StorageOptionError('fsync must be a boolean');
  }
  const resolvedFile = path.resolve(file);
  const dir = path.dirname(resolvedFile);
  let isDirectory = false;
  try {
    isDirectory = fs.statSync(dir).isDirectory();
  } catch {
    isDirectory = false;
  }
  if (!isDirectory) throw new StorageOptionError(`directory does not exist: ${dir}`, { dir });

  let schema: Schema;
  try {
    schema = defineSchema(options['schema']);
  } catch (err) {
    throw new StorageOptionError('schema is invalid', { reason: (err as Error).message });
  }
  return {
    file: resolvedFile,
    dir,
    lockFile: `${resolvedFile}.lock`,
    schema,
    fsync: fsyncOption ?? true,
  };
}

function encodeRecord(body: Buffer): Buffer {
  const out = Buffer.allocUnsafe(HEADER_BYTES + body.length);
  out.writeUInt32BE(RECORD_MAGIC, 0);
  out.writeUInt32BE(body.length, 4);
  out.writeUInt32BE(crc32(body), 8);
  body.copy(out, HEADER_BYTES);
  return out;
}

function putBody(key: string, valueJson: string): Buffer {
  return Buffer.from(`{"op":"put","key":${JSON.stringify(key)},"value":${valueJson}}`, 'utf8');
}

function delBody(key: string): Buffer {
  return Buffer.from(`{"op":"del","key":${JSON.stringify(key)}}`, 'utf8');
}

function isZeroFrom(buf: Buffer, offset: number): boolean {
  for (let i = offset; i < buf.length; i++) {
    if (buf[i] !== 0) return false;
  }
  return true;
}

function writeAll(fd: number, data: Buffer): void {
  let written = 0;
  while (written < data.length) {
    written += fs.writeSync(fd, data, written, data.length - written);
  }
}

function fsyncDirectory(dir: string): void {
  // Directory fsync is not supported on every platform. The rename has already
  // happened, so a failure here only weakens durability of the rename itself.
  let fd: number | undefined;
  try {
    fd = fs.openSync(dir, 'r');
    fs.fsyncSync(fd);
  } catch {
    /* ignored: best effort */
  } finally {
    if (fd !== undefined) {
      try {
        fs.closeSync(fd);
      } catch {
        /* ignored */
      }
    }
  }
}

function acquireLock(lockFile: string): void {
  let fd: number;
  try {
    fd = fs.openSync(lockFile, 'wx');
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'EEXIST') {
      throw new StorageLockedError(
        `store is locked by another process (remove ${lockFile} only if no process is using the store)`,
        { lockFile },
      );
    }
    throw err;
  }
  try {
    writeAll(fd, Buffer.from(`${process.pid}\n`, 'utf8'));
  } finally {
    fs.closeSync(fd);
  }
}

function releaseLock(lockFile: string): void {
  fs.rmSync(lockFile, { force: true });
}

function applyRecord(
  entries: Map<string, string>,
  body: Buffer,
  schema: Schema,
  corrupt: (reason: string, extra?: Record<string, unknown>) => StorageCorruptionError,
): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body.toString('utf8')) as unknown;
  } catch {
    throw corrupt('record body is not valid JSON');
  }
  if (!isRecord(parsed)) throw corrupt('record body is not an object');
  const key = parsed['key'];
  if (typeof key !== 'string' || !isValidKey(key)) throw corrupt('record has an invalid key');

  const op = parsed['op'];
  if (op === 'put') {
    const value = parsed['value'];
    const result = validate(schema, value);
    if (!result.valid) {
      throw corrupt('stored value does not match the schema', { issues: result.issues });
    }
    entries.set(key, JSON.stringify(value));
  } else if (op === 'del') {
    entries.delete(key);
  } else {
    throw corrupt('record has an unknown operation');
  }
}

function replay(buf: Buffer, schema: Schema, file: string): Replay {
  const entries = new Map<string, string>();
  const corrupt = (offset: number) => (reason: string, extra: Record<string, unknown> = {}) =>
    new StorageCorruptionError(`${file}: corrupt record at byte ${offset}: ${reason}`, {
      file,
      offset,
      ...extra,
    });

  let offset = 0;
  while (offset < buf.length) {
    if (buf.length - offset < HEADER_BYTES) break; // torn header at the tail

    if (buf.readUInt32BE(offset) !== RECORD_MAGIC) {
      if (isZeroFrom(buf, offset)) break; // zero-filled tail left by a crash
      throw corrupt(offset)('bad magic number');
    }

    const length = buf.readUInt32BE(offset + 4);
    if (length > MAX_PAYLOAD_BYTES) throw corrupt(offset)(`length ${length} exceeds the payload limit`);

    const end = offset + HEADER_BYTES + length;
    if (end > buf.length) break; // torn payload at the tail

    const body = buf.subarray(offset + HEADER_BYTES, end);
    if (crc32(body) !== buf.readUInt32BE(offset + 8)) {
      if (end === buf.length) break; // damaged final record: treated as a torn write
      throw corrupt(offset)('checksum mismatch');
    }

    applyRecord(entries, body, schema, corrupt(offset));
    offset = end;
  }
  return { entries, goodBytes: offset, tornBytes: buf.length - offset };
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/**
 * A typed, schema-checked key-value store backed by one append-only file.
 * Create instances with {@link RecordStore.open}. Operations are synchronous.
 *
 * @typeParam T - The value type stored under each key.
 */
export class RecordStore<T = unknown> {
  private readonly file: string;
  private readonly lockFile: string;
  private readonly schema: Schema;
  private readonly fsyncEnabled: boolean;
  private readonly map = new Map<string, string>();
  private readonly counters = { appends: 0, deletes: 0, compactions: 0, recoveredTornBytes: 0 };
  private fd: number;
  private closed = false;

  private constructor(resolved: ResolvedOptions, fd: number, loaded: Replay) {
    this.file = resolved.file;
    this.lockFile = resolved.lockFile;
    this.schema = resolved.schema;
    this.fsyncEnabled = resolved.fsync;
    this.fd = fd;
    for (const [key, json] of loaded.entries) this.map.set(key, json);
    this.counters.recoveredTornBytes = loaded.tornBytes;
  }

  /**
   * Opens (or creates) a store. Takes the lock, replays the log, drops a torn
   * tail if there is one, and returns a ready store.
   *
   * @throws {StorageOptionError} If the options are invalid or the directory is missing.
   * @throws {StorageLockedError} If another process holds the lock.
   * @throws {StorageCorruptionError} If the file is damaged beyond a torn tail or
   *   contains values that fail the schema.
   */
  static open<T = unknown>(options: StoreOptions): RecordStore<T> {
    const resolved = resolveOptions(options);
    acquireLock(resolved.lockFile);
    try {
      const existing = fs.existsSync(resolved.file) ? fs.readFileSync(resolved.file) : Buffer.alloc(0);
      const loaded = replay(existing, resolved.schema, resolved.file);
      if (loaded.tornBytes > 0) fs.truncateSync(resolved.file, loaded.goodBytes);
      const fd = fs.openSync(resolved.file, 'a');
      return new RecordStore<T>(resolved, fd, loaded);
    } catch (err) {
      releaseLock(resolved.lockFile);
      throw err;
    }
  }

  /** Number of live keys. */
  get size(): number {
    return this.map.size;
  }

  /**
   * Stores a value under a key, replacing any previous value. The value is
   * validated first, and nothing is written if validation fails.
   *
   * @throws {StorageClosedError} If the store is closed.
   * @throws {StorageKeyError} If the key is empty, not a string, or too long.
   * @throws {StorageValidationError} If the value does not match the schema.
   * @throws {StorageRecordTooLargeError} If the record exceeds {@link MAX_PAYLOAD_BYTES}.
   */
  put(key: string, value: T): void {
    this.assertOpen();
    assertKey(key);
    const result = validate(this.schema, value);
    if (!result.valid) {
      throw new StorageValidationError(`value for "${key}" does not match the schema`, {
        key,
        issues: result.issues,
      });
    }
    const valueJson = JSON.stringify(value);
    const body = putBody(key, valueJson);
    this.checkSize(body.length, key);
    this.append(encodeRecord(body));
    this.map.set(key, valueJson);
    this.counters.appends++;
  }

  /**
   * Returns a fresh copy of the stored value, so mutating it does not change the store.
   *
   * @returns The value, or undefined if the key is absent.
   * @throws {StorageClosedError} If the store is closed.
   * @throws {StorageKeyError} If the key is invalid.
   */
  get(key: string): T | undefined {
    this.assertOpen();
    assertKey(key);
    const json = this.map.get(key);
    return json === undefined ? undefined : (JSON.parse(json) as T);
  }

  /** Whether a key is present. */
  has(key: string): boolean {
    this.assertOpen();
    assertKey(key);
    return this.map.has(key);
  }

  /**
   * Removes a key. Nothing is written when the key is absent.
   *
   * @returns True if the key existed.
   * @throws {StorageClosedError} If the store is closed.
   * @throws {StorageKeyError} If the key is invalid.
   */
  delete(key: string): boolean {
    this.assertOpen();
    assertKey(key);
    if (!this.map.has(key)) return false;
    const body = delBody(key);
    this.append(encodeRecord(body));
    this.map.delete(key);
    this.counters.deletes++;
    return true;
  }

  /** Live keys in insertion order. Overwriting a key keeps its original position. */
  keys(): string[] {
    this.assertOpen();
    return [...this.map.keys()];
  }

  /** Live entries in insertion order, each value a fresh copy. */
  *entries(): IterableIterator<[string, T]> {
    this.assertOpen();
    for (const [key, json] of this.map) {
      yield [key, JSON.parse(json) as T];
    }
  }

  /**
   * Rewrites the log so it holds only the live state. The new file is written
   * and synced under a temporary name, then renamed over the log.
   *
   * @throws {StorageClosedError} If the store is closed.
   */
  compact(): CompactResult {
    this.assertOpen();
    const bytesBefore = fs.fstatSync(this.fd).size;
    const chunks: Buffer[] = [];
    for (const [key, json] of this.map) {
      chunks.push(encodeRecord(putBody(key, json)));
    }
    const data = Buffer.concat(chunks);

    const tmp = `${this.file}.compact`;
    const tmpFd = fs.openSync(tmp, 'w');
    try {
      writeAll(tmpFd, data);
      fs.fsyncSync(tmpFd);
    } catch (err) {
      fs.closeSync(tmpFd);
      fs.rmSync(tmp, { force: true });
      throw err;
    }
    fs.closeSync(tmpFd);

    try {
      fs.renameSync(tmp, this.file);
    } catch (err) {
      fs.rmSync(tmp, { force: true });
      throw err;
    }
    fsyncDirectory(path.dirname(this.file));

    const oldFd = this.fd;
    this.fd = fs.openSync(this.file, 'a');
    fs.closeSync(oldFd);
    this.counters.compactions++;
    return { bytesBefore, bytesAfter: data.length };
  }

  /** Current counters and file size. */
  stats(): StoreStats {
    this.assertOpen();
    const snapshot: StoreStats = {
      entries: this.map.size,
      appends: this.counters.appends,
      deletes: this.counters.deletes,
      compactions: this.counters.compactions,
      recoveredTornBytes: this.counters.recoveredTornBytes,
      fileBytes: fs.fstatSync(this.fd).size,
    };
    return Object.freeze(snapshot);
  }

  /**
   * Closes the file and releases the lock. Every write is already appended (and
   * synced when `fsync` is enabled), so there is nothing to flush. Safe to call
   * more than once.
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.map.clear();
    try {
      fs.closeSync(this.fd);
    } finally {
      releaseLock(this.lockFile);
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new StorageClosedError(`store is closed: ${this.file}`);
  }

  private checkSize(bytes: number, key: string): void {
    if (bytes > MAX_PAYLOAD_BYTES) {
      throw new StorageRecordTooLargeError(
        `record for "${key}" is ${bytes} bytes, above the ${MAX_PAYLOAD_BYTES}-byte limit`,
        { key, bytes },
      );
    }
  }

  private append(record: Buffer): void {
    writeAll(this.fd, record);
    if (this.fsyncEnabled) fs.fsyncSync(this.fd);
  }
}
