/**
 * File-backed audit ledger.
 *
 * Each record is written as one JSON line and flushed to disk before the append returns, so an
 * acknowledged change is never lost in a crash. On open, the whole file is read and its chain is
 * verified; a record that was altered, removed or reordered stops the open. A last line that was
 * cut short by a crash is dropped, and the file is trimmed back to the last complete record.
 *
 * The file is created with owner-only permissions, because it names the actors and the keys they
 * changed.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { AuditChainError, AuditLedgerEngine, type AuditRecord } from './ledger.js';

/** Raised when the audit file cannot be opened, or holds lines that are not records. Code: AUDIT_STORE_INVALID. */
export class AuditStoreError extends Error {
  readonly code = 'AUDIT_STORE_INVALID' as const;
  readonly line: number | undefined;

  constructor(message: string, line?: number) {
    super(message);
    this.name = 'AuditStoreError';
    this.line = line;
  }
}

export interface DurableAudit {
  /** The ledger. Appends made through it are stored before they return. */
  readonly ledger: AuditLedgerEngine;
  /** Records that were loaded from the file when it was opened. */
  readonly loaded: number;
  /** True when a crash-cut last line was removed while opening. */
  readonly repairedTail: boolean;
  /** Stops accepting appends and closes the file. Safe to call more than once. */
  close(): void;
}

const FILE_MODE = 0o600;
const DIRECTORY_MODE = 0o700;

function syncDirectory(directory: string): void {
  let fd: number | undefined;
  try {
    fd = fs.openSync(directory, 'r');
    fs.fsyncSync(fd);
  } catch {
    // Directory sync is not supported on every platform. The file itself is synced on every append.
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
}

function writeAllSync(fd: number, bytes: Buffer): void {
  let offset = 0;
  while (offset < bytes.length) {
    offset += fs.writeSync(fd, bytes, offset, bytes.length - offset);
  }
}

interface ParsedFile {
  readonly records: AuditRecord[];
  /** Byte length of the complete, valid prefix of the file. */
  readonly validBytes: number;
  readonly tornTail: boolean;
  /** True when the final record is complete but lacks its newline. */
  readonly missingNewline: boolean;
}

/** Splits the file into records. Only the very last line may be malformed, and then it is treated as crash debris. */
function parseFile(bytes: Buffer): ParsedFile {
  const text = bytes.toString('utf8');
  const lines = text.split('\n');
  const endsWithNewline = text.endsWith('\n');
  // With a trailing newline the last split element is empty; without one it is the unterminated tail.
  const complete = lines.slice(0, -1);
  const tail = endsWithNewline ? undefined : lines[lines.length - 1];

  const records: AuditRecord[] = [];
  let validBytes = 0;
  for (const [index, line] of complete.entries()) {
    if (line.length === 0) {
      throw new AuditStoreError('the file contains an empty line', index + 1);
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      throw new AuditStoreError('a line is not valid JSON', index + 1);
    }
    records.push(parsed as AuditRecord);
    validBytes += Buffer.byteLength(line, 'utf8') + 1;
  }

  if (tail === undefined || tail.length === 0) {
    return { records, validBytes, tornTail: false, missingNewline: false };
  }
  // A final line without a newline: complete if it parses, otherwise a crash cut it short.
  try {
    records.push(JSON.parse(tail) as AuditRecord);
    return { records, validBytes: validBytes + Buffer.byteLength(tail, 'utf8'), tornTail: false, missingNewline: true };
  } catch {
    return { records, validBytes, tornTail: true, missingNewline: false };
  }
}

/**
 * Opens (or creates) the audit file at `filePath`, verifies its chain, and returns a ledger that
 * stores every new record.
 *
 * @throws {AuditStoreError} If the file cannot be read, or a complete line is not a record.
 * @throws {AuditChainError} If the records do not form a valid chain.
 */
export function openDurableAudit(filePath: string): DurableAudit {
  if (filePath.length === 0) throw new AuditStoreError('the audit file path must not be empty');
  const directory = path.dirname(filePath);
  fs.mkdirSync(directory, { recursive: true, mode: DIRECTORY_MODE });

  const existed = fs.existsSync(filePath);
  const bytes = existed ? fs.readFileSync(filePath) : Buffer.alloc(0);
  const parsed = parseFile(bytes);

  // The file is opened only after the chain has been verified, so a broken file is never appended to.
  let fd: number | undefined;
  let closed = false;
  const ledger = new AuditLedgerEngine({
    sink: (record) => {
      if (closed || fd === undefined) throw new AuditStoreError('the audit file is not open for writing');
      writeAllSync(fd, Buffer.from(`${JSON.stringify(record)}\n`, 'utf8'));
      fs.fsyncSync(fd);
    },
  });
  try {
    ledger.restore(parsed.records);
  } catch (error) {
    if (error instanceof AuditChainError) {
      throw new AuditStoreError(`the audit chain is broken: ${error.message}`, error.index + 1);
    }
    throw error;
  }

  // Trim a crash-cut tail before appending to the file.
  if (parsed.tornTail) fs.truncateSync(filePath, parsed.validBytes);

  fd = fs.openSync(filePath, 'a', FILE_MODE);
  if (!existed) syncDirectory(directory);
  if (parsed.missingNewline) writeAllSync(fd, Buffer.from('\n', 'utf8'));

  return {
    ledger,
    loaded: parsed.records.length,
    repairedTail: parsed.tornTail,
    close(): void {
      if (closed) return;
      closed = true;
      if (fd !== undefined) fs.closeSync(fd);
    },
  };
}
