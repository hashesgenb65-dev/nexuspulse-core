/**
 * The file-backed audit ledger: records survive a reopen, a changed or removed record stops the open,
 * a crash-cut last line is repaired, and a failed write leaves no record behind.
 */

import { describe, it, expect, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { AuditStoreError, openDurableAudit, type DurableAudit } from '../../src/audit/durable_audit';

const dirs: string[] = [];
const opened: DurableAudit[] = [];

afterEach(() => {
  for (const audit of opened.splice(0)) audit.close();
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

function fileIn(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'durable-audit-'));
  dirs.push(dir);
  return path.join(dir, 'nested', 'audit.log');
}

function open(filePath: string): DurableAudit {
  const audit = openDurableAudit(filePath);
  opened.push(audit);
  return audit;
}

function lines(filePath: string): string[] {
  return fs.readFileSync(filePath, 'utf8').split('\n').filter((line) => line.length > 0);
}

function writeLines(filePath: string, content: string[]): void {
  fs.writeFileSync(filePath, content.join('\n') + '\n');
}

describe('opening and storing', () => {
  it('creates the file and its directory with owner-only permissions', () => {
    const filePath = fileIn();
    open(filePath);
    expect(fs.existsSync(filePath)).toBe(true);
    expect(fs.statSync(filePath).mode & 0o777).toBe(0o600);
  });

  it('keeps records across a reopen, with the same content', () => {
    const filePath = fileIn();
    const first = open(filePath);
    first.ledger.append('edge-01', 'SET', 'kv:a', { index: 1, term: 1 });
    first.ledger.append('dash', 'DELETE', 'kv:a', { index: 2, term: 1 });
    first.close();

    const second = open(filePath);
    expect(second.loaded).toBe(2);
    expect(second.repairedTail).toBe(false);
    expect(second.ledger.query({ actorId: 'dash' })[0]).toMatchObject({ action: 'DELETE', resource: 'kv:a' });
    expect(second.ledger.verifyIntegrity().isValid).toBe(true);
  });

  it('continues the chain after a reopen', () => {
    const filePath = fileIn();
    const first = open(filePath);
    first.ledger.append('u', 'SET', 'kv:a', {});
    first.close();

    const second = open(filePath);
    second.ledger.append('u', 'SET', 'kv:b', {});
    second.close();

    const third = open(filePath);
    expect(third.loaded).toBe(2);
    expect(third.ledger.verifyIntegrity().isValid).toBe(true);
  });

  it('writes one JSON line per record', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    audit.ledger.append('u', 'SET', 'kv:a', {});
    audit.ledger.append('u', 'SET', 'kv:b', {});
    expect(lines(filePath)).toHaveLength(2);
    for (const line of lines(filePath)) expect(() => JSON.parse(line)).not.toThrow();
  });

  it('starts empty when the file does not exist yet', () => {
    const audit = open(fileIn());
    expect(audit.loaded).toBe(0);
    expect(audit.ledger.size()).toBe(0);
  });

  it('rejects an empty path', () => {
    expect(() => openDurableAudit('')).toThrow(AuditStoreError);
  });
});

describe('a failed or closed store', () => {
  it('leaves no record behind when the file cannot be written', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    audit.ledger.append('u', 'SET', 'kv:a', {});
    audit.close();

    expect(() => audit.ledger.append('u', 'SET', 'kv:b', {})).toThrow(AuditStoreError);
    expect(audit.ledger.size()).toBe(1);
    expect(lines(filePath)).toHaveLength(1);
  });

  it('close can be called more than once', () => {
    const audit = open(fileIn());
    expect(() => {
      audit.close();
      audit.close();
    }).not.toThrow();
  });
});

describe('tampering', () => {
  it('refuses to open a file in which a record was changed', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    audit.ledger.append('edge-01', 'SET', 'kv:a', {});
    audit.ledger.append('edge-01', 'SET', 'kv:b', {});
    audit.close();

    const content = lines(filePath).map((line) => JSON.parse(line) as Record<string, unknown>);
    content[0]!.actorId = 'someone-else';
    writeLines(filePath, content.map((record) => JSON.stringify(record)));

    expect(() => openDurableAudit(filePath)).toThrow(AuditStoreError);
    expect(() => openDurableAudit(filePath)).toThrow(/altered|broken/);
  });

  it('refuses to open a file in which a record was removed from the middle', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    for (let i = 0; i < 3; i++) audit.ledger.append('u', 'SET', `kv:${i}`, {});
    audit.close();

    const kept = lines(filePath);
    writeLines(filePath, [kept[0]!, kept[2]!]);

    expect(() => openDurableAudit(filePath)).toThrow(/does not link|broken/);
  });

  it('refuses to open a file whose records were reordered', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    audit.ledger.append('u', 'SET', 'kv:a', {});
    audit.ledger.append('u', 'SET', 'kv:b', {});
    audit.close();

    const [first, second] = lines(filePath);
    writeLines(filePath, [second!, first!]);

    expect(() => openDurableAudit(filePath)).toThrow(AuditStoreError);
  });

  it('reports the line of a complete line that is not JSON', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    audit.ledger.append('u', 'SET', 'kv:a', {});
    audit.ledger.append('u', 'SET', 'kv:b', {});
    audit.close();

    const [first, second] = lines(filePath);
    writeLines(filePath, [first!, '{not json', second!]);

    const error = (() => {
      try {
        openDurableAudit(filePath);
        return undefined;
      } catch (err) {
        return err;
      }
    })();
    expect(error).toBeInstanceOf(AuditStoreError);
    expect((error as AuditStoreError).line).toBe(2);
  });
});

describe('crash recovery', () => {
  it('drops a last line that a crash cut short, and trims the file', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    audit.ledger.append('u', 'SET', 'kv:a', {});
    audit.close();

    const complete = fs.readFileSync(filePath);
    fs.writeFileSync(filePath, Buffer.concat([complete, Buffer.from('{"recordId":"aud_partial')]));

    const reopened = open(filePath);
    expect(reopened.repairedTail).toBe(true);
    expect(reopened.loaded).toBe(1);
    expect(fs.readFileSync(filePath)).toEqual(complete);
    reopened.ledger.append('u', 'SET', 'kv:b', {});
    expect(open(filePath).ledger.verifyIntegrity().isValid).toBe(true);
  });

  it('keeps a complete last record that is missing its newline', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    audit.ledger.append('u', 'SET', 'kv:a', {});
    audit.close();

    const withoutNewline = fs.readFileSync(filePath).subarray(0, -1);
    fs.writeFileSync(filePath, withoutNewline);

    const reopened = open(filePath);
    expect(reopened.repairedTail).toBe(false);
    expect(reopened.loaded).toBe(1);
    reopened.ledger.append('u', 'SET', 'kv:b', {});
    expect(lines(filePath)).toHaveLength(2);
    expect(open(filePath).loaded).toBe(2);
  });

  it('does not treat a damaged line in the middle as crash debris', () => {
    const filePath = fileIn();
    const audit = open(filePath);
    audit.ledger.append('u', 'SET', 'kv:a', {});
    audit.ledger.append('u', 'SET', 'kv:b', {});
    audit.close();

    const [first, second] = lines(filePath);
    fs.writeFileSync(filePath, `${first}\n{broken\n${second}\n`);
    expect(() => openDurableAudit(filePath)).toThrow(AuditStoreError);
  });
});
