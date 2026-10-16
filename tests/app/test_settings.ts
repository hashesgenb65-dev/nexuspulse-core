/**
 * Tests for environment settings parsing.
 */

import { describe, it, expect } from 'vitest';
import { parseSettings, SettingsError, type Environment, type Settings } from '../../src/app/settings.js';

function issuesOf(env: Environment): { variable: string; message: string }[] {
  try {
    parseSettings(env);
  } catch (err) {
    if (err instanceof SettingsError) return [...err.issues];
    throw err;
  }
  return [];
}

describe('parseSettings defaults', () => {
  it('returns documented defaults for an empty environment', () => {
    const s: Settings = parseSettings({});
    expect(s).toEqual({
      httpHost: '127.0.0.1',
      httpPort: 8080,
      tcpHost: '127.0.0.1',
      tcpPort: 9000,
      dataDir: './data',
      maxConnections: 64,
      idleTimeoutMs: 60_000,
      maxBatch: 500,
      maxBodyBytes: 1_048_576,
      queueCapacity: 50_000,
      backpressure: 'drop-oldest',
      logLevel: 'info',
      tokensFile: undefined,
      alertWebhookUrl: undefined,
      alertMinSeverity: 'warning',
      alertDedupMs: 60_000,
    });
  });

  it('returns a frozen object', () => {
    expect(Object.isFrozen(parseSettings({}))).toBe(true);
  });

  it('treats empty and whitespace-only values as unset', () => {
    const s = parseSettings({ NEXUSPULSE_HTTP_PORT: '', NEXUSPULSE_TCP_HOST: '   ', NEXUSPULSE_DATA_DIR: '  ' });
    expect(s.httpPort).toBe(8080);
    expect(s.tcpHost).toBe('127.0.0.1');
    expect(s.dataDir).toBe('./data');
  });

  it('ignores variables it does not know', () => {
    expect(() => parseSettings({ UNRELATED: 'x', NEXUSPULSE_UNKNOWN: 'y' })).not.toThrow();
  });
});

describe('parseSettings overrides', () => {
  it('reads every variable', () => {
    const s = parseSettings({
      NEXUSPULSE_HTTP_HOST: '0.0.0.0',
      NEXUSPULSE_HTTP_PORT: '8443',
      NEXUSPULSE_TCP_HOST: 'ingest.local',
      NEXUSPULSE_TCP_PORT: '9443',
      NEXUSPULSE_DATA_DIR: '/var/lib/nexuspulse',
      NEXUSPULSE_MAX_CONNECTIONS: '256',
      NEXUSPULSE_IDLE_TIMEOUT_MS: '5000',
      NEXUSPULSE_MAX_BATCH: '1000',
      NEXUSPULSE_MAX_BODY_BYTES: '2048',
      NEXUSPULSE_QUEUE_CAPACITY: '10000',
      NEXUSPULSE_BACKPRESSURE: 'reject',
      NEXUSPULSE_LOG_LEVEL: 'warn',
      NEXUSPULSE_TOKENS_FILE: '/etc/nexuspulse/tokens.json',
      NEXUSPULSE_ALERT_WEBHOOK_URL: 'https://hooks.example.internal/np',
      NEXUSPULSE_ALERT_MIN_SEVERITY: 'critical',
      NEXUSPULSE_ALERT_DEDUP_MS: '5000',
    });
    expect(s).toEqual({
      httpHost: '0.0.0.0',
      httpPort: 8443,
      tcpHost: 'ingest.local',
      tcpPort: 9443,
      dataDir: '/var/lib/nexuspulse',
      maxConnections: 256,
      idleTimeoutMs: 5000,
      maxBatch: 1000,
      maxBodyBytes: 2048,
      queueCapacity: 10000,
      backpressure: 'reject',
      logLevel: 'warn',
      tokensFile: '/etc/nexuspulse/tokens.json',
      alertWebhookUrl: 'https://hooks.example.internal/np',
      alertMinSeverity: 'critical',
      alertDedupMs: 5000,
    });
  });

  it('trims surrounding whitespace from values', () => {
    expect(parseSettings({ NEXUSPULSE_HTTP_PORT: ' 9090 ' }).httpPort).toBe(9090);
    expect(parseSettings({ NEXUSPULSE_LOG_LEVEL: ' debug ' }).logLevel).toBe('debug');
  });

  it('accepts the bounds of each range', () => {
    const s = parseSettings({
      NEXUSPULSE_MAX_CONNECTIONS: '10000',
      NEXUSPULSE_QUEUE_CAPACITY: '1',
      NEXUSPULSE_HTTP_PORT: '65535',
      NEXUSPULSE_TCP_PORT: '0',
    });
    expect(s.maxConnections).toBe(10_000);
    expect(s.queueCapacity).toBe(1);
    expect(s.httpPort).toBe(65_535);
  });

  it('accepts port 0 for both listeners', () => {
    const s = parseSettings({ NEXUSPULSE_HTTP_PORT: '0', NEXUSPULSE_TCP_PORT: '0' });
    expect(s.httpPort).toBe(0);
    expect(s.tcpPort).toBe(0);
  });

  it('accepts all three backpressure policies and all four log levels', () => {
    for (const policy of ['drop-oldest', 'drop-newest', 'reject']) {
      expect(parseSettings({ NEXUSPULSE_BACKPRESSURE: policy }).backpressure).toBe(policy);
    }
    for (const level of ['debug', 'info', 'warn', 'error']) {
      expect(parseSettings({ NEXUSPULSE_LOG_LEVEL: level }).logLevel).toBe(level);
    }
  });
});

describe('parseSettings validation', () => {
  it('rejects non-integer numbers', () => {
    expect(issuesOf({ NEXUSPULSE_HTTP_PORT: '80.5' })).toEqual([
      { variable: 'NEXUSPULSE_HTTP_PORT', message: 'must be a whole number' },
    ]);
    expect(issuesOf({ NEXUSPULSE_MAX_BATCH: '1e3' })).toHaveLength(1);
    expect(issuesOf({ NEXUSPULSE_MAX_BATCH: '-5' })).toHaveLength(1);
    expect(issuesOf({ NEXUSPULSE_MAX_BATCH: '12abc' })).toHaveLength(1);
  });

  it('rejects numbers outside their range', () => {
    expect(issuesOf({ NEXUSPULSE_HTTP_PORT: '65536' })).toEqual([
      { variable: 'NEXUSPULSE_HTTP_PORT', message: 'must be from 0 to 65535' },
    ]);
    expect(issuesOf({ NEXUSPULSE_MAX_CONNECTIONS: '0' })).toHaveLength(1);
    expect(issuesOf({ NEXUSPULSE_MAX_CONNECTIONS: '10001' })).toHaveLength(1);
    expect(issuesOf({ NEXUSPULSE_IDLE_TIMEOUT_MS: '86400001' })).toHaveLength(1);
    expect(issuesOf({ NEXUSPULSE_MAX_BODY_BYTES: '16777217' })).toHaveLength(1);
  });

  it('rejects integers that are too large to represent exactly', () => {
    expect(issuesOf({ NEXUSPULSE_QUEUE_CAPACITY: '9007199254740993' })).toHaveLength(1);
  });

  it('rejects host names with spaces or unusual characters', () => {
    expect(issuesOf({ NEXUSPULSE_HTTP_HOST: 'bad host' })).toEqual([
      { variable: 'NEXUSPULSE_HTTP_HOST', message: 'must be a host name or IP address without spaces' },
    ]);
    expect(issuesOf({ NEXUSPULSE_TCP_HOST: 'a/b' })).toHaveLength(1);
  });

  it('accepts IPv6 literals and host names', () => {
    expect(parseSettings({ NEXUSPULSE_HTTP_HOST: '::1' }).httpHost).toBe('::1');
    expect(parseSettings({ NEXUSPULSE_TCP_HOST: 'edge-01.example.internal' }).tcpHost).toBe('edge-01.example.internal');
  });

  it('rejects unknown enum values and lists the allowed ones', () => {
    const [issue] = issuesOf({ NEXUSPULSE_BACKPRESSURE: 'block' });
    expect(issue).toEqual({ variable: 'NEXUSPULSE_BACKPRESSURE', message: 'must be one of drop-oldest, drop-newest, reject' });
    expect(issuesOf({ NEXUSPULSE_LOG_LEVEL: 'verbose' })).toHaveLength(1);
  });

  it('is case-sensitive for enum values', () => {
    expect(issuesOf({ NEXUSPULSE_LOG_LEVEL: 'INFO' })).toHaveLength(1);
  });

  it('rejects a TCP port that equals the HTTP port', () => {
    expect(issuesOf({ NEXUSPULSE_HTTP_PORT: '7000', NEXUSPULSE_TCP_PORT: '7000' })).toEqual([
      { variable: 'NEXUSPULSE_TCP_PORT', message: 'must differ from NEXUSPULSE_HTTP_PORT' },
    ]);
  });

  it('allows equal ports when they are both 0', () => {
    expect(() => parseSettings({ NEXUSPULSE_HTTP_PORT: '0', NEXUSPULSE_TCP_PORT: '0' })).not.toThrow();
  });

  it('reports every invalid variable in one error', () => {
    const issues = issuesOf({
      NEXUSPULSE_HTTP_PORT: 'x',
      NEXUSPULSE_MAX_CONNECTIONS: '0',
      NEXUSPULSE_BACKPRESSURE: 'nope',
      NEXUSPULSE_LOG_LEVEL: 'loud',
    });
    expect(issues.map((i) => i.variable)).toEqual([
      'NEXUSPULSE_HTTP_PORT',
      'NEXUSPULSE_MAX_CONNECTIONS',
      'NEXUSPULSE_BACKPRESSURE',
      'NEXUSPULSE_LOG_LEVEL',
    ]);
  });

  it('includes every issue in the error message', () => {
    try {
      parseSettings({ NEXUSPULSE_MAX_BATCH: '0', NEXUSPULSE_LOG_LEVEL: 'loud' });
      expect.unreachable('expected SettingsError');
    } catch (err) {
      expect(err).toBeInstanceOf(SettingsError);
      const e = err as SettingsError;
      expect(e.code).toBe('SETTINGS_INVALID');
      expect(e.name).toBe('SettingsError');
      expect(e.message).toContain('NEXUSPULSE_MAX_BATCH');
      expect(e.message).toContain('NEXUSPULSE_LOG_LEVEL');
    }
  });

  it('freezes the issues array of the error', () => {
    try {
      parseSettings({ NEXUSPULSE_MAX_BATCH: '0' });
      expect.unreachable('expected SettingsError');
    } catch (err) {
      expect(Object.isFrozen((err as SettingsError).issues)).toBe(true);
    }
  });

  it('is an instance of Error with a usable prototype chain', () => {
    try {
      parseSettings({ NEXUSPULSE_MAX_BATCH: '0' });
    } catch (err) {
      expect(err).toBeInstanceOf(Error);
      expect(err).toBeInstanceOf(SettingsError);
    }
  });
});

describe('parseSettings alerting and authentication', () => {
  it('leaves the token file and webhook unset by default', () => {
    const s = parseSettings({});
    expect(s.tokensFile).toBeUndefined();
    expect(s.alertWebhookUrl).toBeUndefined();
    expect(s.alertMinSeverity).toBe('warning');
    expect(s.alertDedupMs).toBe(60_000);
  });

  it('treats an empty webhook or token file as unset', () => {
    const s = parseSettings({ NEXUSPULSE_ALERT_WEBHOOK_URL: '   ', NEXUSPULSE_TOKENS_FILE: '' });
    expect(s.alertWebhookUrl).toBeUndefined();
    expect(s.tokensFile).toBeUndefined();
  });

  it('accepts an http or https webhook URL and normalises it', () => {
    expect(parseSettings({ NEXUSPULSE_ALERT_WEBHOOK_URL: 'http://hooks.internal:8081/x' }).alertWebhookUrl).toBe(
      'http://hooks.internal:8081/x',
    );
    expect(parseSettings({ NEXUSPULSE_ALERT_WEBHOOK_URL: 'HTTPS://Hooks.Example.com' }).alertWebhookUrl).toBe(
      'https://hooks.example.com/',
    );
  });

  it('rejects a webhook that is not an absolute http or https URL', () => {
    expect(issuesOf({ NEXUSPULSE_ALERT_WEBHOOK_URL: 'ftp://files.example.com/x' })).toEqual([
      { variable: 'NEXUSPULSE_ALERT_WEBHOOK_URL', message: 'must be an absolute http or https URL' },
    ]);
    expect(issuesOf({ NEXUSPULSE_ALERT_WEBHOOK_URL: 'hooks.example.com' })).toHaveLength(1);
  });

  it('accepts each alert severity and rejects others', () => {
    for (const level of ['info', 'warning', 'critical']) {
      expect(parseSettings({ NEXUSPULSE_ALERT_MIN_SEVERITY: level }).alertMinSeverity).toBe(level);
    }
    expect(issuesOf({ NEXUSPULSE_ALERT_MIN_SEVERITY: 'page' })).toEqual([
      { variable: 'NEXUSPULSE_ALERT_MIN_SEVERITY', message: 'must be one of info, warning, critical' },
    ]);
  });

  it('accepts a dedup window of 0 and the maximum, and rejects values outside the range', () => {
    expect(parseSettings({ NEXUSPULSE_ALERT_DEDUP_MS: '0' }).alertDedupMs).toBe(0);
    expect(parseSettings({ NEXUSPULSE_ALERT_DEDUP_MS: '86400000' }).alertDedupMs).toBe(86_400_000);
    expect(issuesOf({ NEXUSPULSE_ALERT_DEDUP_MS: '86400001' })).toHaveLength(1);
    expect(issuesOf({ NEXUSPULSE_ALERT_DEDUP_MS: '-1' })).toHaveLength(1);
  });

  it('reports a bad webhook alongside other bad variables', () => {
    const variables = issuesOf({ NEXUSPULSE_ALERT_WEBHOOK_URL: 'nope', NEXUSPULSE_MAX_BATCH: '0' }).map((i) => i.variable);
    expect(variables).toEqual(['NEXUSPULSE_MAX_BATCH', 'NEXUSPULSE_ALERT_WEBHOOK_URL']);
  });
});
