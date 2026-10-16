/**
 * NexusPulse runtime settings, read from environment variables.
 *
 * Every variable is optional and has a default. An empty value counts as unset.
 * All problems are collected before failing, so one run reports every mistake.
 *
 * | Variable                       | Default     | Range / values                   |
 * |--------------------------------|-------------|----------------------------------|
 * | NEXUSPULSE_HTTP_HOST           | 127.0.0.1   | host name or IP, no spaces       |
 * | NEXUSPULSE_HTTP_PORT           | 8080        | 0-65535 (0 = any free port)      |
 * | NEXUSPULSE_TCP_HOST            | 127.0.0.1   | host name or IP, no spaces       |
 * | NEXUSPULSE_TCP_PORT            | 9000        | 0-65535 (0 = any free port)      |
 * | NEXUSPULSE_DATA_DIR            | ./data      | directory, created if missing    |
 * | NEXUSPULSE_MAX_CONNECTIONS     | 64          | 1-10000                          |
 * | NEXUSPULSE_IDLE_TIMEOUT_MS     | 60000       | 1-86400000                       |
 * | NEXUSPULSE_MAX_BATCH           | 500         | 1-10000                          |
 * | NEXUSPULSE_MAX_BODY_BYTES      | 1048576     | 1-16777216                       |
 * | NEXUSPULSE_QUEUE_CAPACITY      | 50000       | 1-1000000                        |
 * | NEXUSPULSE_BACKPRESSURE        | drop-oldest | drop-oldest, drop-newest, reject |
 * | NEXUSPULSE_LOG_LEVEL           | info        | debug, info, warn, error         |
 * | NEXUSPULSE_TOKENS_FILE         | (unset)     | path to a 0600 token file        |
 * | NEXUSPULSE_ALERT_WEBHOOK_URL   | (unset)     | http or https URL                |
 * | NEXUSPULSE_ALERT_MIN_SEVERITY  | warning     | info, warning, critical          |
 * | NEXUSPULSE_ALERT_DEDUP_MS      | 60000       | 0-86400000                       |
 *
 * When NEXUSPULSE_TOKENS_FILE is unset the HTTP API accepts unauthenticated
 * requests. The app logs a warning at start-up in that case.
 */

export type BackpressureSetting = 'drop-oldest' | 'drop-newest' | 'reject';
export type LogLevelSetting = 'debug' | 'info' | 'warn' | 'error';
export type AlertSeveritySetting = 'info' | 'warning' | 'critical';

export interface Settings {
  readonly httpHost: string;
  readonly httpPort: number;
  readonly tcpHost: string;
  readonly tcpPort: number;
  readonly dataDir: string;
  readonly maxConnections: number;
  readonly idleTimeoutMs: number;
  readonly maxBatch: number;
  readonly maxBodyBytes: number;
  readonly queueCapacity: number;
  readonly backpressure: BackpressureSetting;
  readonly logLevel: LogLevelSetting;
  readonly tokensFile: string | undefined;
  readonly alertWebhookUrl: string | undefined;
  readonly alertMinSeverity: AlertSeveritySetting;
  readonly alertDedupMs: number;
}

/** One problem found while reading the environment. */
export interface SettingIssue {
  readonly variable: string;
  readonly message: string;
}

/** Raised when one or more variables are invalid. Code: SETTINGS_INVALID. */
export class SettingsError extends Error {
  public readonly code = 'SETTINGS_INVALID';
  public readonly issues: readonly SettingIssue[];

  constructor(issues: readonly SettingIssue[]) {
    super(`invalid settings: ${issues.map((i) => `${i.variable} ${i.message}`).join('; ')}`);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'SettingsError';
    this.issues = Object.freeze([...issues]);
  }
}

export type Environment = Readonly<Record<string, string | undefined>>;

const HOST_PATTERN = /^[A-Za-z0-9._:-]{1,255}$/;
const INTEGER_PATTERN = /^\d+$/;
const BACKPRESSURE_VALUES: readonly BackpressureSetting[] = ['drop-oldest', 'drop-newest', 'reject'];
const LOG_LEVEL_VALUES: readonly LogLevelSetting[] = ['debug', 'info', 'warn', 'error'];
const SEVERITY_VALUES: readonly AlertSeveritySetting[] = ['info', 'warning', 'critical'];

/** Reads a variable, treating an empty or whitespace-only value as unset. */
function raw(env: Environment, name: string): string | undefined {
  const value = env[name];
  if (value === undefined) return undefined;
  const trimmed = value.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

function readInteger(
  env: Environment,
  issues: SettingIssue[],
  name: string,
  fallback: number,
  min: number,
  max: number,
): number {
  const value = raw(env, name);
  if (value === undefined) return fallback;
  if (!INTEGER_PATTERN.test(value)) {
    issues.push({ variable: name, message: 'must be a whole number' });
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
    issues.push({ variable: name, message: `must be from ${min} to ${max}` });
    return fallback;
  }
  return parsed;
}

function readHost(env: Environment, issues: SettingIssue[], name: string, fallback: string): string {
  const value = raw(env, name);
  if (value === undefined) return fallback;
  if (!HOST_PATTERN.test(value)) {
    issues.push({ variable: name, message: 'must be a host name or IP address without spaces' });
    return fallback;
  }
  return value;
}

function readEnum<T extends string>(
  env: Environment,
  issues: SettingIssue[],
  name: string,
  fallback: T,
  allowed: readonly T[],
): T {
  const value = raw(env, name);
  if (value === undefined) return fallback;
  if (!(allowed as readonly string[]).includes(value)) {
    issues.push({ variable: name, message: `must be one of ${allowed.join(', ')}` });
    return fallback;
  }
  return value as T;
}

/** Reads an optional http or https URL. Returns undefined when the variable is unset. */
function readOptionalUrl(env: Environment, issues: SettingIssue[], name: string): string | undefined {
  const value = raw(env, name);
  if (value === undefined) return undefined;
  try {
    const url = new URL(value);
    if (url.protocol === 'http:' || url.protocol === 'https:') return url.toString();
  } catch {
    // Reported below.
  }
  issues.push({ variable: name, message: 'must be an absolute http or https URL' });
  return undefined;
}

/**
 * Parses and validates settings from an environment map.
 *
 * @throws {SettingsError} Listing every invalid variable.
 */
export function parseSettings(env: Environment): Settings {
  const issues: SettingIssue[] = [];

  const settings: Settings = {
    httpHost: readHost(env, issues, 'NEXUSPULSE_HTTP_HOST', '127.0.0.1'),
    httpPort: readInteger(env, issues, 'NEXUSPULSE_HTTP_PORT', 8080, 0, 65_535),
    tcpHost: readHost(env, issues, 'NEXUSPULSE_TCP_HOST', '127.0.0.1'),
    tcpPort: readInteger(env, issues, 'NEXUSPULSE_TCP_PORT', 9000, 0, 65_535),
    dataDir: raw(env, 'NEXUSPULSE_DATA_DIR') ?? './data',
    maxConnections: readInteger(env, issues, 'NEXUSPULSE_MAX_CONNECTIONS', 64, 1, 10_000),
    idleTimeoutMs: readInteger(env, issues, 'NEXUSPULSE_IDLE_TIMEOUT_MS', 60_000, 1, 86_400_000),
    maxBatch: readInteger(env, issues, 'NEXUSPULSE_MAX_BATCH', 500, 1, 10_000),
    maxBodyBytes: readInteger(env, issues, 'NEXUSPULSE_MAX_BODY_BYTES', 1_048_576, 1, 16_777_216),
    queueCapacity: readInteger(env, issues, 'NEXUSPULSE_QUEUE_CAPACITY', 50_000, 1, 1_000_000),
    backpressure: readEnum(env, issues, 'NEXUSPULSE_BACKPRESSURE', 'drop-oldest', BACKPRESSURE_VALUES),
    logLevel: readEnum(env, issues, 'NEXUSPULSE_LOG_LEVEL', 'info', LOG_LEVEL_VALUES),
    tokensFile: raw(env, 'NEXUSPULSE_TOKENS_FILE'),
    alertWebhookUrl: readOptionalUrl(env, issues, 'NEXUSPULSE_ALERT_WEBHOOK_URL'),
    alertMinSeverity: readEnum(env, issues, 'NEXUSPULSE_ALERT_MIN_SEVERITY', 'warning', SEVERITY_VALUES),
    alertDedupMs: readInteger(env, issues, 'NEXUSPULSE_ALERT_DEDUP_MS', 60_000, 0, 86_400_000),
  };

  if (settings.httpPort !== 0 && settings.httpPort === settings.tcpPort) {
    issues.push({ variable: 'NEXUSPULSE_TCP_PORT', message: 'must differ from NEXUSPULSE_HTTP_PORT' });
  }

  if (issues.length > 0) throw new SettingsError(issues);
  return Object.freeze(settings);
}
