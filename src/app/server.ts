/**
 * NexusPulse application: wires the engine, alert rules, the record store, the
 * HTTP API, TCP ingest, API authentication, the metrics endpoint and alert
 * delivery into one process with ordered start-up and shutdown.
 *
 * Start-up order: data directory and rule store, rules, token file (if set),
 * alert notifier, engine (background flushing), HTTP listener, TCP listener. If
 * any step fails, everything started so far is torn down and the error is
 * rethrown as an {@link AppError}.
 *
 * Shutdown order: HTTP and TCP listeners close first, so nothing new arrives.
 * Then the engine drains its queue while anomaly listeners are still attached.
 * Then the alert notifier drains. Then the rule store closes.
 *
 * Routes. `GET /metrics` serves Prometheus text. `/healthz` is always public.
 * The `/v1/*` routes require a token when a token file is configured.
 *
 * Rules. Rules are stored through the HTTP API and take effect after
 * {@link NexusPulseApp.reloadRules} runs. Rules are evaluated for every event
 * the engine accepts, whether it arrived over TCP or over the HTTP API.
 *
 * Alerts. Anomalies and rule findings go to an {@link AlertNotifier}. Its sinks
 * are the log and, if configured, a webhook. Repeats within the dedup window are
 * suppressed from alert delivery. Counters in {@link AppStats} still count every
 * occurrence.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import type { Server } from 'node:http';
import {
  createDefaultEngine,
  DEFAULT_MAX_FUTURE_SKEW_MS,
  validateEvent,
  type Anomaly,
  type EngineErrorEvent,
  type EngineMetrics,
  type StreamEngine,
  type SubmitResult,
  type TelemetryEvent,
} from '../core/engine.js';
import { RuleError, RuleSet, type RuleFinding, type RuleSetStats } from '../core/rules.js';
import { createApiServer, type Handler } from '../api/http_router.js';
import { createTelemetryApi, RULE_SCHEMA, type RuleDefinition } from '../api/telemetry_api.js';
import { TokenAuth, type Role } from '../api/auth.js';
import { RecordStore } from '../db/record_store.js';
import { TcpIngestServer, type IngestStats } from '../ingest/tcp_ingest.js';
import { createLogger, type Logger, type LogSink } from '../utils/logger.js';
import {
  AlertNotifier,
  callbackSink,
  minSeverityFilter,
  webhookSink,
  type Alert,
  type AlertSink,
  type FetchLike,
  type NotifierStats,
} from '../alerts/notifier.js';
import { renderNexusPulseMetrics } from '../observability/prometheus.js';
import { EventLoopMonitor, type EventLoopLagSnapshot } from '../observability/event_loop_monitor.js';
import { loadTokenFile } from './token_file.js';
import type { Settings } from './settings.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Raised for start-up failures and misuse of the lifecycle. Codes: APP_STATE, APP_START_FAILED. */
export class AppError extends Error {
  public readonly code: 'APP_STATE' | 'APP_START_FAILED';
  public readonly details: Readonly<Record<string, unknown>>;

  constructor(code: 'APP_STATE' | 'APP_START_FAILED', message: string, details: Record<string, unknown> = {}) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
    this.name = 'AppError';
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

export interface AppOptions {
  readonly settings: Settings;
  /** Receives each log line. Default writes to stdout. */
  readonly logSink?: LogSink;
  /** Time source in epoch ms, shared by the engine, the logger, rule cooldowns and alerts. Default Date.now. */
  readonly clock?: () => number;
  /** Transport for the webhook sink. Default global fetch. Tests inject a fake. */
  readonly alertFetch?: FetchLike;
}

export interface AppAddresses {
  readonly http: number;
  readonly tcp: number;
}

export type AppState = 'created' | 'running' | 'stopped';

export interface AppStats {
  readonly state: AppState;
  readonly engine: EngineMetrics;
  readonly ingest: IngestStats | undefined;
  readonly rules: RuleSetStats;
  readonly findings: number;
  readonly ruleReloads: number;
  readonly ruleReloadFailures: number;
  readonly authenticated: boolean;
  readonly alerts: NotifierStats | undefined;
  /** Event-loop delay since the app started. Undefined before start() and after stop(). */
  readonly eventLoop: EventLoopLagSnapshot | undefined;
}

const RULE_FILE = 'rules.npdb';
const MAX_ALERT_SUMMARY = 1024;
const MAX_ALERT_KEY = 256;
const METRICS_CONTENT_TYPE = 'text/plain; version=0.0.4; charset=utf-8';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function describeError(err: unknown): string {
  return err instanceof Error ? `${err.name}: ${err.message}` : String(err);
}

function bounded(text: string, max: number): string {
  return text.length <= max ? text : text.slice(0, max);
}

function listenHttp(server: Server, port: number, host: string): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const onError = (err: Error): void => reject(err);
    server.once('error', onError);
    server.listen(port, host, () => {
      server.off('error', onError);
      const address = server.address();
      if (address !== null && typeof address === 'object') {
        resolve(address.port);
      } else {
        reject(new Error('HTTP server did not bind to a TCP port'));
      }
    });
  });
}

function closeHttp(server: Server): Promise<void> {
  return new Promise<void>((resolve) => {
    if (!server.listening) {
      resolve();
      return;
    }
    server.close(() => resolve());
    server.closeAllConnections();
  });
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

export class NexusPulseApp {
  private readonly settings: Settings;
  private readonly clock: () => number;
  private readonly alertFetch: FetchLike | undefined;
  private readonly logger: Logger;
  private readonly engine: StreamEngine;
  private readonly ruleSet = new RuleSet();
  private readonly unsubscribers: (() => void)[] = [];
  private readonly counters = { findings: 0, ruleReloads: 0, ruleReloadFailures: 0 };
  private store: RecordStore<RuleDefinition> | undefined;
  private auth: TokenAuth | undefined;
  private notifier: AlertNotifier | undefined;
  private httpServer: Server | undefined;
  private ingest: TcpIngestServer | undefined;
  private state: AppState = 'created';
  private readonly lagMonitor = new EventLoopMonitor();

  constructor(options: AppOptions) {
    this.settings = options.settings;
    this.clock = options.clock ?? Date.now;
    this.alertFetch = options.alertFetch;
    this.logger = createLogger({
      level: this.settings.logLevel,
      bindings: { service: 'nexuspulse' },
      clock: this.clock,
      ...(options.logSink === undefined ? {} : { sink: options.logSink }),
    });
    this.engine = createDefaultEngine({
      clock: this.clock,
      queueCapacity: this.settings.queueCapacity,
      backpressure: this.settings.backpressure,
    });
  }

  /**
   * Starts every component in order. On failure, tears down what was started
   * and rejects with an {@link AppError}.
   *
   * @throws {AppError} APP_STATE if the app is not in the `created` state,
   *   APP_START_FAILED if a component could not start.
   */
  async start(): Promise<AppAddresses> {
    if (this.state !== 'created') {
      throw new AppError('APP_STATE', `cannot start an app that is ${this.state}`, { state: this.state });
    }
    const s = this.settings;
    try {
      fs.mkdirSync(s.dataDir, { recursive: true });
      this.store = RecordStore.open<RuleDefinition>({ file: path.join(s.dataDir, RULE_FILE), schema: RULE_SCHEMA });
      this.reloadRules();

      if (s.tokensFile !== undefined) {
        this.auth = loadTokenFile(s.tokensFile);
        this.logger.info('API authentication enabled', { tokens: this.auth.size });
      } else {
        this.logger.warn('API authentication is disabled; set NEXUSPULSE_TOKENS_FILE before exposing the API');
      }

      this.notifier = this.createNotifier();

      this.unsubscribers.push(
        this.engine.on('anomaly', (anomaly: Anomaly) => this.reportAnomaly(anomaly)),
        this.engine.on('error', (event: EngineErrorEvent) => this.reportEngineError(event)),
      );
      this.engine.start();
      this.lagMonitor.start();

      const router = createTelemetryApi({
        engine: this.engine,
        rules: this.store,
        maxBatch: s.maxBatch,
        maxBodyBytes: s.maxBodyBytes,
        submit: (raw: unknown) => this.submitAccepted(raw),
        onError: (err, info) =>
          this.logger.error('HTTP handler failed', {
            requestId: info.requestId,
            method: info.method,
            path: info.path,
            error: describeError(err),
          }),
        ...(this.auth === undefined ? {} : { auth: this.auth }),
      });
      router.add('GET', '/metrics', this.guard('reader', () => ({
        status: 200,
        raw: renderNexusPulseMetrics(this.stats()),
        headers: { 'content-type': METRICS_CONTENT_TYPE },
      })));

      this.httpServer = createApiServer(router);
      const httpPort = await listenHttp(this.httpServer, s.httpPort, s.httpHost);

      this.ingest = new TcpIngestServer({
        engine: this.engine,
        ...(this.auth === undefined ? {} : { auth: this.auth }),
        maxConnections: s.maxConnections,
        idleTimeoutMs: s.idleTimeoutMs,
        submit: (raw: unknown) => this.submitAccepted(raw),
      });
      const tcpPort = await this.ingest.listen(s.tcpPort, s.tcpHost);

      this.state = 'running';
      this.logger.info('listening', { http: httpPort, tcp: tcpPort, dataDir: s.dataDir });
      return { http: httpPort, tcp: tcpPort };
    } catch (err) {
      await this.teardown();
      this.state = 'stopped';
      throw new AppError('APP_START_FAILED', `start-up failed: ${describeError(err)}`, {
        cause: describeError(err),
      });
    }
  }

  /**
   * Stops every component in the order described in the module header. Safe to
   * call more than once, and before start().
   *
   * @returns The number of queued events the engine processed during shutdown.
   */
  async stop(): Promise<{ drained: number }> {
    if (this.state !== 'running') {
      this.state = 'stopped';
      return { drained: 0 };
    }
    const drained = await this.teardown();
    this.state = 'stopped';
    this.logger.info('stopped', { drained });
    return { drained };
  }

  /**
   * Loads every stored rule into the active rule set. If any stored rule is
   * rejected, the previous rules stay in force and the failure is counted.
   *
   * @throws {AppError} APP_STATE if the app has not been started.
   */
  reloadRules(): { loaded: number; error?: string } {
    if (this.store === undefined) {
      throw new AppError('APP_STATE', 'rules can only be loaded after start()', {});
    }
    const definitions = [...this.store.entries()].map(([name, rule]) => [name, rule] as const);
    try {
      this.ruleSet.update(definitions);
      this.counters.ruleReloads++;
      this.logger.info('rules loaded', { loaded: definitions.length });
      return { loaded: definitions.length };
    } catch (err) {
      if (!(err instanceof RuleError)) throw err;
      this.counters.ruleReloadFailures++;
      this.logger.error('rule reload rejected; previous rules kept', { error: err.message });
      return { loaded: this.ruleSet.size, error: err.message };
    }
  }

  stats(): AppStats {
    return Object.freeze({
      state: this.state,
      engine: this.engine.metrics(),
      ingest: this.ingest?.stats(),
      rules: this.ruleSet.stats(),
      findings: this.counters.findings,
      ruleReloads: this.counters.ruleReloads,
      ruleReloadFailures: this.counters.ruleReloadFailures,
      authenticated: this.auth !== undefined,
      alerts: this.notifier?.stats(),
      eventLoop: this.lagMonitor.isRunning ? this.lagMonitor.snapshot() : undefined,
    });
  }

  // -- internals -------------------------------------------------------------

  /** Wraps a handler with the token check when authentication is enabled. */
  private guard(role: Role, handler: Handler): Handler {
    return this.auth === undefined ? handler : this.auth.guard(role, handler);
  }

  private createNotifier(): AlertNotifier {
    const s = this.settings;
    const sinks: AlertSink[] = [
      callbackSink('log', (alert: Alert) => {
        this.logger.warn('alert', {
          key: alert.key,
          severity: alert.severity,
          summary: alert.summary,
          firedAt: alert.firedAt,
          labels: alert.labels,
        });
      }),
    ];
    if (s.alertWebhookUrl !== undefined) {
      sinks.push(
        webhookSink({
          name: 'webhook',
          url: s.alertWebhookUrl,
          ...(this.alertFetch === undefined ? {} : { fetch: this.alertFetch }),
        }),
      );
    }
    return new AlertNotifier({
      sinks: sinks.map((sink) => minSeverityFilter(sink, s.alertMinSeverity)),
      dedupWindowMs: s.alertDedupMs,
      clock: this.clock,
    });
  }

  /** Stops listeners, drains the engine and then the notifier, then releases the store. Never throws. */
  private async teardown(): Promise<number> {
    this.lagMonitor.stop();
    if (this.httpServer !== undefined) {
      await closeHttp(this.httpServer);
      this.httpServer = undefined;
    }
    if (this.ingest !== undefined) {
      await this.ingest.close();
      this.ingest = undefined;
    }
    let drained = 0;
    if (this.engine.metrics().state === 'running') {
      drained = this.engine.stop();
    }
    for (const off of this.unsubscribers.splice(0)) off();
    if (this.notifier !== undefined) {
      await this.notifier.close({ drain: true });
      this.notifier = undefined;
    }
    if (this.store !== undefined) {
      this.store.close();
      this.store = undefined;
    }
    return drained;
  }

  /**
   * Engine submit plus rule evaluation. Shared by the TCP ingest and the HTTP
   * API so that a rule sees every accepted event, whichever way it arrived.
   * Rejected events are not evaluated.
   */
  private submitAccepted(raw: unknown): SubmitResult {
    const result = this.engine.submit(raw);
    if (result.accepted) this.evaluateRules(raw);
    return result;
  }

  private evaluateRules(raw: unknown): void {
    let event: TelemetryEvent;
    try {
      event = validateEvent(raw, { nowMs: this.clock(), maxFutureSkewMs: DEFAULT_MAX_FUTURE_SKEW_MS });
    } catch {
      // The engine has already accepted this record, so this is not expected. Skip rules rather than fail ingest.
      return;
    }
    for (const finding of this.ruleSet.evaluate(event)) this.reportFinding(finding);
  }

  private reportFinding(finding: RuleFinding): void {
    this.counters.findings++;
    this.enqueueAlert({
      key: bounded(`rule:${finding.rule}:${finding.source}:${finding.metric}`, MAX_ALERT_KEY),
      severity: finding.severity,
      summary: bounded(finding.message, MAX_ALERT_SUMMARY),
      firedAt: finding.timestamp,
      labels: {
        rule: finding.rule,
        kind: finding.kind,
        source: finding.source,
        metric: finding.metric,
        eventId: finding.eventId,
      },
    });
  }

  private reportAnomaly(anomaly: Anomaly): void {
    this.enqueueAlert({
      key: bounded(`anomaly:${anomaly.seriesKey}:${anomaly.detector}`, MAX_ALERT_KEY),
      severity: anomaly.severity,
      summary: bounded(anomaly.message, MAX_ALERT_SUMMARY),
      firedAt: anomaly.timestamp,
      labels: {
        detector: anomaly.detector,
        source: anomaly.source,
        metric: anomaly.metric,
        eventId: anomaly.eventId,
      },
    });
  }

  private enqueueAlert(alert: Alert): void {
    if (this.notifier === undefined) return;
    try {
      this.notifier.notify(alert);
    } catch (err) {
      // The alert text is bounded above, so this only happens on a programming error. Never fail ingest for it.
      this.logger.error('alert could not be queued', { key: alert.key, error: describeError(err) });
    }
  }

  private reportEngineError(event: EngineErrorEvent): void {
    this.logger.error('engine error', {
      phase: event.phase,
      detector: event.detector,
      eventId: event.eventId,
      error: describeError(event.error),
    });
  }
}
