/**
 * NexusPulse telemetry API: HTTP routes that connect the router to the stream
 * engine and the record store.
 *
 * Routes (all JSON):
 *   GET    /healthz                     engine state; 503 once the engine is stopped
 *   GET    /v1/metrics                  engine counters
 *   POST   /v1/events                   one event object, or an array of events (a batch)
 *   GET    /v1/series?source=&metric=   rolling-window snapshot of one series
 *   GET    /v1/rules                    list stored rules
 *   GET    /v1/rules/:name              one rule
 *   PUT    /v1/rules/:name              create or replace a rule
 *   DELETE /v1/rules/:name              remove a rule
 *
 * Rules are stored and validated here. The engine does not read them yet, so a
 * stored rule has no effect on detection until that wiring exists.
 *
 * Status codes for ingest:
 *   202  accepted (single event), or a batch was processed (see the counts)
 *   422  a single event failed validation
 *   503  the queue is full (with Retry-After) or the engine is stopped
 */

import {
  BackpressureError,
  type DropReason,
  type Severity,
  type StreamEngine,
  type SubmitResult,
} from '../core/engine.js';
import { RecordStore, StorageCorruptionError, StorageRecordTooLargeError, StorageValidationError } from '../db/record_store.js';
import { defineSchema, validate, type Schema } from '../db/schema.js';
import { ApiError, Router, type ErrorHook, type Handler, type Method, type RequestContext, type ApiResponse } from './http_router.js';
import type { Role, Authenticator } from './auth.js';

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** A stored alerting rule. */
export interface RuleDefinition {
  readonly metric: string;
  readonly min?: number;
  readonly max?: number;
  readonly severity: Severity;
}

/** Options for {@link createTelemetryApi}. */
export interface TelemetryApiOptions {
  readonly engine: StreamEngine;
  /** Optional rule store. Without it, the /v1/rules routes answer 503. */
  readonly rules?: RecordStore<RuleDefinition>;
  /** Largest number of events accepted in one batch. Default 500. */
  readonly maxBatch?: number;
  /** Largest request body in bytes. Default 1 MiB. */
  readonly maxBodyBytes?: number;
  /** Receives unexpected failures. */
  readonly onError?: ErrorHook;
  /**
   * When set, `/v1/*` routes require a bearer token: readers may call GET
   * routes, writers may also call POST, PUT and DELETE. `/healthz` stays public.
   */
  readonly auth?: Authenticator;
  /**
   * Submits one raw event. Defaults to `engine.submit`. The host passes a
   * wrapper here so that rules see events that arrive over HTTP.
   */
  readonly submit?: (raw: unknown) => SubmitResult;
}

// ---------------------------------------------------------------------------
// Constants and schemas
// ---------------------------------------------------------------------------

/** Default batch limit. */
export const DEFAULT_MAX_BATCH = 500;
/** Hard ceiling on the batch limit, to bound memory per request. */
export const MAX_BATCH_LIMIT = 10_000;

const RULE_NAME_PATTERN = /^[a-z0-9][a-z0-9-]{0,62}$/;
const NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/;

/** Schema for a stored rule. Cross-field checks (min not above max) live in the handler. */
export const RULE_SCHEMA: Schema = defineSchema({
  type: 'object',
  properties: {
    metric: { type: 'string', minLength: 1, maxLength: 256, pattern: NAME_PATTERN },
    min: { type: 'number' },
    max: { type: 'number' },
    severity: { type: 'string', enum: ['info', 'warning', 'critical'] },
  },
  required: ['metric', 'severity'],
  additionalProperties: false,
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function requireBody(body: unknown): unknown {
  if (body === undefined) throw new ApiError(400, 'BODY_REQUIRED', 'request body is required');
  return body;
}

function parseBatchLimit(value: number | undefined): number {
  const limit = value ?? DEFAULT_MAX_BATCH;
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_BATCH_LIMIT) {
    throw new RangeError(`maxBatch must be an integer between 1 and ${MAX_BATCH_LIMIT}`);
  }
  return limit;
}

function ruleNameOf(ctx: RequestContext): string {
  const name = ctx.params['name'] ?? '';
  if (!RULE_NAME_PATTERN.test(name)) {
    throw new ApiError(400, 'INVALID_RULE_NAME', 'rule name must be 1-63 lowercase letters, digits or hyphens', {
      name,
    });
  }
  return name;
}

function requireRules(rules: RecordStore<RuleDefinition> | undefined): RecordStore<RuleDefinition> {
  if (rules === undefined) {
    throw new ApiError(503, 'RULES_UNAVAILABLE', 'no rule store is configured');
  }
  return rules;
}

function checkRuleBounds(rule: RuleDefinition): void {
  if (rule.min !== undefined && rule.max !== undefined && rule.min > rule.max) {
    throw new ApiError(422, 'RULE_BOUNDS_INVERTED', 'min must not exceed max', { min: rule.min, max: rule.max });
  }
}

/** Translates storage errors into API errors; anything else is rethrown. */
function mapStorageError(err: unknown): unknown {
  if (err instanceof StorageValidationError) {
    return new ApiError(422, 'RULE_INVALID', err.message, { issues: err.details['issues'] ?? [] });
  }
  if (err instanceof StorageRecordTooLargeError) {
    return new ApiError(413, 'RECORD_TOO_LARGE', err.message);
  }
  if (err instanceof StorageCorruptionError) {
    return new ApiError(500, 'STORAGE_CORRUPT', 'stored data is corrupt');
  }
  return err;
}

/**
 * Submits each event in order and tallies the outcome. This mirrors
 * `StreamEngine.submitBatch`, but goes through the injected submit function.
 * Backpressure is tallied as `queue-full`. Any other exception propagates.
 */
function submitAll(
  raws: readonly unknown[],
  submit: (raw: unknown) => SubmitResult,
): { total: number; accepted: number; rejected: number; rejectedByReason: Partial<Record<DropReason, number>> } {
  let accepted = 0;
  const rejectedByReason: Partial<Record<DropReason, number>> = {};
  const reject = (reason: DropReason): void => {
    rejectedByReason[reason] = (rejectedByReason[reason] ?? 0) + 1;
  };
  for (const raw of raws) {
    try {
      const result = submit(raw);
      if (result.accepted) accepted++;
      else reject(result.reason);
    } catch (err) {
      if (err instanceof BackpressureError) reject('queue-full');
      else throw err;
    }
  }
  return { total: raws.length, accepted, rejected: raws.length - accepted, rejectedByReason };
}

function engineSubmitError(result: Extract<SubmitResult, { accepted: false }>): ApiError {
  switch (result.reason) {
    case 'invalid':
      return new ApiError(422, 'EVENT_INVALID', result.error?.message ?? 'event is invalid', { reason: 'invalid' });
    case 'queue-full':
      return new ApiError(503, 'QUEUE_FULL', 'ingest queue is full', { reason: 'queue-full' }, { 'retry-after': '1' });
    case 'shutdown':
      return new ApiError(503, 'ENGINE_STOPPED', 'engine is stopped', { reason: 'shutdown' });
    default:
      return new ApiError(409, 'EVENT_REJECTED', `event was rejected: ${result.reason}`, { reason: result.reason });
  }
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/**
 * Builds the telemetry router. The caller owns the engine and the store and is
 * responsible for starting, stopping and closing them.
 *
 * @throws {RangeError} If `maxBatch` is out of range.
 */
export function createTelemetryApi(options: TelemetryApiOptions): Router {
  const { engine, rules } = options;
  const submit = options.submit ?? ((raw: unknown): SubmitResult => engine.submit(raw));
  const maxBatch = parseBatchLimit(options.maxBatch);
  const router = new Router({
    ...(options.maxBodyBytes === undefined ? {} : { maxBodyBytes: options.maxBodyBytes }),
    ...(options.onError === undefined ? {} : { onError: options.onError }),
  });

  const route = (method: Method, pattern: string, role: Role | 'public', handler: Handler): void => {
    const guarded = options.auth === undefined || role === 'public' ? handler : options.auth.guard(role, handler);
    router.add(method, pattern, guarded);
  };

  router.get('/healthz', () => {
    const { state } = engine.metrics();
    const status = state === 'stopped' ? 503 : 200;
    return { status, body: { status: state === 'stopped' ? 'unavailable' : 'ok', state } };
  });

  route('GET', '/v1/metrics', 'reader', () => ({ status: 200, body: engine.metrics() }));

  route('POST', '/v1/events', 'writer', async (ctx) => {
    const body = requireBody(await ctx.readJson());

    if (Array.isArray(body)) {
      if (body.length === 0) throw new ApiError(400, 'EMPTY_BATCH', 'batch must contain at least one event');
      if (body.length > maxBatch) {
        throw new ApiError(413, 'BATCH_TOO_LARGE', `batch has ${body.length} events, above the limit of ${maxBatch}`, {
          limit: maxBatch,
        });
      }
      return { status: 202, body: { mode: 'batch', ...submitAll(body, submit) } };
    }

    let result: SubmitResult;
    try {
      result = submit(body);
    } catch (err) {
      if (err instanceof BackpressureError) {
        throw new ApiError(503, 'QUEUE_FULL', err.message, { reason: 'queue-full' }, { 'retry-after': '1' });
      }
      throw err;
    }
    if (!result.accepted) throw engineSubmitError(result);
    return { status: 202, body: { mode: 'single', accepted: true } };
  });

  route('GET', '/v1/series', 'reader', (ctx) => {
    const source = ctx.query.get('source');
    const metric = ctx.query.get('metric');
    if (source === null || metric === null || source.length === 0 || metric.length === 0) {
      throw new ApiError(400, 'MISSING_QUERY', 'source and metric query parameters are required');
    }
    if (source.length > 256 || metric.length > 256) {
      throw new ApiError(400, 'QUERY_TOO_LONG', 'source and metric must be at most 256 characters');
    }
    const snapshot = engine.getSeriesSnapshot(source, metric);
    if (snapshot === undefined) throw new ApiError(404, 'SERIES_NOT_FOUND', 'no such series', { source, metric });
    return { status: 200, body: snapshot };
  });

  route('GET', '/v1/rules', 'reader', () => {
    const store = requireRules(rules);
    return { status: 200, body: { rules: Object.fromEntries(store.entries()) } };
  });

  route('GET', '/v1/rules/:name', 'reader', (ctx) => {
    const store = requireRules(rules);
    const name = ruleNameOf(ctx);
    const rule = store.get(name);
    if (rule === undefined) throw new ApiError(404, 'RULE_NOT_FOUND', 'no such rule', { name });
    return { status: 200, body: rule };
  });

  route('PUT', '/v1/rules/:name', 'writer', async (ctx): Promise<ApiResponse> => {
    const store = requireRules(rules);
    const name = ruleNameOf(ctx);
    const body = requireBody(await ctx.readJson());
    const check = validate(RULE_SCHEMA, body);
    if (!check.valid) {
      throw new ApiError(422, 'RULE_INVALID', 'rule does not match the schema', { issues: check.issues });
    }
    const rule = body as RuleDefinition;
    checkRuleBounds(rule);
    const existed = store.has(name);
    try {
      store.put(name, rule);
    } catch (err) {
      throw mapStorageError(err);
    }
    return { status: existed ? 200 : 201, body: rule };
  });

  route('DELETE', '/v1/rules/:name', 'writer', (ctx) => {
    const store = requireRules(rules);
    const name = ruleNameOf(ctx);
    try {
      if (!store.delete(name)) throw new ApiError(404, 'RULE_NOT_FOUND', 'no such rule', { name });
    } catch (err) {
      throw mapStorageError(err);
    }
    return undefined;
  });

  return router;
}
