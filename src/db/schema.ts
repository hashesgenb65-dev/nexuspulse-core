/**
 * NexusPulse persistence layer: a small schema language.
 *
 * Supported types are `string`, `number`, `integer`, `boolean`, `array` and
 * `object`. Schema definitions are checked when they are built with
 * {@link defineSchema}; values are checked with {@link validate}, which
 * collects every issue instead of stopping at the first one.
 */

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Base class for errors raised by the schema module. */
export class SchemaError extends Error {
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

/** Raised when a schema definition is malformed. Code: `SCHEMA_DEFINITION_INVALID`. */
export class SchemaDefinitionError extends SchemaError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super('SCHEMA_DEFINITION_INVALID', message, details);
  }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** String schema. */
export interface StringSchema {
  readonly type: 'string';
  readonly minLength?: number;
  readonly maxLength?: number;
  /** Pattern the string must match (a RegExp, or a string compiled as one). */
  readonly pattern?: RegExp | string;
  readonly enum?: readonly string[];
}

/** Floating point number schema (finite numbers only). */
export interface NumberSchema {
  readonly type: 'number';
  readonly min?: number;
  readonly max?: number;
}

/** Integer schema (safe or unsafe integers; fractional values are rejected). */
export interface IntegerSchema {
  readonly type: 'integer';
  readonly min?: number;
  readonly max?: number;
}

/** Boolean schema. */
export interface BooleanSchema {
  readonly type: 'boolean';
}

/** Array schema. */
export interface ArraySchema {
  readonly type: 'array';
  readonly items: Schema;
  readonly minItems?: number;
  readonly maxItems?: number;
}

/** Object schema. Unknown keys are rejected unless `additionalProperties` is true. */
export interface ObjectSchema {
  readonly type: 'object';
  readonly properties?: Readonly<Record<string, Schema>>;
  readonly required?: readonly string[];
  readonly additionalProperties?: boolean;
}

/** Any supported schema. */
export type Schema =
  | StringSchema
  | NumberSchema
  | IntegerSchema
  | BooleanSchema
  | ArraySchema
  | ObjectSchema;

/** A single validation problem. `path` is a JSON pointer ("" is the root value). */
export interface ValidationIssue {
  readonly path: string;
  readonly message: string;
}

/** Result of {@link validate}. */
export type ValidationResult =
  | { readonly valid: true }
  | { readonly valid: false; readonly issues: readonly ValidationIssue[] };

// ---------------------------------------------------------------------------
// Definition
// ---------------------------------------------------------------------------

const MAX_DEPTH = 64;
const defined = new WeakSet<object>();

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function escapePointer(segment: string): string {
  return segment.replace(/~/g, '~0').replace(/\//g, '~1');
}

function fail(path: string, message: string): never {
  throw new SchemaDefinitionError(`Invalid schema at "${path}": ${message}`, { path });
}

function readCount(def: Record<string, unknown>, key: string, path: string): number | undefined {
  const v = def[key];
  if (v === undefined) return undefined;
  if (typeof v !== 'number' || !Number.isInteger(v) || v < 0) {
    fail(path, `${key} must be a non-negative integer`);
  }
  return v;
}

function readBound(def: Record<string, unknown>, key: string, path: string): number | undefined {
  const v = def[key];
  if (v === undefined) return undefined;
  if (typeof v !== 'number' || !Number.isFinite(v)) fail(path, `${key} must be a finite number`);
  return v;
}

function checkOrder(
  lo: number | undefined,
  hi: number | undefined,
  loName: string,
  hiName: string,
  path: string,
): void {
  if (lo !== undefined && hi !== undefined && lo > hi) {
    fail(path, `${loName} must not exceed ${hiName}`);
  }
}

function build(def: unknown, path: string, depth: number): Schema {
  if (depth > MAX_DEPTH) fail(path, `schema nesting exceeds ${MAX_DEPTH} levels`);
  if (!isRecord(def)) fail(path, 'schema must be an object');
  const type = def['type'];
  const out: Record<string, unknown> = { type };
  switch (type) {
    case 'string': {
      const minLength = readCount(def, 'minLength', path);
      const maxLength = readCount(def, 'maxLength', path);
      checkOrder(minLength, maxLength, 'minLength', 'maxLength', path);
      if (minLength !== undefined) out['minLength'] = minLength;
      if (maxLength !== undefined) out['maxLength'] = maxLength;
      const pattern = def['pattern'];
      if (pattern !== undefined) {
        if (!(pattern instanceof RegExp) && typeof pattern !== 'string') {
          fail(path, 'pattern must be a RegExp or a string');
        }
        try {
          // Drop the stateful flags so that repeated tests are deterministic.
          const flags = pattern instanceof RegExp ? pattern.flags.replace(/[gy]/g, '') : '';
          out['pattern'] = new RegExp(pattern instanceof RegExp ? pattern.source : pattern, flags);
        } catch {
          fail(path, 'pattern is not a valid regular expression');
        }
      }
      const en = def['enum'];
      if (en !== undefined) {
        if (!Array.isArray(en) || en.length === 0 || !en.every((e) => typeof e === 'string')) {
          fail(path, 'enum must be a non-empty array of strings');
        }
        out['enum'] = Object.freeze([...(en as string[])]);
      }
      break;
    }
    case 'number':
    case 'integer': {
      const min = readBound(def, 'min', path);
      const max = readBound(def, 'max', path);
      checkOrder(min, max, 'min', 'max', path);
      if (min !== undefined) out['min'] = min;
      if (max !== undefined) out['max'] = max;
      break;
    }
    case 'boolean':
      break;
    case 'array': {
      if (def['items'] === undefined) fail(path, 'array schema requires items');
      out['items'] = build(def['items'], `${path}/items`, depth + 1);
      const minItems = readCount(def, 'minItems', path);
      const maxItems = readCount(def, 'maxItems', path);
      checkOrder(minItems, maxItems, 'minItems', 'maxItems', path);
      if (minItems !== undefined) out['minItems'] = minItems;
      if (maxItems !== undefined) out['maxItems'] = maxItems;
      break;
    }
    case 'object': {
      const props: Record<string, Schema> = Object.create(null) as Record<string, Schema>;
      const rawProps = def['properties'];
      if (rawProps !== undefined) {
        if (!isRecord(rawProps)) fail(path, 'properties must be an object');
        for (const key of Object.keys(rawProps)) {
          props[key] = build(rawProps[key], `${path}/properties/${escapePointer(key)}`, depth + 1);
        }
      }
      out['properties'] = Object.freeze(props);
      const req = def['required'];
      if (req !== undefined) {
        if (!Array.isArray(req) || !req.every((r) => typeof r === 'string')) {
          fail(path, 'required must be an array of strings');
        }
        const list = req as string[];
        if (new Set(list).size !== list.length) fail(path, 'required contains duplicates');
        for (const name of list) {
          if (!Object.hasOwn(props, name)) {
            fail(path, `required property "${name}" is not declared in properties`);
          }
        }
        out['required'] = Object.freeze([...list]);
      }
      const ap = def['additionalProperties'];
      if (ap !== undefined && typeof ap !== 'boolean') {
        fail(path, 'additionalProperties must be a boolean');
      }
      out['additionalProperties'] = ap === true;
      break;
    }
    default:
      fail(path, `unknown type ${JSON.stringify(type)}`);
  }
  const frozen = Object.freeze(out) as unknown as Schema;
  defined.add(frozen);
  return frozen;
}

/**
 * Validates a schema definition and returns an immutable, normalised copy.
 *
 * @param def - Schema definition (plain object). Nested schemas are validated
 *   recursively; patterns given as strings are compiled to RegExp.
 * @returns A deeply frozen schema usable with {@link validate}.
 * @throws {SchemaDefinitionError} If the type is unknown, a constraint has the
 *   wrong type or an inconsistent range, a pattern does not compile, `required`
 *   names an undeclared property, `items` is missing on an array, or nesting
 *   exceeds 64 levels.
 */
export function defineSchema(def: unknown): Schema {
  return build(def, '', 0);
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

function describe(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

function check(schema: Schema, value: unknown, path: string, issues: ValidationIssue[]): void {
  const add = (message: string): void => {
    issues.push({ path, message });
  };
  switch (schema.type) {
    case 'string': {
      if (typeof value !== 'string') return add(`expected string, got ${describe(value)}`);
      if (schema.minLength !== undefined && value.length < schema.minLength) {
        add(`length ${value.length} is below minLength ${schema.minLength}`);
      }
      if (schema.maxLength !== undefined && value.length > schema.maxLength) {
        add(`length ${value.length} exceeds maxLength ${schema.maxLength}`);
      }
      if (schema.pattern !== undefined && !(schema.pattern as RegExp).test(value)) {
        add(`does not match pattern ${String(schema.pattern)}`);
      }
      if (schema.enum !== undefined && !schema.enum.includes(value)) {
        add(`must be one of ${schema.enum.map((e) => JSON.stringify(e)).join(', ')}`);
      }
      return;
    }
    case 'number':
    case 'integer': {
      if (typeof value !== 'number' || !Number.isFinite(value)) {
        return add(`expected ${schema.type}, got ${typeof value === 'number' ? 'non-finite number' : describe(value)}`);
      }
      if (schema.type === 'integer' && !Number.isInteger(value)) {
        return add(`expected integer, got ${value}`);
      }
      if (schema.min !== undefined && value < schema.min) add(`${value} is below min ${schema.min}`);
      if (schema.max !== undefined && value > schema.max) add(`${value} exceeds max ${schema.max}`);
      return;
    }
    case 'boolean': {
      if (typeof value !== 'boolean') add(`expected boolean, got ${describe(value)}`);
      return;
    }
    case 'array': {
      if (!Array.isArray(value)) return add(`expected array, got ${describe(value)}`);
      if (schema.minItems !== undefined && value.length < schema.minItems) {
        add(`has ${value.length} items, below minItems ${schema.minItems}`);
      }
      if (schema.maxItems !== undefined && value.length > schema.maxItems) {
        add(`has ${value.length} items, exceeding maxItems ${schema.maxItems}`);
      }
      for (let i = 0; i < value.length; i++) {
        check(schema.items, value[i], `${path}/${i}`, issues);
      }
      return;
    }
    case 'object': {
      if (!isRecord(value)) return add(`expected object, got ${describe(value)}`);
      const props = schema.properties ?? {};
      for (const name of schema.required ?? []) {
        if (!Object.hasOwn(value, name) || value[name] === undefined) {
          issues.push({ path: `${path}/${escapePointer(name)}`, message: 'required property is missing' });
        }
      }
      for (const key of Object.keys(value)) {
        const v = value[key];
        if (v === undefined) continue;
        const sub = Object.hasOwn(props, key) ? props[key] : undefined;
        if (sub !== undefined) {
          check(sub, v, `${path}/${escapePointer(key)}`, issues);
        } else if (schema.additionalProperties !== true) {
          issues.push({ path: `${path}/${escapePointer(key)}`, message: 'unknown property' });
        }
      }
      return;
    }
  }
}

/**
 * Validates a value against a schema, collecting every issue.
 *
 * Properties whose value is `undefined` are treated as absent. Paths are JSON
 * pointers (for example `/items/0/name`); the root value has path `""`.
 *
 * @param schema - A schema from {@link defineSchema}. A raw definition is also
 *   accepted and is validated first.
 * @param value - The value to check.
 * @returns `{ valid: true }`, or `{ valid: false, issues }` with at least one issue.
 * @throws {SchemaDefinitionError} If a raw definition passed as `schema` is malformed.
 */
export function validate(schema: Schema, value: unknown): ValidationResult {
  const s = defined.has(schema) ? schema : defineSchema(schema);
  const issues: ValidationIssue[] = [];
  check(s, value, '', issues);
  return issues.length === 0 ? { valid: true } : { valid: false, issues };
}
