/**
 * lib/mcp/schemaHash.ts -- canonicalisation and hashing for MCP tool schemas.
 *
 * TWO HASHES, AND THE REASON THERE ARE TWO
 * ----------------------------------------
 * schema_hash   every byte of the schema that survives canonicalisation. Any
 *               change at all moves it, including a description edit. This is
 *               the general drift signal.
 * contract_hash the validation-relevant subset only: annotations are dropped
 *               before hashing. A publisher improving a docstring does NOT move
 *               it.
 *
 * Prediction catalogue item 3 (Breaking Change) defines breaking mechanically as
 * "a tool disappears, OR a required parameter is added, OR a parameter's type
 * changes". If item 3 keyed on schema_hash it would score a Breaking Change
 * every time someone fixed a typo in a description. It keys on contract_hash.
 * Items 19 and 21 (Standardization / Schema Convergence) want the distance
 * metric described in the audit -- "param name set, type signature,
 * required/optional split" -- which is what paramSet() below produces.
 *
 * NORMALISATION, exactly
 * ----------------------
 *   1. JSON.parse. Whitespace and indentation cease to exist at this step.
 *   2. Object keys sorted by code unit, recursively.
 *   3. Elements of SET_LIKE_KEYS sorted. `required` and `enum` are SETS per
 *      JSON Schema, so their order carries no meaning. Every other array keeps
 *      its order: prefixItems is positional, and allOf/anyOf/oneOf branch order,
 *      while not semantically significant, is not something we are entitled to
 *      reorder without a real JSON Schema normaliser.
 *   4. $ref is NOT resolved. Resolution would mean inlining $defs or fetching,
 *      and a hash that depends on a network fetch is not a hash. hasRef() records
 *      its presence instead.
 *   5. JSON.stringify with no spacing, UTF-8, SHA-256, lowercase hex.
 *
 * STABLE AGAINST: key reordering, whitespace, `required`/`enum` element order,
 * equivalent numeric literals (JSON.parse collapses 1 and 1.0).
 *
 * MOVES WHEN: a property is added or removed, a type changes, the required set
 * changes, an enum gains or loses a value, nested structure changes, or any
 * constraint changes (minimum, pattern, format, ...).
 *
 * NOT STABLE AGAINST -- stated because these produce FALSE DRIFT and are
 * measured rather than hidden (see falseDriftClasses):
 *   - Semantically equivalent, structurally different schemas.
 *     {"type":["string","null"]} and {"anyOf":[{"type":"string"},{"type":"null"}]}
 *     mean the same thing and hash differently.
 *   - Inline vs $ref. A publisher refactoring a repeated shape into $defs moves
 *     every dependent hash while changing nothing semantically.
 * There is no canonical form for either without a full JSON Schema normaliser.
 * The rate at which the corpus uses these forms is reported as a measurement so
 * a reader can judge how much drift is real.
 */

import { createHash } from 'node:crypto';

/** Arrays whose element order carries no meaning, so they are sorted. */
const SET_LIKE_KEYS = new Set(['required', 'enum']);

/**
 * Keys dropped for contract_hash. Annotation and documentation only: none of
 * them changes whether a given argument validates.
 *
 * `default` is dropped deliberately and it is the arguable one: it can change
 * behaviour when an argument is omitted, but it cannot make a previously valid
 * call invalid, which is what item 3 means by breaking.
 *
 * `x-mcp-header` is deliberately NOT dropped. Per the Streamable HTTP transport
 * spec it mirrors an argument value into an HTTP header, so it is transport
 * contract, not prose.
 */
const ANNOTATION_KEYS = new Set([
  'description', 'title', '$comment', 'examples', 'example',
  'deprecated', 'readOnly', 'writeOnly', 'default',
]);

export type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/** Sort key for set-like array elements: their own canonical serialisation. */
const elementKey = (v: Json): string => JSON.stringify(canonicalise(v, false));

/**
 * Recursively canonicalise. `dropAnnotations` produces the contract form.
 * Pure: never mutates its input.
 */
export function canonicalise(value: unknown, dropAnnotations: boolean): Json {
  if (value === null || typeof value !== 'object') return value as Json;

  if (Array.isArray(value)) {
    return value.map((v) => canonicalise(v, dropAnnotations));
  }

  const src = value as Record<string, unknown>;
  const out: Record<string, Json> = {};
  for (const key of Object.keys(src).sort()) {
    if (dropAnnotations && ANNOTATION_KEYS.has(key)) continue;
    const child = src[key];
    if (SET_LIKE_KEYS.has(key) && Array.isArray(child)) {
      const items = child.map((v) => canonicalise(v, dropAnnotations));
      items.sort((a, b) => {
        const ka = elementKey(a), kb = elementKey(b);
        return ka < kb ? -1 : ka > kb ? 1 : 0;
      });
      out[key] = items;
      continue;
    }
    out[key] = canonicalise(child, dropAnnotations);
  }
  return out;
}

const sha256 = (s: string): string => createHash('sha256').update(s, 'utf8').digest('hex');

/** Full canonical hash. Moves on any change, including a description edit. */
export function schemaHash(schema: unknown): string {
  return sha256(JSON.stringify(canonicalise(schema, false)));
}

/** Validation-relevant hash. Does NOT move on annotation-only edits. */
export function contractHash(schema: unknown): string {
  return sha256(JSON.stringify(canonicalise(schema, true)));
}

/* ---------------------------------------------------------------------------
 * shape features -- items 19, 21, 39, and the P2 features todo
 * ------------------------------------------------------------------------ */

export interface ParamEntry {
  name: string;
  /** JSON Schema `type` as published. An array (union) is joined with '|'. */
  type: string | null;
  required: boolean;
}

/**
 * The param set items 19 and 21 measure distance over: name, type signature,
 * required/optional split. Only top-level properties -- nesting is reported
 * separately as maxDepth, because a distance metric that flattens nested
 * objects into the same namespace as top-level ones compares unlike things.
 */
export function paramSet(schema: unknown): ParamEntry[] {
  const s = schema as Record<string, any> | null;
  const props = s && typeof s === 'object' ? s.properties : null;
  if (!props || typeof props !== 'object') return [];
  const required: string[] = Array.isArray(s?.required) ? s.required.filter((x: unknown) => typeof x === 'string') : [];
  const req = new Set(required);
  return Object.keys(props).sort().map((name) => {
    const p = props[name];
    const t = p && typeof p === 'object' ? p.type : undefined;
    return {
      name,
      type: Array.isArray(t) ? t.map(String).sort().join('|') : typeof t === 'string' ? t : null,
      required: req.has(name),
    };
  });
}

export function requiredCount(schema: unknown): number {
  const r = (schema as any)?.required;
  return Array.isArray(r) ? r.filter((x: unknown) => typeof x === 'string').length : 0;
}

export function paramCount(schema: unknown): number {
  const p = (schema as any)?.properties;
  return p && typeof p === 'object' ? Object.keys(p).length : 0;
}

/** Deepest nesting level of the schema object graph. A flat schema is 1. */
export function maxDepth(value: unknown, depth = 1): number {
  if (value === null || typeof value !== 'object') return depth - 1;
  let deepest = depth;
  for (const v of Array.isArray(value) ? value : Object.values(value as object)) {
    if (v !== null && typeof v === 'object') {
      const d = maxDepth(v, depth + 1);
      if (d > deepest) deepest = d;
    }
  }
  return deepest;
}

/* ---------------------------------------------------------------------------
 * false-drift instrumentation
 * ------------------------------------------------------------------------ */

export interface FalseDriftClasses {
  /**
   * `type` given as an array of two or more values, e.g. {"type":["string","null"]}.
   * The anyOf spelling of the same constraint hashes differently, so a publisher
   * switching between the two spellings produces drift with no semantic change.
   */
  typeUnion: boolean;
  /**
   * anyOf/oneOf whose branches are ALL bare {type: ...} -- the composition
   * spelling of the same thing typeUnion expresses inline. Counted separately so
   * the two populations can be compared rather than merged.
   */
  unionViaComposition: boolean;
  /** $ref present anywhere. Inline-vs-reference refactors move the hash. */
  hasRef: boolean;
  /** $defs present. A schema carrying definitions can be inlined or referenced. */
  hasDefs: boolean;
}

/** Walk every node once and report which false-drift forms the schema uses. */
export function falseDriftClasses(schema: unknown): FalseDriftClasses {
  const out: FalseDriftClasses = {
    typeUnion: false, unionViaComposition: false, hasRef: false, hasDefs: false,
  };

  const isBareType = (v: unknown): boolean => {
    if (!v || typeof v !== 'object' || Array.isArray(v)) return false;
    const keys = Object.keys(v as object);
    return keys.length === 1 && keys[0] === 'type';
  };

  const walk = (v: unknown): void => {
    if (v === null || typeof v !== 'object') return;
    if (Array.isArray(v)) { for (const x of v) walk(x); return; }
    const o = v as Record<string, unknown>;
    if (Array.isArray(o.type) && o.type.length > 1) out.typeUnion = true;
    if ('$ref' in o) out.hasRef = true;
    if ('$defs' in o || 'definitions' in o) out.hasDefs = true;
    for (const k of ['anyOf', 'oneOf']) {
      const branches = o[k];
      if (Array.isArray(branches) && branches.length > 1 && branches.every(isBareType)) {
        out.unionViaComposition = true;
      }
    }
    for (const val of Object.values(o)) walk(val);
  };
  walk(schema);
  return out;
}

/** Aggregate the per-schema flags into corpus counts. A MEASUREMENT, not a filter. */
export interface FalseDriftTotals {
  schemas: number;
  typeUnion: number;
  unionViaComposition: number;
  hasRef: number;
  hasDefs: number;
}

export function tallyFalseDrift(all: FalseDriftClasses[]): FalseDriftTotals {
  const t: FalseDriftTotals = { schemas: all.length, typeUnion: 0, unionViaComposition: 0, hasRef: 0, hasDefs: 0 };
  for (const f of all) {
    if (f.typeUnion) t.typeUnion++;
    if (f.unionViaComposition) t.unionViaComposition++;
    if (f.hasRef) t.hasRef++;
    if (f.hasDefs) t.hasDefs++;
  }
  return t;
}

/**
 * A crude token estimate for item 42 (Context Cost Pressure). Deliberately NOT
 * called a token count: it is bytes/4, the standard rule of thumb, and it is
 * stated as an estimate everywhere it surfaces. The raw JSON is stored so anyone
 * can tokenize it properly with whichever tokenizer they care about -- which is
 * the whole point of the audit's "trivially verifiable by anyone" note.
 */
export function tokenEstimate(raw: string): number {
  return Math.ceil(Buffer.byteLength(raw, 'utf8') / 4);
}
