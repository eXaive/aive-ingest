/**
 * lib/mcp/canonicalize.ts
 *
 * Deterministic hash input for the MCP registry drift tracking.
 *
 * WHY: Postgres jsonb stores object keys sorted (by length, then bytewise),
 * while JSON.stringify preserves INSERTION order. If a backfill hashes
 * JSON.stringify(raw_jsonb_readback) and the worker hashes
 * JSON.stringify(api_response), the two can serialize identical content in
 * different key orders and produce DIFFERENT hashes — so a row that never
 * changed would look like it drifted. canonicalize() removes key order from
 * the equation: it recursively sorts every object's keys (arrays keep their
 * order — element position is meaningful), so identical content always
 * serializes identically regardless of source key order.
 *
 * Use hashCanonical() for ALL new hash inputs (status_hash,
 * status_message_hash). definition_hash is intentionally left on its legacy
 * unsorted JSON.stringify for now (changing it would invalidate every stored
 * definition_hash) — see the worker.
 */

import { createHash } from "crypto";

/** Recursively sort object keys; leave primitives and array order intact. */
export function canonicalize(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalize);
  const obj = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(obj).sort()) {
    out[key] = canonicalize(obj[key]);
  }
  return out;
}

/** SHA-256 of the canonical serialization of `value`. Stable across key order. */
export function hashCanonical(value: unknown): string {
  return createHash("sha256").update(JSON.stringify(canonicalize(value))).digest("hex");
}
