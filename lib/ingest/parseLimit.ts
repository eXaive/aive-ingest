// Extracted verbatim from the private repo's shared constants module
// (lib/trading/constants.ts) when the ingest workers moved to this repo —
// the one function the MCP worker needed; the rest of that module is
// private-surface configuration that does not belong in a public tree.
// Keep behavior identical to the source: fail closed, never substitute the
// default for a set-but-unreadable value.

/**
 * Parse a rate/quota limit from an env var, failing closed.
 *
 * `parseInt(process.env.X ?? "10")` looks safe and is not: the `??` default only
 * applies when the variable is UNSET. A variable set to "ten", "1 0" or "$5"
 * still yields NaN, and every `count >= NaN` comparison is false — the limit
 * silently disappears while the code reads as though it is enforced.
 *
 * Returns null when the value cannot be read. Callers must refuse rather than
 * substitute the default: falling back would hide that the configured value was
 * never used, which is exactly how a misconfiguration survives unnoticed.
 *
 * @param raw          the env value
 * @param fallback     used ONLY when the variable is entirely unset
 */
export function parseLimit(
  raw: string | undefined | null,
  fallback: number,
): { value: number | null; ok: boolean; problem: string | null } {
  if (raw === undefined || raw === null || raw.trim() === "") {
    return { value: fallback, ok: true, problem: null }; // unset → documented default
  }
  const n = Number(raw.trim());
  if (!Number.isFinite(n) || n <= 0 || !Number.isInteger(n)) {
    return {
      value: null,
      ok: false,
      problem: `not a positive integer (${JSON.stringify(raw.trim())})`,
    };
  }
  return { value: n, ok: true, problem: null };
}
