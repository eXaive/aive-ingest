# Broadcast Content Production V1: scope-only client

Candidate parity correction to checkpoint b74fda43. No live workflow or production
is enabled. Existing workflow_dispatch still runs mocked tests only; no schedule,
secrets or live job. CLI exits 1 / LIVE_TRANSPORT_DISABLED without network access.

## AIVE owns selection and production

AIVE owns canonical specs, deterministic sequence/spec-ID ordering, grants,
transactional claims/fencing, paid caps, provider operations, private media,
Still/Short binding and owner gates. Ingest sends scope, never a selected spec,
episode, action, prompt, script or media identity. AIVE reselects at advance time;
preflight evidence is not dispatch authority. One call advances at most one spec.

## Final strict contract

POST /api/broadcast/content-production/worker/preflight
POST /api/broadcast/content-production/worker/advance

HTTPS origin https://aive.global; dedicated production-machine Bearer token.
Both operations send exactly the same shape:

```json
{
  "contract": "AIVE_CONTENT_PRODUCTION_CRON_V1",
  "batch_id": "522f97ea-9397-4045-a810-27086b69dc9b",
  "grant_id": "<protected existing owner-created grant UUID>",
  "operational_run_id": "<numeric GitHub run ID>:<attempt>"
}
```

Batch fixes Space Explorers Club. There is no client-side Episode 014 range or
selection field. GitHub run identity is evidence only, never paid idempotency.

Exact response keys:
contract, batch_id, coordination, production_enabled, eligible_work, grant_valid,
outcome, spec_id, episode, initial_state, final_state.

coordination: SHARED_TRANSACTIONAL / LOCAL / UNAVAILABLE.
The three availability fields are booleans. outcome is null for enabled preflight,
otherwise one of PRODUCED_TO_OWNER_GATE, NO_ELIGIBLE_WORK,
BLOCKED_COORDINATION_NOT_SHARED, BLOCKED_GRANT, CLAIM_LOST, FAILED_DEFINITE, UNKNOWN.
spec_id is UUID/null, episode positive integer/null. initial_state is STILL_REQUIRED,
SHORT_REQUIRED or null. final_state additionally permits OWNER_REVIEW_REQUIRED and
BLOCKED. Successful advance requires OWNER_REVIEW_REQUIRED and a spec identity.
Unknown fields are rejected. AIVE's machineProductionContract.ts is the server
schema; cross-repository mocked acceptance invokes this client against its routes.

No advance unless preflight enables production with shared coordination, valid
grant and eligible work. Advance response must disable further production and name
an outcome. Never loop to another item. Preflight/advance deadlines: 15s/8m; manual
mock workflow timeout 10m. The future transport must abort timed-out HTTP, reject
redirects and disable retries. Uncertain/malformed advance results become UNKNOWN;
no retry. Repeated GitHub runs do not manufacture spec/action identities.

## Credentials and release blockers

Dedicated machine secret lives only in protected runtime configuration and is
accepted only by AIVE's production routes. Never use owner, OpenAI, Blotato or
Supabase service credentials. The machine token does not authorize generation
without an existing bounded owner grant. Daily paid protection must be expressed
through AIVE's authoritative grant validity/cap; ingest has no spend ledger.

AIVE source now supplies the machine endpoint candidate and read-only inspection
migration 25090000. Migrations 25080000/25090000 remain NOT HOSTED. Narrow machine
and database-worker credentials, server enablement, runtime verification and hosted
acceptance remain pending. Existing Short generation requires local Kokoro/FFmpeg
and reports unavailable on Vercel. No endpoint existence in source proves deployment.
Live transport/job enablement requires a separately reviewed release change.

## Tests and preservation

Run npx tsx lib/broadcast/contentProduction.test.ts and npm run typecheck.
Tests use mocks only. Logs allow only result/reason, batch, numeric run identity,
validated spec ID, enum states and elapsed time. No grant, token, raw error or URLs.
All existing Broadcast schedules/publication workers and unrelated MCP work remain
unchanged. No dependencies added. Future cadence remains undecided and disabled.
