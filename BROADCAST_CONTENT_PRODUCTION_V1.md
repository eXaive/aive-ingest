# Broadcast Content Production V1: client boundary

Implementation candidate only; no commit, workflow invocation or real request.
The workflow has workflow_dispatch for mocked acceptance only, no secrets, no live
job and no schedule. The CLI always exits 1 with BLOCKED_COORDINATION_NOT_SHARED /
MISSING_AIVE_WORKER_CONTRACT before reading credentials or making any request.
There is deliberately no environment switch that enables production today.

## Missing AIVE prerequisites

AIVE checkpoint b6c9a19f contains shared coordination but migration 25080000 is not
hosted. Existing POST /api/broadcast/content-twin/advance accepts spec_id/grant_id,
requires an interactive owner cookie, and rejects hosted/non-local execution. It
advances one action, not one complete episode. It is unsuitable for this worker.
The existing Content Twin read endpoint is also owner-controlled. Do not reuse
AIVE_CRON_SECRET, owner cookies or a Supabase service key to bypass these gates.

The smallest missing AIVE surface is a narrow worker controller with two operations:
POST /api/broadcast/content-production/worker/preflight (read-only selector), and
POST /api/broadcast/content-production/worker/advance (one selected episode).
These routes DO NOT EXIST yet. This document and injected Transport describe a
PROPOSED V1 contract, not a live API. AIVE must implement/approve it separately,
reusing its canonical catalog, runner, storage and authority contracts. Its existing
owner-mediated save path needs a narrowly authorized server execution path too.

## Proposed request / authentication

Both operations require HTTPS at the approved https://aive.global origin, a dedicated
production-worker Bearer credential, Content-Type application/json, no redirects.
Provisioning/verification of that credential is missing. It must grant only the two
operations under an existing owner-created production grant. Never supply owner,
OpenAI, Blotato or database credentials to this workflow. No credential is created here.
Future protected configuration: AIVE_BASE_URL, a dedicated production worker token,
and owner-created grant UUID; do not use workflow text inputs for authority.

Common JSON request:

```json
{
  "contract": "AIVE_CONTENT_PRODUCTION_CRON_V1",
  "channel": "space-explorers-club",
  "batch_id": "522f97ea-9397-4045-a810-27086b69dc9b",
  "first_episode": 14,
  "last_episode": 30,
  "max_specs": 1,
  "grant_id": "<protected owner-created grant UUID>",
  "operational_run_id": "<GitHub run ID>:<attempt>"
}
```

Advance adds the exact preflight selection object. Neither request contains prompts,
scripts, media URLs, action IDs or locally manufactured paid idempotency keys.
Run/attempt is operational evidence only. AIVE must revalidate grant, canonical spec
revision, range and state at advance; preflight is not permission to dispatch.

## Proposed responses

HTTP 200 JSON always includes contract, channel and batch_id matching the request.
Preflight additionally contains coordination, worker_contract_ready, grant_valid and
selection. Only SHARED_TRANSACTIONAL plus true readiness/validity permits advance.
selection is null for no work, otherwise {spec_id: UUID, spec_revision: positive
integer, episode: 14..30, initial_state: STILL_REQUIRED | SHORT_REQUIRED}.
AIVE selects authoritatively; this repo only validates the bounded target.

Advance includes spec_id, spec_revision, outcome and final_state (or null).
The identity must match selection. Outcomes: PRODUCED_TO_OWNER_GATE,
BLOCKED_COORDINATION_NOT_SHARED, BLOCKED_GRANT, CLAIM_LOST, FAILED_DEFINITE, UNKNOWN.
NO_ELIGIBLE_WORK is a preflight no-op, never a successful advance response.
PRODUCED_TO_OWNER_GATE requires OWNER_REVIEW_REQUIRED. Other permitted final states
are STILL_REQUIRED, SHORT_REQUIRED and BLOCKED. No approval, readiness or publication
state can be claimed as success by this client.

AIVE must advance ONLY the selected spec through Still/binding then Short/binding
and stop at the owner gate. It alone owns shared claim/fencing, paid count caps,
provider calls, production identity and media operations. No ingest spend counter.
Daily protection must use appropriately bounded owner grant validity/count policy;
this client promises one spec per invocation, not an independent daily spend cap.

## Timeouts, retries and safe output

Preflight deadline 15 seconds; advance deadline eight minutes; workflow ten minutes.
At most one advance call, no poll/retry loop, no second selection. HTTP failure,
malformed output or lost contact after advance => UNKNOWN/reconciliation required.
The injected transport must honor timeout_ms, abort network I/O on timeout, refuse
redirects and never retry. No live transport is supplied until prerequisites pass.
Rerun safety ultimately depends on AIVE's deterministic identity/shared claim, not
GitHub concurrency or run IDs. Mock tests demonstrate this required server behavior;
they do not establish hosted acceptance.

Output is a fixed allowlist: classification, fixed reason, batch, numeric run/attempt,
validated spec UUID, enumerated initial/final state and elapsed milliseconds. No raw
response/error, grant ID, token, private URLs or provider data is logged. Non-success
live CLI exits nonzero; it currently only reports the missing-contract blocker.

## Verification and future schedule

Run npx tsx lib/broadcast/contentProduction.test.ts and npm run typecheck.
Tests inject AIVE responses and do not invoke real APIs or generate media. Existing
Broadcast and MCP files are unchanged. No dependencies or package scripts added.

Next: checkpoint this client candidate; separately implement/verify narrow AIVE API,
worker credential and hosted shared coordination, then review a live transport/manual
job and perform one authorized GitHub acceptance. Only afterward choose a conservative
cadence, one episode per invocation with multiple hours between runs. No final cron
cadence is selected or enabled by this candidate.
