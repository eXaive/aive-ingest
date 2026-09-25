# Broadcast Content Production V1: manual Still transport

Local candidate only. No workflow has been invoked, no production request made,
and no schedule is configured. Release and secret provisioning remain separate.

## Manual workflow

Broadcast Content Production V1 accepts only mode (mock/live, default mock) and
grant_id. Mock mode runs offline client/transport tests without a machine secret.
Live mode requires a valid grant UUID and the dedicated GitHub secret
AIVE_BROADCAST_PRODUCTION_MACHINE_SECRET, bound only to the live execution step.
The job retains contents:read, non-cancelling concurrency and a 10-minute timeout.

The new entry point is workers/broadcast-still-production-manual.ts. It requires
--live plus GITHUB_ACTIONS=true, GITHUB_EVENT_NAME=workflow_dispatch and
AIVE_PRODUCTION_MODE=live. Ordinary local execution remains disabled. These are
accidental-execution guards, not substitutes for AIVE authentication and grants.

## Fixed contract

Origin is exactly https://aive.global (no alternate port, URL input or redirect).
The client POSTs to /api/broadcast/content-production/worker/preflight, then at
most once to /api/broadcast/content-production/worker/advance. Requests contain
only contract=AIVE_CONTENT_PRODUCTION_CRON_V1, the fixed batch_id, grant_id and
operational_run_id (numeric GitHub run ID:attempt). Authentication is the dedicated
Bearer machine secret. AIVE alone selects specs and enforces paid reservations.

Preflight must report shared coordination, enabled production, valid grant,
eligible work and STILL_REQUIRED. Invalid/blocked preflight prevents advance.
PRODUCED_TO_SHORT_REQUIRED with final_state SHORT_REQUIRED is live-client success;
NO_ELIGIBLE_WORK is a successful no-op. Full-path PRODUCED_TO_OWNER_GATE remains
recognized by the shared parser for compatibility but is not Still-only success.
Unknown response keys, enum values, identities and mismatched success states fail
closed. Other bounded AIVE outcomes are reported and return nonzero exit status.

Timeouts are 15 seconds preflight and 330 seconds advance, allowing margin beyond
AIVE's 300-second route. AbortController covers fetch and body reads. Redirects
are rejected; no automatic retry exists. Advance timeout, reset, non-200 response
or malformed body becomes UNKNOWN / TRANSPORT_UNCERTAIN. Never rerun an uncertain
advance to recover a result: inspect AIVE's shared action first.

Logs contain sanitized bounded outcomes/states, batch, validated spec/run IDs and
elapsed time. They omit credentials, raw responses/errors, headers and media URLs.

## Required grant and release prerequisites

Use an owner-established exact one-spec/revision/hash grant containing only
GENERATE_STILL, paid_cap=1 and a short validity window. The current preflight does
not expose allowed_actions: the client cannot distinguish a Still-only grant from
a broader grant that starts at STILL_REQUIRED. A broader grant could authorize
AIVE's full path before the client rejects its final outcome. Verify grant scope
before the future live run; no client-only claim of action restriction is made.

AIVE Still-only code, hosted coordination/inspection migrations, authenticator
membership, dedicated machine/database worker credentials, reference bundling and
private storage must be verified during release. Nothing here deploys them or
creates a grant/secret. No OpenAI, Blotato, owner, Supabase or database credential
is supplied to this transport. Windows/heartbeat work is outside this scope.

## Offline verification

Run:
- npx tsx lib/broadcast/contentProduction.test.ts
- npx tsx lib/broadcast/stillProductionManual.test.ts
- npm run typecheck

Tests inject fake HTTP, including real AbortController cancellation against a
mock fetch. Existing Broadcast crons and unrelated MCP work are preserved.
No dependencies or package/lockfile changes are required.
