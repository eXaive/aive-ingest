# aive-ingest

Public-data ingest workers for [AIVE](https://github.com/eXaive). Each worker
scans a public upstream registry on a daily GitHub Actions schedule and writes
what it finds to AIVE's Supabase Postgres.

| Worker | Upstream | Cadence (UTC) |
|---|---|---|
| `workers/discover-mcp-servers.ts` | MCP remote endpoints (`server/discover`) | daily 03:00 |
| `workers/ingest-mcp-governance.ts` | modelcontextprotocol issues + discussions (GitHub) | daily 04:30 |
| `workers/ingest-mcp-registry.ts` | registry.modelcontextprotocol.io | daily 05:00 |
| `workers/ingest-webhook-providers.ts` | Pipedream / n8n public repos (GitHub) | daily 05:30 |
| `workers/ingest-homebrew-formulae.ts` | formulae.brew.sh | daily 05:45 |
| `workers/ingest-npm-sdk-packages.ts` | registry.npmjs.org search | daily 06:15 |
| `workers/probe-mcp-endpoints.ts` | MCP remote endpoints (HEAD reachability) | daily 07:00 |

The two MCP endpoint workers are deliberately **separate** and deliberately
**ordered**. `probe-mcp-endpoints` sends HEAD (GET on 405) and never negotiates
protocol; `discover-mcp-servers` sends one `server/discover` POST and never
sends `initialize` or a session header — both were removed in MCP revision
2026-07-28 (SEP-2575 and SEP-2567). Discover runs at 03:00 so the 07:00 HEAD
row stays the newest row per endpoint, which keeps the published reachability
verdicts on the method they document instead of silently re-basing them onto
POST outcomes.

Advertised protocol revisions are stored **verbatim** — no trimming, case
folding or date reformatting. Shape is judged at read time by the
`mcp_server_era` view, which classifies a malformed value as `unknown` rather
than letting it inflate a band; normalising on write would destroy the evidence
that guard exists to catch.

## Per-host pacing

The corpus is long-tailed: ~7,300 distinct hosts, but two of them
(`gateway.pipeworx.io` at 1,311 endpoints and `server.smithery.ai` at 216)
carry about 15.6% of all endpoints. So pacing state lives on the **host**, not
the request:

- work is queued per host and dispatched round-robin — a flat list would let
  one mega-host monopolise every worker slot
- per-host concurrency is 1; global concurrency only ever spreads across hosts
- a 429 raises **that host's** gap (150 ms floor, ×4 per 429, 60 s ceiling,
  `Retry-After` honoured up to 5 min) and defers only that host
- each endpoint is requeued at most once after a 429, then recorded as
  `rate_limited`
- a wall-clock deadline (`DISCOVER_DEADLINE_MINUTES`, default 150) stops
  dispatch under the workflow timeout, so a hard-throttling mega-host cannot
  get the run killed mid-sweep; the shortfall is logged, never implied

`npm run verify:host-backoff` proves this against a synthetic 429 on a loopback
server, driving the shipped `HostScheduler` / `runSweep` / `discoverOnce` — not
a re-implementation. No database, no external network.

## Database access model

All database access goes through **one scoped, least-privilege Postgres role**
(`aive_ingest`), connected via `AIVE_INGEST_DATABASE_URL` — the only secret
this repo reads. There is no Supabase service key here, and the shared
connection helper (`lib/ingest/db.ts`) fails closed if the URL is missing
rather than falling back to anything broader.

The role can touch a short, enumerated list of tables — the ingest corpus
tables, their snapshot tables, and the run-bookkeeping tables:

- `registry_artifacts` (select / insert / update)
- `registry_artifact_snapshots` (insert)
- `scan_runs` (select / insert / update)
- `mcp_servers` (select / insert / update)
- `mcp_server_snapshots` (select / insert)
- `mcp_endpoint_probes` (select / insert)
- `ingestion_log` (insert)
- `mcp_governance_items` (select / insert / update) — **grant pending**
- `mcp_governance_transitions` (insert) — **grant pending**

It **cannot delete from any table**, cannot execute any database function,
cannot read any other table, and does not bypass row-level security. A
compromise of this repo or its secret is bounded to writing ingest rows in
those tables.

The last two are marked pending because they were created `service_role`-only
and still are. `ingest-mcp-governance` preflights `has_table_privilege` and
**refuses to run** without them, rather than letting a permissions wall be
logged as an upstream GitHub outage. The grants (and matching RLS policies —
both tables have RLS enabled) are a migration in the private platform repo.

## Failure model

There is no alerting side-channel: **a red Actions run is the alert.** Each
worker exits non-zero on ingest errors and carries its own staleness tripwire
(it fails the run if its snapshot stream is older than the allowed window),
so a worker that silently stops writing turns its next scheduled run red.

**No hollow success.** `discover-mcp-servers` and `ingest-mcp-governance` also
exit non-zero on a run that *completes* having written zero rows. Every outcome
— including every failure — is recorded as a row, so zero rows never means "the
upstream was quiet"; it means the write path did not work. A staleness tripwire
alone does not catch this: yesterday's rows keep the stream fresh while today
writes nothing.

## Running locally

```
npm ci
AIVE_INGEST_DATABASE_URL=<connection string for the aive_ingest role> npm run ingest:homebrew-formulae
```

(The connection string is a Postgres URL for the `aive_ingest` role — never
commit it; `.env` / `.env.local` are gitignored.)

**Host, because getting it wrong looks like an auth failure:** use the
Supavisor pooler at **`aws-1-us-east-1.pooler.supabase.com`** with the
tenant-suffixed username (`aive_ingest.<project-ref>`). `aws-0` is a different
tenant region and fails with *"Tenant or user not found"* — which reads as the
pooler rejecting the custom role, and is not. The direct host
(`db.<ref>.supabase.co`) is IPv6-only, so it works from an IPv6 network but is
unreachable from GitHub Actions runners (`ENETUNREACH`).

Workflows are `schedule` + `workflow_dispatch` only — nothing in this repo
runs on pull requests.
