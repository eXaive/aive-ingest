# aive-ingest

Public-data ingest workers for [AIVE](https://github.com/eXaive). Each worker
scans a public upstream registry on a daily GitHub Actions schedule and writes
what it finds to AIVE's Supabase Postgres.

| Worker | Upstream | Cadence (UTC) |
|---|---|---|
| `workers/ingest-mcp-registry.ts` | registry.modelcontextprotocol.io | daily 05:00 |
| `workers/ingest-webhook-providers.ts` | Pipedream / n8n public repos (GitHub) | daily 05:30 |
| `workers/ingest-homebrew-formulae.ts` | formulae.brew.sh | daily 05:45 |
| `workers/ingest-npm-sdk-packages.ts` | registry.npmjs.org search | daily 06:15 |

## Database access model

All database access goes through **one scoped, least-privilege Postgres role**
(`aive_ingest`), connected via `AIVE_INGEST_DATABASE_URL` — the only secret
this repo reads. There is no Supabase service key here, and the shared
connection helper (`lib/ingest/db.ts`) fails closed if the URL is missing
rather than falling back to anything broader.

The role can touch exactly **six tables** — the ingest corpus tables, their
snapshot tables, and the run-bookkeeping tables:

- `registry_artifacts` (select / insert / update)
- `registry_artifact_snapshots` (insert)
- `scan_runs` (select / insert / update)
- `mcp_servers` (select / insert / update)
- `mcp_server_snapshots` (select / insert)
- `ingestion_log` (insert)

It **cannot delete from any table**, cannot execute any database function,
cannot read any other table, and does not bypass row-level security. A
compromise of this repo or its secret is bounded to writing ingest rows in
those six tables.

## Failure model

There is no alerting side-channel: **a red Actions run is the alert.** Each
worker exits non-zero on ingest errors and carries its own staleness tripwire
(it fails the run if its snapshot stream is older than the allowed window),
so a worker that silently stops writing turns its next scheduled run red.

## Running locally

```
npm ci
AIVE_INGEST_DATABASE_URL=<connection string for the aive_ingest role> npm run ingest:homebrew-formulae
```

(The connection string is a Postgres URL for the `aive_ingest` role against the
database's direct host — never commit it; `.env` / `.env.local` are gitignored.)

Workflows are `schedule` + `workflow_dispatch` only — nothing in this repo
runs on pull requests.
