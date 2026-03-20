## SBI Grant Tracker

Track Grants.gov opportunities relevant to Native American tribal governments, detect changes over time, and store AI/heuristic relevance classifications.

## Project structure

- **`app/`**  
  Flask app (API + mission control dashboard). Under construction.

- **`pipelines/`**  
  Data ingestion pipelines. See [`pipelines/README.MD`](pipelines/README.MD).  
  - **`pipelines/gran_gov/`** — Grants.gov API ingestion, normalization, snapshots, change detection, and (optional) Groq classification.  
  - **`pipelines/gran_gov/quick_classification.py`** — heuristic relevance scoring (shared by daily loop and backlog; avoids circular imports with `backlog_ingestion`).

- **`db/`**  
  - **`db_util.py`** — `get_db_connection(test_mode=...)` → `grants.db` or `grants_test.db`.

- **`jobs/`**  
  Scheduled / orchestrated runs, pipeline run logging, and job-only DB tables. See [`jobs/README.md`](jobs/README.md).

- **`grants.db`** (and optionally **`grants_test.db`**)  
  SQLite databases (paths are relative to the project root when you run commands from there).

## SQLite tables (domain)

Defined in `pipelines/gran_gov/init_tables.py` via `create_tables(conn)`:

- **`grants`** — latest normalized opportunity row per `opportunity_id`
- **`grant_snapshots`** — historical snapshots (canonical JSON + hash) for diffing
- **`grant_alerts`** — field-level alerts from snapshot comparison
- **`grant_classifications`** — relevance tags / scores / reasoning (one row per opportunity, upserted on conflict)

## SQLite tables (job orchestration)

Defined in `jobs/init_tables.py` via `create_pipeline_tables(conn)`:

- **`pipeline_runs`** — one row per job run (status, timestamps, counters)
- **`pipeline_logs`** — log lines tied to a `job_id`

## Running (from repo root)

Use **dotted** module names with `python -m` (not slashes).

| What | Command |
|------|---------|
| **Daily Grants.gov job** (creates pipeline tables, logs run, calls `grants_main`) | `python -m jobs.daily_jobs` |
| **Backfill + classify** many opportunities | `python -m pipelines.gran_gov.backlog_ingestion` |

Environment / config:

- **Groq**: set `GROQ_API_KEY` (and optionally `GROQ_MODEL`) for AI classification paths.
- Run commands from **`SBI_Grant_Tracker`** so imports like `pipelines.*` and `db.*` resolve.

## Keeping schema and code in sync

If you change keys returned by `normalize_opportunity()` in `pipelines/gran_gov/ingestion_utils.py`, update:

1. `pipelines/gran_gov/init_tables.py` (`grants` columns / `ensure_columns`)
2. `pipelines/gran_gov/ingestion_loop.py` (`upsert_grant_current` column list and parameters)

## Dependencies

See **`requirements.txt`**.
