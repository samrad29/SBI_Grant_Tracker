## SBI Grant Tracker

Track Grants.gov opportunities relevant to Native American tribal governments, detect changes over time, and store AI/heuristic relevance classifications.

## Project structure

- **`app/`**  
  Flask app (API + mission control dashboard). Under construction.

## HTTP API reference

With the web app running (`python run.py` or `python -m scripts.run_web`), open **`/api-docs`** for the interactive-style HTML reference (all JSON routes, query parameters, and notes on session auth). The template lives at `app/templates/api_docs.html`.


TODO: ADD A RECENTLY DELETED THING

**Sessions (login / client portal):** Set **`SECRET_KEY`** in production so Flask can sign session cookies. `POST /api/auth/login` sets a **permanent** session cookie that expires after **`SESSION_LIFETIME_MINUTES`** of inactivity (default **37**; set to `30` or `45` as needed). User-activity routes read `session["user_id"]`—do not pass `user_id` in the query string. **`/portal`** uses this flow. Seed a test user with `python -m scripts.seed_demo_user`.

- **`pipelines/`**  
  Data ingestion pipelines. See [`pipelines/README.MD`](pipelines/README.MD).  
  - **`pipelines/gran_gov/`** — Grants.gov API ingestion, normalization, snapshots, change detection, and (optional) Groq classification.  
  - **`pipelines/wi_psc/`** — Wisconsin PSC OEI webpage ingestion with content-hash change detection, attachment retrieval, RAG enrichment, and AI extraction logging.
  - **`pipelines/gran_gov/quick_classification.py`** — heuristic relevance scoring (shared by daily loop and backlog; avoids circular imports with `backlog_ingestion`).

- **`db/`**  
  - **`db_util.py`** — `get_db_connection(test_mode=...)` using `DATABASE_URL` (Postgres) or local sqlite fallback.

- **`jobs/`**  
  Scheduled / orchestrated runs, pipeline run logging, and job-only DB tables. See [`jobs/README.md`](jobs/README.md).

- **Database**
  - Production/deploy: Postgres via `DATABASE_URL`
  - Local fallback: `grants.db` / `grants_test.db`

## Database tables (domain)

Defined in `pipelines/gran_gov/init_tables.py` via `create_tables(conn)`:

- **`grants`** — latest normalized opportunity row per `opportunity_id`
- **`grant_snapshots`** — historical snapshots (canonical JSON + hash) for diffing
- **`grant_alerts`** — field-level alerts from snapshot comparison
- **`grant_classifications`** — relevance tags / scores / reasoning (one row per opportunity, upserted on conflict)

Defined in `pipelines/wi_psc/db_util.py` via `init_tables(conn)`:

- **`oei_programs`** — latest extracted program-level fields keyed by source page URL
- **`attachment_documents`** — fetched attachment metadata and dedupe hash
- **`attachment_chunks`** — chunked attachment text for retrieval
- **`attachment_chunk_embeddings`** — vector embeddings for chunk retrieval
- **`ai_extraction_logs`** — prompt/response/extracted payload logs for each extraction attempt

## Database tables (job orchestration)

Defined in `jobs/init_tables.py` via `create_pipeline_tables(conn)`:

- **`pipeline_runs`** — one row per job run (status, timestamps, counters)
- **`pipeline_logs`** — log lines tied to a `job_id`

## Running (from repo root)

Use **dotted** module names with `python -m` (not slashes).

| What | Command |
|------|---------|
| **Daily jobs** (creates pipeline tables, logs runs, calls both `grants_main` and `wis_psc_main`) | `python -m jobs.daily_jobs` |
| **Backfill + classify** many opportunities | `python -m pipelines.gran_gov.backlog_ingestion` |
| **Web app (dev/local)** | `python run.py` |
| **Web app entrypoint (deploy-friendly)** | `python -m scripts.run_web` |
| **Daily job entrypoint (deploy-friendly)** | `python -m scripts.run_daily_job` |
| **Backlog job entrypoint (deploy-friendly)** | `python -m scripts.run_backlog_job` |
| **Seed a demo login user** (Werkzeug-hashed password for `/api/auth/login`) | `python -m scripts.seed_demo_user --email you@example.com --password '...'` |

Environment / config:

- **Groq**: set `GROQ_API_KEY` (and optionally `GROQ_MODEL`) for AI classification paths.
- **OpenAI embeddings**: set `OPENAI_API_KEY` for `pipelines.wi_psc.rag_util` embedding/retrieval flow.
- **Postgres**: set `DATABASE_URL`.
- **Provider switch**: `LLM_PROVIDER` = `groq` or `ollama`.
- **Ollama**: `OLLAMA_BASE_URL` (default `http://localhost:11434`) and `OLLAMA_MODEL` (for example, `llama3.2:latest`).
- **Backlog controls**: `MAX_GRANTS_PER_RUN`, `MAX_FAILURES_PER_RUN`, `MAX_RATE_LIMIT_RETRIES`, `RETRY_SLEEP_DEFAULT_SECONDS`.
- Run commands from **`SBI_Grant_Tracker`** so imports like `pipelines.*` and `db.*` resolve.

## Keeping schema and code in sync

If you change keys returned by `normalize_opportunity()` in `pipelines/gran_gov/ingestion_utils.py`, update:

1. `pipelines/gran_gov/init_tables.py` (`grants` columns)
2. `pipelines/gran_gov/ingestion_loop.py` (`upsert_grant_current` column list and parameters)

## Dependencies

See **`requirements.txt`**.
