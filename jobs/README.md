# Jobs

Launch points for **scheduled** and **orchestrated** pipeline runs (daily / weekly, etc.).

## Current scripts

| File | Purpose |
|------|---------|
| **`daily_jobs.py`** | Entry for daily work: opens DB via `db.db_util.get_db_connection`, ensures **`jobs/init_tables.py`** pipeline tables exist, creates a **`pipeline_runs`** row, runs **`pipelines.gran_gov.main.grants_main(conn, job_id)`**, updates run status, closes connection. |
| **`init_tables.py`** | `create_pipeline_tables(conn)` — creates **`pipeline_runs`** and **`pipeline_logs`** if missing. |
| **`log_utils.py`** | Helpers such as `create_pipeline_run`, `update_pipeline_run`, and `log(conn, job_id, message, level)` for pipeline observability. |

## Run daily jobs

From the **repository root**:

```bash
python -m jobs.daily_jobs
```

> `daily_jobs.py` may use `test_mode=True` when connecting (see `get_db_connection` in `db/db_util.py`) — adjust there if you want production `grants.db` for scheduled runs.

## Planned
- **`weekly_jobs.py`** (or similar) for weekly pipelines once defined.
- Move the grant tagging (for already seen grants) to the weekly job to reduce use of AI slightly
- Weekly job can also review the updated grants to see if they are still tribal eligible (maybe do this daily?)
