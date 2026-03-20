# Pipelines

This folder holds **data ingestion pipelines** (one subdirectory per source).

## `gran_gov/` — Grants.gov

Uses the [Grants.gov API](https://www.grants.gov/) (`search2`, `fetchOpportunity`).

### Layout
- `main.py` 
    - `get_opportunity_ids()`, `grants_main(conn, job_id)` — search + trim IDs, then `daily_ingestion` (used by `jobs.daily_jobs`). 
- `ingestion_loop.py` 
    - `daily_ingestion`, `upsert_grant_current`, snapshots, `grant_alerts`; may call Groq when needed. 
- `ingestion_utils.py` 
    - `fetch_opportunity`, `normalize_opportunity`, `update_grant_classification`, etc. 
-  `init_tables.py` 
    - `create_tables(conn)` — `grants`, `grant_snapshots`, `grant_alerts`, `grant_classifications`. 
- `change_detection.py` 
    - `detect_changes(old, new)` for alerts.
- `quick_classification.py` 
    - Heuristic scoring (`quick_classification`) — **import this** from `ingestion_loop` instead of importing `backlog_ingestion` (avoids circular imports). 
- `backlog_ingestion.py` 
    - One-off / large backfill: normalize, upsert, snapshot, quick classify, optional AI.
- `ai_utils.py`    
    - Groq client + `classify_grant`.

### Run backlog ingestion

From the **repository root**:

```bash
python -m pipelines.gran_gov.backlog_ingestion
```

### Notes

- **Groq** is used when heuristic classification is uncertain or rules say to double-check.
- Package imports use the **`pipelines.gran_gov.*`** prefix (run as modules from project root).
