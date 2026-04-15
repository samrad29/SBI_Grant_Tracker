from __future__ import annotations

import sqlite3

from db.db_util import ensure_postgres_id_defaults

SCHEMA_SQL = r"""

-- Current "latest known" view of each opportunity
CREATE TABLE IF NOT EXISTS grants (
  opportunity_id TEXT PRIMARY KEY,

  number TEXT,
  title TEXT,
  agency TEXT,
  agency_code TEXT,
  status TEXT,

  posted_date TEXT,  -- ISO-8601 date string: YYYY-MM-DD (or NULL)
  close_date TEXT,   -- ISO-8601 date string: YYYY-MM-DD (or NULL)

  deadline_date TEXT,
  deadline_description TEXT,
  last_updated_date TEXT,

  award_floor REAL,
  award_ceiling REAL,
  estimated_funding REAL,
  cost_sharing TEXT,

  category TEXT,
  eligibility_description TEXT,
  alns TEXT,            -- JSON array string
  eligibilities TEXT, -- JSON array string

  description TEXT,
  funding_categories TEXT, -- JSON array string
  attachments TEXT,   -- JSON array string

  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_grants_status ON grants(status);

-- Historical snapshots for diffing
CREATE TABLE IF NOT EXISTS grant_snapshots (
  id BIGSERIAL PRIMARY KEY,

  opportunity_id TEXT NOT NULL,

  fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data_json TEXT NOT NULL,   -- canonical JSON string of your normalized data
  hash TEXT NOT NULL,        -- SHA256 of canonical JSON

  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);

-- Prevent exact duplicate snapshots for the same opportunity+content
CREATE UNIQUE INDEX IF NOT EXISTS uniq_grant_snapshot_hash
ON grant_snapshots(opportunity_id, hash);

CREATE INDEX IF NOT EXISTS idx_snapshot_opportunity_time
ON grant_snapshots(opportunity_id, fetched_at DESC);

-- Alerts generated from comparing snapshot N-1 to snapshot N
CREATE TABLE IF NOT EXISTS grant_alerts (
  id BIGSERIAL PRIMARY KEY,

  opportunity_id TEXT NOT NULL,

  detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  alert_type TEXT NOT NULL, -- e.g. deadline_extended
  field TEXT NOT NULL,      -- e.g. close_date, total_funding, attachments_added, etc.

  old_value TEXT,          -- store as text (often JSON string for lists)
  new_value TEXT,

  old_snapshot_hash TEXT NOT NULL,
  new_snapshot_hash TEXT NOT NULL,

  fetched_at_old TEXT,
  fetched_at_new TEXT,

  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);

-- Deduplicate alerts so re-runs don't create duplicates
CREATE UNIQUE INDEX IF NOT EXISTS uniq_grant_alert_dedupe
ON grant_alerts(opportunity_id, alert_type, field, old_snapshot_hash, new_snapshot_hash);


CREATE TABLE IF NOT EXISTS tribal_eligibility (
  id BIGSERIAL PRIMARY KEY,
  opportunity_id TEXT NOT NULL,
  model TEXT NOT NULL,
  eligibility_score INTEGER NOT NULL,
  eligibility_reasoning TEXT NOT NULL,
  is_tribal_eligible BOOLEAN NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_tribal_eligibility_opportunity_id
ON tribal_eligibility(opportunity_id);

CREATE TABLE IF NOT EXISTS grant_tags (
  id BIGSERIAL PRIMARY KEY,
  opportunity_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  tag_score INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_grant_tags_opportunity_id
ON grant_tags(opportunity_id, tag);

CREATE TABLE IF NOT EXISTS user_grant_activity (
  id BIGSERIAL PRIMARY KEY,
  user_id TEXT NOT NULL,
  opportunity_id TEXT NOT NULL,
  is_bookmarked BOOLEAN NOT NULL DEFAULT FALSE,
  status TEXT NOT NULL,  -- viewed, saved, applied
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS grant_checklist_items (
  item_id BIGSERIAL PRIMARY KEY,
  user_id TEXT NOT NULL,
  opportunity_id TEXT NOT NULL,
  item_name TEXT NOT NULL,
  is_completed BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_grant_checklist_items_opportunity_id
ON grant_checklist_items(user_id, opportunity_id, item_name);
"""


def _schema_for_sqlite(sql: str) -> str:
    """SQLite has no BIGSERIAL; use INTEGER PRIMARY KEY AUTOINCREMENT for surrogate ids."""
    return sql.replace("BIGSERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")


def create_tables(conn) -> None:
    """
    Create all grant ingestion tables (idempotent).

    SQLite: uses executescript (multiple statements). Postgres: one statement per execute().
    """
    if isinstance(conn, sqlite3.Connection):
        conn.executescript(_schema_for_sqlite(SCHEMA_SQL))
    else:
        for part in SCHEMA_SQL.split(";"):
            part = part.strip()
            if not part:
                continue
            conn.execute(part + ";")
        ensure_postgres_id_defaults(
            conn,
            (
                ("grant_snapshots", "grant_snapshots_id_seq"),
                ("grant_alerts", "grant_alerts_id_seq"),
                ("tribal_eligibility", "tribal_eligibility_id_seq"),
                ("grant_tags", "grant_tags_id_seq"),
                ("user_grant_activity", "user_grant_activity_id_seq"),
            ),
        )
    conn.commit()


__all__ = ["create_tables"]