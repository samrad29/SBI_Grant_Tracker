from __future__ import annotations

import sqlite3

# SQLite schema for the grant ingestion/diff workflow.
# Kept as raw SQL so the rest of the code can simply call `create_tables(conn)`.
SCHEMA_SQL = r"""
PRAGMA foreign_keys = ON;

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

  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  last_seen_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_grants_status ON grants(status);

-- Historical snapshots for diffing
CREATE TABLE IF NOT EXISTS grant_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,

  opportunity_id TEXT NOT NULL,

  fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
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
  id INTEGER PRIMARY KEY AUTOINCREMENT,

  opportunity_id TEXT NOT NULL,

  detected_at TEXT NOT NULL DEFAULT (datetime('now')),

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
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  opportunity_id TEXT NOT NULL,
  model TEXT NOT NULL,
  eligibility_score INTEGER NOT NULL,
  eligibility_reasoning TEXT NOT NULL,
  is_tribal_eligible BOOLEAN NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS uniq_tribal_eligibility_opportunity_id
ON tribal_eligibility(opportunity_id);

CREATE TABLE IF NOT EXISTS grant_tags (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  opportunity_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  tag_score INTEGER NOT NULL,
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_grant_tags_opportunity_id
ON grant_tags(opportunity_id, tag);

CREATE TABLE IF NOT EXISTS user_grant_activity (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  opportunity_id TEXT NOT NULL,
  status TEXT NOT NULL,  -- viewed, saved, applied
  created_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (opportunity_id) REFERENCES grants(opportunity_id)
    ON DELETE CASCADE
);
"""


def create_tables(conn: sqlite3.Connection) -> None:
    """
    Create all grant ingestion tables (idempotent).

    Call this once after opening your sqlite3 connection.
    """

    # `executescript` runs multiple statements (including PRAGMAs).
    conn.executescript(SCHEMA_SQL)

    # If `grants.db` already exists from an earlier run, `CREATE TABLE IF NOT EXISTS`
    # won't add new columns. Ensure the columns that `ingestion_loop.py` expects exist.
    ensure_columns = {
        "number": "TEXT",
        "title": "TEXT",
        "agency": "TEXT",
        "agency_code": "TEXT",
        "status": "TEXT",
        "posted_date": "TEXT",
        "close_date": "TEXT",
        "deadline_date": "TEXT",
        "deadline_description": "TEXT",
        "last_updated_date": "TEXT",
        "award_floor": "REAL",
        "award_ceiling": "REAL",
        "estimated_funding": "REAL",
        "cost_sharing": "TEXT",
        "category": "TEXT",
        "eligibility_description": "TEXT",
        "alns": "TEXT",
        "eligibilities": "TEXT",
        "funding_categories": "TEXT",
        "description": "TEXT",
        "attachments": "TEXT",
        "updated_at": "TEXT",
        "last_seen_at": "TEXT",
    }

    existing_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(grants)").fetchall()
    }
    for col, col_type in ensure_columns.items():
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE grants ADD COLUMN {col} {col_type}")


__all__ = ["create_tables"]