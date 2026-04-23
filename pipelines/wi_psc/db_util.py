from __future__ import annotations

import json


def init_tables(conn):
    """
    Initialize the tables for the wisconsin psc pipeline
    """
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(
        """
CREATE TABLE IF NOT EXISTS grants (
  opportunity_source TEXT NOT NULL,
  opportunity_id TEXT NOT NULL,

  number TEXT,
  title TEXT,
  agency TEXT,
  agency_code TEXT,
  status TEXT,

  posted_date TEXT,
  close_date TEXT,

  deadline_date TEXT,
  deadline_description TEXT,
  last_updated_date TEXT,

  award_floor REAL,
  award_ceiling REAL,
  estimated_funding REAL,
  cost_sharing TEXT,

  category TEXT,
  eligibility_description TEXT,
  alns TEXT,
  eligibilities TEXT,

  description TEXT,
  funding_categories TEXT,
  attachments TEXT,

  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS oei_programs (
    url TEXT NOT NULL PRIMARY KEY,
    program_name TEXT NOT NULL,
    program_status TEXT NOT NULL,
    attachments TEXT,
    elibilities TEXT,
    description TEXT,
    estimated_funding REAL,
    estimated_funding_description TEXT,
    deadline_date TEXT,
    webpage_text_hash TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS attachment_documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attachment_url TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    char_len INTEGER NOT NULL,
    embedding_model TEXT NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(attachment_url, content_sha256)
);

CREATE TABLE IF NOT EXISTS attachment_chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL REFERENCES attachment_documents(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    UNIQUE(document_id, chunk_index)
);

CREATE TABLE IF NOT EXISTS attachment_chunk_embeddings (
    chunk_id INTEGER PRIMARY KEY REFERENCES attachment_chunks(id) ON DELETE CASCADE,
    dim INTEGER NOT NULL,
    vector BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS ai_extraction_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prompt TEXT NOT NULL,
    raw_response TEXT,
    extracted_json TEXT
);
"""
    )
    conn.commit()


def get_stored_hash(conn, url: str) -> str | None:
    row = conn.execute(
        "SELECT webpage_text_hash FROM oei_programs WHERE url = ?",
        (url,),
    ).fetchone()
    if row is None:
        return None
    return row[0]


def save_ai_extraction(conn, extraction: dict, url: str, webpage_text_hash: str) -> None:
    """
    Upsert one row into oei_programs from AI JSON and content hash.
    Expected extraction keys: program_name, program_status, description,
    elibilities (list), estimated_funding, deadline_date, attachments (list).
    """
    program_name = (extraction.get("program_name") or "").strip() or "Unknown program"
    program_status = (extraction.get("program_status") or "").strip() or "unknown"
    description = extraction.get("description")
    if description is not None:
        description = str(description)

    elig = extraction.get("elibilities")
    if elig is None:
        elig_json = None
    elif isinstance(elig, list):
        elig_json = json.dumps([str(x) for x in elig])
    else:
        elig_json = json.dumps([str(elig)])

    att = extraction.get("attachments")
    if att is None:
        att_json = None
    elif isinstance(att, list):
        att_json = json.dumps([str(x) for x in att])
    else:
        att_json = json.dumps([str(att)])

    est = extraction.get("estimated_funding")
    if est is None or est == "":
        est_funding = None
    else:
        try:
            est_funding = float(est)
        except (TypeError, ValueError):
            est_funding = None

    estimated_funding_description = extraction.get("estimated_funding_description")
    if estimated_funding_description is not None:
        estimated_funding_description = str(estimated_funding_description).strip()

    deadline = extraction.get("deadline_date")
    deadline_str = None if deadline in (None, "") else str(deadline)

    conn.execute(
        """
        INSERT INTO oei_programs (
            url, program_name, program_status, attachments, elibilities,
            description, estimated_funding, estimated_funding_description, deadline_date, webpage_text_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            program_name = excluded.program_name,
            program_status = excluded.program_status,
            attachments = excluded.attachments,
            elibilities = excluded.elibilities,
            description = excluded.description,
            estimated_funding = excluded.estimated_funding,
            estimated_funding_description = excluded.estimated_funding_description,
            deadline_date = excluded.deadline_date,
            webpage_text_hash = excluded.webpage_text_hash,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            url,
            program_name,
            program_status,
            att_json,
            elig_json,
            description,
            est_funding,
            estimated_funding_description,
            deadline_str,
            webpage_text_hash,
        ),
    )
    conn.commit()


def save_ai_extraction_log(
    conn,
    *,
    url: str,
    prompt: str,
    raw_response: str | None,
    extracted_payload: dict | None,
) -> None:
    extracted_json = None
    if extracted_payload is not None:
        extracted_json = json.dumps(extracted_payload, ensure_ascii=False)

    conn.execute(
        """
        INSERT INTO ai_extraction_logs (url, prompt, raw_response, extracted_json)
        VALUES (?, ?, ?, ?)
        """,
        (url, prompt, raw_response, extracted_json),
    )
    conn.commit()
