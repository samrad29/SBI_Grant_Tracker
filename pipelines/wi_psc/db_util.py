from __future__ import annotations

import json
from datetime import datetime

from db.db_util import scalar_from_row


def init_tables(conn):
    """
    Initialize the tables for the wisconsin psc pipeline
    """

    id_pk = "BIGSERIAL PRIMARY KEY"
    vector_type = "BYTEA"

    statements = [
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
);""",
        """

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
);""",
        f"""

CREATE TABLE IF NOT EXISTS attachment_documents (
    id {id_pk},
    attachment_url TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    char_len INTEGER NOT NULL,
    embedding_model TEXT NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(attachment_url, content_sha256)
);""",
        f"""

CREATE TABLE IF NOT EXISTS attachment_chunks (
    id {id_pk},
    document_id INTEGER NOT NULL REFERENCES attachment_documents(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    UNIQUE(document_id, chunk_index)
);""",
        f"""

CREATE TABLE IF NOT EXISTS attachment_chunk_embeddings (
    chunk_id INTEGER PRIMARY KEY REFERENCES attachment_chunks(id) ON DELETE CASCADE,
    dim INTEGER NOT NULL,
    vector {vector_type} NOT NULL
);""",
        f"""

CREATE TABLE IF NOT EXISTS ai_extraction_logs (
    id {id_pk},
    url TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    prompt TEXT NOT NULL,
    raw_response TEXT,
    extracted_json TEXT
);
""",
    ]

    for stmt in statements:
        conn.execute(stmt)
    conn.commit()


def get_stored_hash(conn, url: str) -> str | None:
    row = conn.execute(
        "SELECT webpage_text_hash FROM oei_programs WHERE url = %s",
        (url,),
    ).fetchone()
    if row is None:
        return None
    return scalar_from_row(row)


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

    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn.execute(
        """
        INSERT INTO grants (
            opportunity_source,
            opportunity_id,
            number,
            title,
            status,
            deadline_date,
            estimated_funding,
            eligibilities,
            description,
            attachments,
            updated_at,
            last_seen_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            "wi_psc_oei",
            f"{url}-{program_name}",
            -1,
            program_name,
            program_status,
            deadline_str,
            est_funding,
            elig_json,
            description,
            att_json,
            date_str,
            date_str,
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
        VALUES (%s, %s, %s, %s)
        """,
        (url, prompt, raw_response, extracted_json),
    )
    conn.commit()
