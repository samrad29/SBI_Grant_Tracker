"""
Grant embedding utilities for grants_ai.

Reusable from scripts.extract_embed and the daily ingestion job.

Env:
  OPENAI_API_KEY (required)
  OPENAI_EMBEDDING_MODEL (optional, default text-embedding-3-small)
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

from db.db_util import row_get

load_dotenv()

EMBEDDING_DIM = 1536
DEFAULT_EMBED_API_BATCH_SIZE = 64
DEFAULT_EMBED_CHUNK_SIZE = 500


def get_embedding_model_name() -> str:
    return os.getenv("EMBEDDING_MODEL") or "text-embedding-3-small"


def _placeholders(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return [raw]
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [raw]
    return [str(value).strip()] if str(value).strip() else []


def _section(title: str, items: list[str]) -> str:
    if not items:
        return ""
    lines = "\n".join(f"- {item}" for item in items)
    return f"{title}:\n{lines}\n"


def build_embedding_document(
    extracted: dict,
    *,
    title: str | None = None,
    agency: str | None = None,
) -> str:
    """
    Build the text blob stored in grants_ai.embedding_document and sent to OpenAI.
    """
    parts: list[str] = []

    if title and title.strip():
        parts.append(f"Title: {title.strip()}")
    if agency and agency.strip():
        parts.append(f"Agency: {agency.strip()}")

    purpose = (extracted.get("purpose") or "").strip()
    if purpose:
        parts.append(f"Purpose:\n{purpose}")

    parts.append(
        _section("Funding topics", _as_string_list(extracted.get("funding_topics")))
    )
    parts.append(
        _section("Desired outcomes", _as_string_list(extracted.get("desired_outcomes")))
    )
    parts.append(
        _section("Problems addressed", _as_string_list(extracted.get("problems_addressed")))
    )
    parts.append(
        _section("Project examples", _as_string_list(extracted.get("project_examples")))
    )
    parts.append(
        _section(
            "Common search queries",
            _as_string_list(extracted.get("common_search_queries")),
        )
    )

    document = "\n\n".join(part.strip() for part in parts if part and part.strip())
    return document.strip()


def build_embedding_document_from_row(row: dict) -> str:
    """Build an embedding document from a grants_ai (+ optional grants) DB row."""
    extracted = {
        "purpose": row.get("purpose"),
        "funding_topics": row.get("funding_topics"),
        "desired_outcomes": row.get("desired_outcomes"),
        "project_examples": row.get("project_examples"),
        "problems_addressed": row.get("problems_addressed"),
        "common_search_queries": row.get("common_search_queries"),
    }
    return build_embedding_document(
        extracted,
        title=row.get("title"),
        agency=row.get("agency"),
    )


def embed_texts(
    texts: list[str],
    *,
    model: str | None = None,
    batch_size: int = DEFAULT_EMBED_API_BATCH_SIZE,
) -> list[list[float]]:
    """Call OpenAI embeddings API in batches. Output order matches input order."""
    if not texts:
        return []

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for embeddings.")

    model = model or get_embedding_model_name()
    client = OpenAI(api_key=api_key)
    out: list[list[float]] = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        resp = client.embeddings.create(model=model, input=batch)
        ordered = sorted(resp.data, key=lambda row: row.index)
        for row in ordered:
            out.append(list(row.embedding))

    return out


def vector_to_db_value(conn: Any, vector: list[float]) -> Any:
    """Format a vector for the current database backend."""
    if isinstance(conn, sqlite3.Connection):
        return json.dumps(vector)
    return "[" + ",".join(f"{float(x):.8g}" for x in vector) + "]"


def fetch_grants_ai_by_opportunity_ids(
    conn: Any,
    opportunity_ids: list[str],
) -> list[dict]:
    """Load grants_ai rows (with grant title/agency) for specific opportunity ids."""
    ids = [str(oid) for oid in opportunity_ids if str(oid).strip()]
    if not ids:
        return []

    ph = _placeholders(conn)
    in_ph = ", ".join([ph] * len(ids))
    sql = f"""
        SELECT
            ga.opportunity_id,
            ga.purpose,
            ga.funding_topics,
            ga.desired_outcomes,
            ga.project_examples,
            ga.problems_addressed,
            ga.common_search_queries,
            ga.embedding_document,
            g.title,
            g.agency
        FROM grants_ai ga
        LEFT JOIN grants g ON g.opportunity_id = ga.opportunity_id
        WHERE ga.opportunity_id IN ({in_ph})
        ORDER BY ga.opportunity_id
    """
    cur = conn.cursor()
    cur.execute(sql, tuple(ids))
    rows = cur.fetchall()

    out: list[dict] = []
    for row in rows:
        out.append(
            {
                "opportunity_id": row_get(row, "opportunity_id", 0),
                "purpose": row_get(row, "purpose", 1),
                "funding_topics": row_get(row, "funding_topics", 2),
                "desired_outcomes": row_get(row, "desired_outcomes", 3),
                "project_examples": row_get(row, "project_examples", 4),
                "problems_addressed": row_get(row, "problems_addressed", 5),
                "common_search_queries": row_get(row, "common_search_queries", 6),
                "embedding_document": row_get(row, "embedding_document", 7),
                "title": row_get(row, "title", 8),
                "agency": row_get(row, "agency", 9),
            }
        )
    return out


def fetch_grants_ai_pending_embedding(
    conn: Any,
    *,
    limit: int | None = None,
) -> list[dict]:
    """Rows in grants_ai that still need an embedding."""
    ph = _placeholders(conn)
    sql = f"""
        SELECT
            ga.opportunity_id,
            ga.purpose,
            ga.funding_topics,
            ga.desired_outcomes,
            ga.project_examples,
            ga.problems_addressed,
            ga.common_search_queries,
            ga.embedding_document,
            g.title,
            g.agency
        FROM grants_ai ga
        LEFT JOIN grants g ON g.opportunity_id = ga.opportunity_id
        WHERE ga.embedding IS NULL
          AND ga.purpose IS NOT NULL
          AND TRIM(ga.purpose) != ''
        ORDER BY ga.opportunity_id
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()

    out: list[dict] = []
    for row in rows:
        out.append(
            {
                "opportunity_id": row_get(row, "opportunity_id", 0),
                "purpose": row_get(row, "purpose", 1),
                "funding_topics": row_get(row, "funding_topics", 2),
                "desired_outcomes": row_get(row, "desired_outcomes", 3),
                "project_examples": row_get(row, "project_examples", 4),
                "problems_addressed": row_get(row, "problems_addressed", 5),
                "common_search_queries": row_get(row, "common_search_queries", 6),
                "embedding_document": row_get(row, "embedding_document", 7),
                "title": row_get(row, "title", 8),
                "agency": row_get(row, "agency", 9),
            }
        )
    return out


def count_grants_ai_pending_embedding(conn: Any) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM grants_ai
        WHERE embedding IS NULL
          AND purpose IS NOT NULL
          AND TRIM(purpose) != ''
        """
    )
    row = cur.fetchone()
    if row is None:
        return 0
    if isinstance(row, dict):
        return int(next(iter(row.values())))
    return int(row[0])


def save_grant_embedding(
    conn: Any,
    opportunity_id: str,
    embedding_document: str,
    embedding: list[float],
    *,
    model: str | None = None,
) -> bool:
    """Persist embedding_document, embedding vector, and model name."""
    model = model or get_embedding_model_name()
    ph = _placeholders(conn)
    vector_value = vector_to_db_value(conn, embedding)

    if isinstance(conn, sqlite3.Connection):
        sql = f"""
            UPDATE grants_ai
            SET embedding_document = {ph},
                embedding = {ph},
                model = {ph}
            WHERE opportunity_id = {ph}
        """
        params = (embedding_document, vector_value, model, opportunity_id)
    else:
        sql = f"""
            UPDATE grants_ai
            SET embedding_document = {ph},
                embedding = {ph}::vector,
                model = {ph}
            WHERE opportunity_id = {ph}
        """
        params = (embedding_document, vector_value, model, opportunity_id)

    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error saving embedding for {opportunity_id}: {e}")
        return False


def embed_grants_ai_rows(
    conn: Any,
    rows: list[dict],
    *,
    model: str | None = None,
    api_batch_size: int = DEFAULT_EMBED_API_BATCH_SIZE,
) -> tuple[int, int]:
    """
    Build documents, batch-embed via OpenAI, and save to grants_ai.

    Returns (saved_count, failed_count).
    """
    if not rows:
        return 0, 0

    model = model or get_embedding_model_name()
    documents: list[str] = []
    opportunity_ids: list[str] = []

    for row in rows:
        oid = str(row["opportunity_id"])
        document = (row.get("embedding_document") or "").strip()
        if not document:
            document = build_embedding_document_from_row(row)
        if not document:
            print(f"Skipping {oid}: empty embedding document")
            continue
        documents.append(document)
        opportunity_ids.append(oid)

    if not documents:
        return 0, len(rows)

    vectors = embed_texts(documents, model=model, batch_size=api_batch_size)
    if len(vectors) != len(documents):
        raise RuntimeError("OpenAI embedding count does not match document count")

    saved = 0
    failed = 0
    for oid, document, vector in zip(opportunity_ids, documents, vectors):
        if len(vector) != EMBEDDING_DIM:
            print(
                f"Skipping {oid}: expected {EMBEDDING_DIM} dimensions, got {len(vector)}"
            )
            failed += 1
            continue
        if save_grant_embedding(
            conn,
            oid,
            document,
            vector,
            model=model,
        ):
            saved += 1
        else:
            failed += 1

    skipped = len(rows) - len(opportunity_ids)
    return saved, failed + skipped
