"""
Grant embedding utilities for grants_ai.

Reusable from scripts.extract_embed, the daily ingestion job, and the API.

Env:
  OPENAI_API_KEY (required)
  EMBEDDING_MODEL (optional, default text-embedding-3-small)
"""
from __future__ import annotations

import json
import os
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

from db.db_util import row_get

load_dotenv()

EMBEDDING_DIM = 1536
DEFAULT_EMBED_API_BATCH_SIZE = 64
DEFAULT_EMBED_CHUNK_SIZE = 500
DEFAULT_SEMANTIC_SEARCH_LIMIT = 50
MAX_SEMANTIC_SEARCH_LIMIT = 150
OPEN_TRIBAL_STATUSES = ("posted", "forecasted")


def get_embedding_model_name() -> str:
    return os.getenv("EMBEDDING_MODEL") or "text-embedding-3-small"


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


def vector_to_db_value(vector: list[float]) -> str:
    """Format a vector literal for Postgres pgvector."""
    return "[" + ",".join(f"{float(x):.8g}" for x in vector) + "]"


def fetch_grants_ai_by_opportunity_ids(
    conn: Any,
    opportunity_ids: list[str],
) -> list[dict]:
    """Load grants_ai rows (with grant title/agency) for specific opportunity ids."""
    ids = [str(oid) for oid in opportunity_ids if str(oid).strip()]
    if not ids:
        return []

    in_ph = ", ".join(["%s"] * len(ids))
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
    sql = """
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
    vector_value = vector_to_db_value(embedding)
    sql = """
        UPDATE grants_ai
        SET embedding_document = %s,
            embedding = %s::vector,
            model = %s
        WHERE opportunity_id = %s
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


def embed_query(text: str, *, model: str | None = None) -> list[float]:
    """Embed a single user search query."""
    query = (text or "").strip()
    if not query:
        raise ValueError("Query must be a non-empty string.")
    vectors = embed_texts([query], model=model)
    return vectors[0]


def _search_row_dict(row: Any) -> dict:
    if isinstance(row, dict):
        return {
            "opportunity_id": row.get("opportunity_id"),
            "title": row.get("title"),
            "description": row.get("description"),
            "agency": row.get("agency"),
            "status": row.get("status"),
            "posted_date": row.get("posted_date"),
            "estimated_funding": row.get("estimated_funding"),
            "grant_gov_url": row.get("grant_gov_url"),
            "purpose": row.get("purpose"),
            "relevancy_score": row.get("relevancy_score"),
            "freshness_score": row.get("freshness_score"),
            "similarity_score": row.get("similarity_score"),
        }
    return {
        "opportunity_id": row_get(row, "opportunity_id", 0),
        "title": row_get(row, "title", 1),
        "description": row_get(row, "description", 2),
        "agency": row_get(row, "agency", 3),
        "status": row_get(row, "status", 4),
        "posted_date": row_get(row, "posted_date", 5),
        "estimated_funding": row_get(row, "estimated_funding", 6),
        "grant_gov_url": row_get(row, "grant_gov_url", 7),
        "purpose": row_get(row, "purpose", 8),
        "relevancy_score": row_get(row, "relevancy_score", 9),
        "freshness_score": row_get(row, "freshness_score", 10),
        "similarity_score": row_get(row, "similarity_score", 11),
    }


def search_tribal_grants_semantic(
    conn: Any,
    query: str,
    *,
    limit: int = DEFAULT_SEMANTIC_SEARCH_LIMIT,
    statuses: tuple[str, ...] = OPEN_TRIBAL_STATUSES,
) -> list[dict]:
    """
    Embed a user query and return tribally eligible grants ranked by cosine similarity.

    Only includes open grants (posted/forecasted by default) with a stored embedding.
    """
    query = (query or "").strip()
    if not query:
        raise ValueError("Query must be a non-empty string.")

    limit = max(1, min(int(limit), MAX_SEMANTIC_SEARCH_LIMIT))
    query_vector = embed_query(query)
    vector_literal = vector_to_db_value(query_vector)
    status_ph = ", ".join(["%s"] * len(statuses))
    sql = f"""
        SELECT
            g.opportunity_id,
            g.title,
            g.description,
            g.agency,
            g.status,
            g.posted_date,
            g.estimated_funding,
            g.grant_gov_url,
            ga.purpose,
            te.relevancy_score,
            te.freshness_score,
            1 - (ga.embedding <=> %s::vector) AS similarity_score
        FROM grants_ai ga
        INNER JOIN grants g ON g.opportunity_id = ga.opportunity_id
        INNER JOIN tribal_eligibility te ON te.opportunity_id = g.opportunity_id
        WHERE ga.embedding IS NOT NULL
          AND te.is_tribal_eligible = TRUE
          AND g.status IN ({status_ph})
        ORDER BY ga.embedding <=> %s::vector ASC
        LIMIT %s
    """
    params: tuple[Any, ...] = (vector_literal, *statuses, vector_literal, int(limit))
    cur = conn.cursor()
    cur.execute(sql, params)
    return [_search_row_dict(row) for row in cur.fetchall()]
