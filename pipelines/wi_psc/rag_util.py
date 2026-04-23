"""
Attachment + program-page RAG using OpenAI embeddings and SQLite cache.

Env:
  OPENAI_API_KEY (required for indexing/retrieval)
  OPENAI_EMBEDDING_MODEL (optional, default text-embedding-3-small)

Rows in attachment_documents use attachment_url as the logical source key:
  - Program listing URL + hash(main_text) for extracted page text
  - Each attachment URL + hash(attachment_text) for downloaded files
"""

from __future__ import annotations

import array
import os
import re
from collections import defaultdict
from typing import Sequence

import numpy as np
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

EMBED_BATCH_SIZE = 64
# Smaller windows improve precision for sentence-level facts like "$10 million".
CHUNK_CHARS = 900
CHUNK_OVERLAP = 180

ELIGIBILITY_QUESTIONS = [
    "What types of entities and/or organizations are eligible to apply?",
    "Who should apply for this program?",
    "Can tribes apply for this program?",
    "Tribal eligibility for this program.",
]

FUNDING_QUESTIONS = [
    "What is the total currently available funding?",
    "How much money will be awarded to the winning applicants?",
    "What is the budget for the program?",
    "This program awards dollar funding amounts.",
    "Total funding pool for this year in dollars.",
]

DESCRIPTION_QUESTIONS = [
    "What is the purpose of the program?",
    "What is the goal of the program?",
    "What is the objective of the program?",
    "What is the goal of the program?",
]

DEADLINE_QUESTIONS = [
    "What is the application deadline?",
    "When do applications need to be submitted by?",
    "What is the deadline date and time for this program?",
]

PROGRAM_DESCRIPTION_QUESTIONS = [
    "What is this program trying to accomplish?",
    "What activities or project types does this program fund?",
    "Summarize the program description and scope.",
]

DEFAULT_RAG_QUERY_SETS: dict[str, list[str]] = {
    "eligibility": ELIGIBILITY_QUESTIONS,
    "funding": FUNDING_QUESTIONS,
    "deadline": DEADLINE_QUESTIONS,
    "program_description": PROGRAM_DESCRIPTION_QUESTIONS,
}

# Per-set retrieval depth; funding gets more room.
DEFAULT_TOP_K_PER_SET: dict[str, int] = {
    "eligibility": 3,
    "funding": 5,
    "deadline": 3,
    "program_description": 3,
    "general": 3,
}

FUNDING_KEYWORDS = (
    "funding",
    "award",
    "budget",
    "available",
    "million",
    "dollar",
    "pool",
    "up to",
)


def _funding_lexical_score(text: str) -> float:
    """
    Lightweight lexical score used to hybrid-rank funding chunks.
    Higher values for currency tokens + funding-specific words/phrases.
    """
    t = (text or "").lower()
    if not t:
        return 0.0
    currency_hits = len(re.findall(r"\$\s?\d[\d,]*(?:\.\d+)?", t))
    numeric_money = len(re.findall(r"\b\d+(?:\.\d+)?\s*(?:million|billion|thousand)\b", t))
    kw_hits = sum(1 for kw in FUNDING_KEYWORDS if kw in t)
    return float(currency_hits * 2 + numeric_money * 1.5 + kw_hits * 0.5)


def get_embedding_model_name() -> str:
    return os.getenv("OPENAI_EMBEDDING_MODEL") or "text-embedding-3-small"


def chunk_text(text: str, chunk_size: int = CHUNK_CHARS, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Overlapping character windows; drop empties."""
    t = (text or "").strip()
    if not t:
        return []
    if len(t) <= chunk_size:
        return [t]
    out: list[str] = []
    i = 0
    n = len(t)
    while i < n:
        end = min(i + chunk_size, n)
        piece = t[i:end].strip()
        if piece:
            out.append(piece)
        if end >= n:
            break
        i = max(end - overlap, i + 1)
    return out if out else [t[:chunk_size]]


def _vec_to_blob(vec: Sequence[float]) -> bytes:
    arr = array.array("f", [float(x) for x in vec])
    return arr.tobytes()


def _blob_to_vec(blob: bytes) -> np.ndarray:
    arr = array.array("f")
    arr.frombytes(blob)
    return np.asarray(arr.tolist(), dtype=np.float32)


def embed_texts_openai(texts: list[str], model: str | None = None) -> list[list[float]]:
    if not texts:
        return []
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for RAG embeddings.")
    model = model or get_embedding_model_name()
    client = OpenAI(api_key=api_key)
    out: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        batch = texts[i : i + EMBED_BATCH_SIZE]
        resp = client.embeddings.create(model=model, input=batch)
        ordered = sorted(resp.data, key=lambda d: d.index)
        for row in ordered:
            out.append(list(row.embedding))
    return out


def _delete_documents_at_url(conn, attachment_url: str) -> None:
    conn.execute("DELETE FROM attachment_documents WHERE attachment_url = ?", (attachment_url,))


def ensure_indexed(
    conn,
    attachment_url: str,
    full_text: str,
    embedding_model: str | None = None,
) -> int | None:
    """
    If (url, content_hash) already exists, return document id without re-embedding.
    Otherwise replace any prior rows for the same URL and insert chunks + vectors.
    """
    from web_scraping_utils import hash_attachment_text

    embedding_model = embedding_model or get_embedding_model_name()
    t = (full_text or "").strip()
    if not t:
        return None

    h = hash_attachment_text(t)
    row = conn.execute(
        "SELECT id FROM attachment_documents WHERE attachment_url = ? AND content_sha256 = ?",
        (attachment_url, h),
    ).fetchone()
    if row is not None:
        print(f"[rag] cache hit for {attachment_url[:80]}")
        return int(row[0])

    chunks = chunk_text(t)
    if not chunks:
        return None

    vectors = embed_texts_openai(chunks, embedding_model)
    if len(vectors) != len(chunks):
        raise RuntimeError("OpenAI embeddings count does not match chunk count")

    dim = len(vectors[0])
    _delete_documents_at_url(conn, attachment_url)
    cur = conn.execute(
        """
        INSERT INTO attachment_documents (attachment_url, content_sha256, char_len, embedding_model)
        VALUES (?, ?, ?, ?)
        """,
        (attachment_url, h, len(t), embedding_model),
    )
    doc_id = int(cur.lastrowid)

    for idx, (chunk, vec) in enumerate(zip(chunks, vectors)):
        ccur = conn.execute(
            "INSERT INTO attachment_chunks (document_id, chunk_index, text) VALUES (?, ?, ?)",
            (doc_id, idx, chunk),
        )
        chunk_id = int(ccur.lastrowid)
        conn.execute(
            "INSERT INTO attachment_chunk_embeddings (chunk_id, dim, vector) VALUES (?, ?, ?)",
            (chunk_id, dim, _vec_to_blob(vec)),
        )

    conn.commit()
    print(f"[rag] indexed {len(chunks)} chunks (dim={dim}) for {attachment_url[:80]}")
    return doc_id


def retrieve_for_program(
    conn,
    program_page_url: str,
    document_ids: list[int],
    queries: list[str] | None = None,
    query_sets: dict[str, list[str]] | None = None,
    *,
    top_k_total: int = 28,
    top_k_per_set: int = 3,
    per_source_cap: int = 5,
    embedding_model: str | None = None,
) -> str:
    """
    Retrieve top chunks for each question set separately, then merge into one
    evidence block with caps.
    """
    if not document_ids:
        return ""
    if query_sets is None:
        if queries:
            query_sets = {"general": queries}
        else:
            query_sets = DEFAULT_RAG_QUERY_SETS

    # Keep only non-empty groups/questions.
    query_sets = {
        set_name: [q.strip() for q in set_queries if q and q.strip()]
        for set_name, set_queries in query_sets.items()
        if set_queries
    }
    if not query_sets:
        return ""

    embedding_model = embedding_model or get_embedding_model_name()

    placeholders = ",".join("?" * len(document_ids))
    rows = conn.execute(
        f"""
        SELECT c.text, c.chunk_index, d.attachment_url, e.vector
        FROM attachment_chunks c
        JOIN attachment_documents d ON c.document_id = d.id
        JOIN attachment_chunk_embeddings e ON e.chunk_id = c.id
        WHERE d.id IN ({placeholders})
        ORDER BY d.id, c.chunk_index
        """,
        tuple(document_ids),
    ).fetchall()

    if not rows:
        return ""

    matrix = np.stack([_blob_to_vec(r["vector"]) for r in rows], axis=0)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    V = matrix / norms

    flat_queries: list[str] = []
    q_ranges: dict[str, tuple[int, int]] = {}
    cursor = 0
    for set_name, set_queries in query_sets.items():
        start = cursor
        flat_queries.extend(set_queries)
        cursor += len(set_queries)
        q_ranges[set_name] = (start, cursor)

    qvecs = embed_texts_openai(flat_queries, embedding_model)
    Q = np.asarray(qvecs, dtype=np.float32)
    qn = np.linalg.norm(Q, axis=1, keepdims=True)
    qn[qn == 0] = 1.0
    Q = Q / qn

    scores = V @ Q.T
    picked_pairs: list[tuple[str, int]] = []
    used_pairs: set[tuple[str, int]] = set()
    url_counts: dict[str, int] = defaultdict(int)

    for set_name, (q_start, q_end) in q_ranges.items():
        set_scores = scores[:, q_start:q_end]
        set_best = set_scores.max(axis=1)
        set_order = np.argsort(-set_best)
        set_cap = max(1, DEFAULT_TOP_K_PER_SET.get(set_name, top_k_per_set))
        set_picked = 0

        # Hybrid funding retrieval: semantic score + lexical boost.
        if set_name == "funding":
            lex = np.asarray([_funding_lexical_score(r["text"]) for r in rows], dtype=np.float32)
            if float(lex.max()) > 0:
                lex_norm = lex / float(lex.max())
            else:
                lex_norm = lex
            hybrid = set_best + (0.45 * lex_norm)
            set_order = np.argsort(-hybrid)

            # Coverage hook: force-include up to 2 strong lexical funding chunks.
            lexical_sorted = np.argsort(-lex)
            forced = 0
            for idx in lexical_sorted.tolist():
                if forced >= min(2, set_cap):
                    break
                if lex[idx] <= 0:
                    break
                if len(picked_pairs) >= top_k_total:
                    break
                src = rows[idx]["attachment_url"]
                if url_counts[src] >= per_source_cap:
                    continue
                pair = (set_name, idx)
                if pair in used_pairs:
                    continue
                used_pairs.add(pair)
                url_counts[src] += 1
                set_picked += 1
                forced += 1
                picked_pairs.append(pair)

        for idx in set_order.tolist():
            if set_picked >= set_cap:
                break
            if len(picked_pairs) >= top_k_total:
                break
            src = rows[idx]["attachment_url"]
            if url_counts[src] >= per_source_cap:
                continue
            pair = (set_name, idx)
            if pair in used_pairs:
                continue
            used_pairs.add(pair)
            url_counts[src] += 1
            set_picked += 1
            picked_pairs.append(pair)

    parts: list[str] = []
    set_counts: dict[str, int] = defaultdict(int)
    for rank, (set_name, idx) in enumerate(picked_pairs, start=1):
        r = rows[idx]
        src = r["attachment_url"]
        label = (
            "Program page (extracted text)"
            if src == program_page_url
            else f"Attachment: {src}"
        )
        n_chunks = sum(1 for x in rows if x["attachment_url"] == src)
        q_start, q_end = q_ranges[set_name]
        local_q_i = int(np.argmax(scores[idx, q_start:q_end]))
        q_i = q_start + local_q_i
        matched_query = flat_queries[q_i]
        matched_score = float(scores[idx, q_i])
        set_counts[set_name] += 1
        parts.append(
            f"--- RAG excerpt {rank} | set={set_name} | {label} | chunk {int(r['chunk_index']) + 1}/{max(n_chunks, 1)} ---\n"
            f"best_query ({q_i}): {matched_query}\n"
            f"score: {matched_score:.4f}\n"
            f"{r['text']}\n"
        )
    if set_counts:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(set_counts.items()))
        print(f"[rag] selected chunks by set: {summary}")
    return "\n".join(parts).strip()
