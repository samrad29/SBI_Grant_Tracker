"""
Extract and embed grants into grants_ai.

Usage (from repo root):

  # Sample extraction to CSV for manual review
  python -m scripts.extract_embed --limit 50 --output grant_extraction_sample.csv

  # Extract one chunk into grants_ai (skips grants already extracted)
  python -m scripts.extract_embed --save-db --limit 250

  # Embed one chunk of grants_ai rows missing vectors (default 500)
  python -m scripts.extract_embed --embed-db

  # Extract then embed in one run
  python -m scripts.extract_embed --save-db --embed-db

  # Preview what a chunk would process
  python -m scripts.extract_embed --embed-db --dry-run

  # One-time: enable pgvector on Render Postgres (uses DATABASE_URL from .env)
  python -m scripts.enable_pgvector

Requires OPENAI_API_KEY.
Uses DATABASE_URL when set, otherwise local SQLite (grants.db).
Postgres: pgvector must be enabled once (see scripts.enable_pgvector).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time

from db.db_util import get_db_connection
from pipelines.gran_gov.init_tables import create_grants_ai_table
from pipelines.ai_utils.embed import (
    DEFAULT_EMBED_CHUNK_SIZE,
    count_grants_ai_pending_embedding,
    embed_grants_ai_rows,
    fetch_grants_ai_pending_embedding,
    get_embedding_model_name,
)
from pipelines.ai_utils.extraction import (
    count_grants_pending_extraction,
    create_grants_ai_row,
    extract_grant_ai_fields,
    fetch_grants_for_extraction,
)
from pipelines.ai_utils.llm_clients import create_llm_service

DEFAULT_CSV_LIMIT = 50
DEFAULT_DB_CHUNK_LIMIT = 250
DEFAULT_EMBED_LIMIT = DEFAULT_EMBED_CHUNK_SIZE

CSV_FIELDS = [
    "opportunity_id",
    "number",
    "title",
    "agency",
    "status",
    "grant_gov_url",
    "purpose",
    "funding_topics",
    "eligible_applicants",
    "project_examples",
    "problems_addressed",
    "desired_outcomes",
    "common_search_queries",
    "extraction_error",
]


def _json_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _row_from_extraction(grant: dict, extracted: dict | None, error: str = "") -> dict:
    if not extracted:
        return {
            **grant,
            "purpose": "",
            "funding_topics": "",
            "eligible_applicants": "",
            "project_examples": "",
            "problems_addressed": "",
            "desired_outcomes": "",
            "common_search_queries": "",
            "extraction_error": error or "extraction returned no data",
        }
    return {
        **grant,
        "purpose": extracted.get("purpose") or "",
        "funding_topics": _json_cell(extracted.get("funding_topics")),
        "eligible_applicants": _json_cell(extracted.get("eligible_applicants")),
        "project_examples": _json_cell(extracted.get("project_examples")),
        "problems_addressed": _json_cell(extracted.get("problems_addressed")),
        "desired_outcomes": _json_cell(extracted.get("desired_outcomes")),
        "common_search_queries": _json_cell(extracted.get("common_search_queries")),
        "extraction_error": error,
    }


def _ensure_grants_ai_table() -> None:
    conn = get_db_connection()
    try:
        create_grants_ai_table(conn)
        print("Ensured grants_ai table exists.")
    finally:
        conn.close()


def _run_csv_export(args: argparse.Namespace) -> int:
    conn = get_db_connection()
    try:
        grants = fetch_grants_for_extraction(
            conn,
            limit=args.limit,
            status_equals=args.status,
            skip_existing=False,
        )
    finally:
        conn.close()

    print(f"Found {len(grants)} grant(s) with descriptions to extract.")

    if args.dry_run:
        for grant in grants[:20]:
            title = (grant.get("title") or "")[:60]
            print(f"  {grant['opportunity_id']}: {title}")
        if len(grants) > 20:
            print(f"  ... and {len(grants) - 20} more")
        return 0

    if not grants:
        print("Nothing to extract.")
        return 0

    llm_service = create_llm_service(job_id=-1)
    rows: list[dict] = []
    llm_calls = 0
    errors = 0

    for i, grant in enumerate(grants, start=1):
        oid = grant["opportunity_id"]
        title = (grant.get("title") or "")[:50]
        print(f"[{i}/{len(grants)}] {oid} — {title}")

        error = ""
        extracted = None
        try:
            extracted = extract_grant_ai_fields(grant, llm_service)
            llm_calls += 1
            if args.delay > 0 and i < len(grants):
                time.sleep(args.delay)
        except Exception as e:
            error = str(e)
            errors += 1
            print(f"  ERROR: {e}", file=sys.stderr)

        if extracted is None and not error:
            errors += 1
            print("  ERROR: extraction returned no data", file=sys.stderr)

        rows.append(_row_from_extraction(grant, extracted, error=error))
        print("  -> ok" if extracted else "  -> failed")

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    ok = len(rows) - errors
    print(
        f"\nWrote {len(rows)} row(s) to {args.output!r} "
        f"({ok} ok, {errors} failed, {llm_calls} OpenAI call(s))."
    )
    return 0 if errors == 0 else 1


def _run_db_chunk(args: argparse.Namespace) -> int:
    _ensure_grants_ai_table()

    conn = get_db_connection()
    try:
        pending = count_grants_pending_extraction(conn, status_equals=args.status)
        grants = fetch_grants_for_extraction(
            conn,
            limit=args.limit,
            status_equals=args.status,
            skip_existing=True,
        )
    finally:
        conn.close()

    print(f"Pending extraction: {pending} grant(s) not yet in grants_ai.")
    print(f"This chunk: up to {args.limit} grant(s); loaded {len(grants)}.")

    if args.dry_run:
        for grant in grants[:20]:
            title = (grant.get("title") or "")[:60]
            print(f"  {grant['opportunity_id']}: {title}")
        if len(grants) > 20:
            print(f"  ... and {len(grants) - 20} more")
        remaining_after = max(pending - len(grants), 0)
        print(f"After this chunk, about {remaining_after} grant(s) would remain.")
        return 0

    if not grants:
        print("Nothing to do — all matching grants are already in grants_ai.")
        return 0

    llm_service = create_llm_service(job_id=-1)
    conn = get_db_connection()
    saved = 0
    errors = 0

    try:
        for i, grant in enumerate(grants, start=1):
            oid = grant["opportunity_id"]
            title = (grant.get("title") or "")[:50]
            print(f"[{i}/{len(grants)}] {oid} — {title}")

            try:
                ok = create_grants_ai_row(conn, grant, llm_service)
            except Exception as e:
                errors += 1
                print(f"  ERROR: {e}", file=sys.stderr)
                continue

            if ok:
                saved += 1
                print("  -> saved")
            else:
                errors += 1
                print("  -> failed", file=sys.stderr)

            if args.delay > 0 and i < len(grants):
                time.sleep(args.delay)
    finally:
        conn.close()

    remaining = max(pending - saved, 0)
    print(
        f"\nChunk complete: saved {saved}, failed {errors}, "
        f"about {remaining} grant(s) still pending."
    )
    print("Re-run the same command to process the next chunk.")
    return 0 if errors == 0 else 1


def _run_embed_chunk(args: argparse.Namespace) -> int:
    _ensure_grants_ai_table()

    limit = args.embed_limit if args.embed_limit is not None else DEFAULT_EMBED_LIMIT
    conn = get_db_connection()
    try:
        pending = count_grants_ai_pending_embedding(conn)
        rows = fetch_grants_ai_pending_embedding(conn, limit=limit)
    finally:
        conn.close()

    model = get_embedding_model_name()
    print(f"Pending embedding: {pending} grant(s) in grants_ai.")
    print(f"This chunk: up to {limit} grant(s); loaded {len(rows)}.")
    print(f"Embedding model: {model}")

    if args.dry_run:
        for row in rows[:20]:
            title = (row.get("title") or "")[:60]
            print(f"  {row['opportunity_id']}: {title}")
        if len(rows) > 20:
            print(f"  ... and {len(rows) - 20} more")
        remaining_after = max(pending - len(rows), 0)
        print(f"After this chunk, about {remaining_after} grant(s) would remain.")
        return 0

    if not rows:
        print("Nothing to do — all extracted grants already have embeddings.")
        return 0

    conn = get_db_connection()
    try:
        saved, failed = embed_grants_ai_rows(conn, rows)
    finally:
        conn.close()

    remaining = max(pending - saved, 0)
    print(
        f"\nEmbedding chunk complete: saved {saved}, failed {failed}, "
        f"about {remaining} grant(s) still pending."
    )
    print("Re-run the same command to process the next chunk.")
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract and embed grants_ai rows (CSV review or DB chunks)."
    )
    parser.add_argument(
        "--save-db",
        action="store_true",
        help="Extract grant fields into grants_ai.",
    )
    parser.add_argument(
        "--embed-db",
        action="store_true",
        help="Embed grants_ai rows that are missing vectors.",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="grant_extraction_sample.csv",
        help="Output CSV path when not using --save-db.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Max grants per extraction chunk. Default: 50 for CSV, 250 for --save-db."
        ),
    )
    parser.add_argument(
        "--embed-limit",
        type=int,
        default=None,
        help=(
            "Max grants per embedding chunk (default: 500). "
            "Falls back to --limit when not set."
        ),
    )
    parser.add_argument(
        "--status",
        default=None,
        help="Only extract grants with this status value.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds to sleep between OpenAI chat calls during extraction (default: 0.5).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List grants in this chunk without calling OpenAI or writing output.",
    )
    args = parser.parse_args()

    if not args.save_db and not args.embed_db:
        if args.limit is None:
            args.limit = DEFAULT_CSV_LIMIT
        return _run_csv_export(args)

    rc = 0
    if args.save_db:
        if args.limit is None:
            args.limit = DEFAULT_DB_CHUNK_LIMIT
        rc = _run_db_chunk(args)
        if rc != 0:
            return rc

    if args.embed_db:
        if args.embed_limit is None:
            args.embed_limit = DEFAULT_EMBED_LIMIT
        rc = _run_embed_chunk(args)

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
