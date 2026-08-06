"""
Smoke-test semantic grant search using the same functions as the API endpoint.

Does not start the Flask app or call HTTP — runs search_tribal_grants_semantic()
directly, then applies the API response formatter for comparison.

Usage (from repo root, venv active):

  python -m scripts.test_semantic_search
  python -m scripts.test_semantic_search --limit 10
  python -m scripts.test_semantic_search --output semantic_search_smoke.json

Requires OPENAI_API_KEY and DATABASE_URL (Postgres with pgvector + embedded grants).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from app.routes.api import _format_semantic_search_results
from db.db_util import get_db_connection, is_test_mode
from pipelines.ai_utils.embed import search_tribal_grants_semantic

SAMPLE_QUERIES = [
    "broadband internet access for rural tribal communities",
    "mental health and substance abuse programs for Native families",
    "solar panels and renewable energy on tribal lands",
    "grants to replace lead pipes and improve drinking water",
    "workforce development and job training for Native youth",
]


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compact_row(row: dict, *, similarity_key: str = "similarity_score") -> dict:
    return {
        "opportunity_id": row.get("opportunity_id"),
        "title": (row.get("title") or "")[:80],
        "agency": row.get("agency"),
        "similarity_score": _as_float(row.get(similarity_key)),
        "relevancy_score": _as_float(row.get("relevancy_score")),
        "freshness_score": _as_float(row.get("freshness_score")),
    }


def _ranking_ids(rows: list[dict]) -> list[str]:
    return [str(row.get("opportunity_id")) for row in rows if row.get("opportunity_id") is not None]


def _print_result_block(title: str, rows: list[dict]) -> None:
    print(title)
    if not rows:
        print("  (no results)")
        return
    for i, row in enumerate(rows, start=1):
        compact = _compact_row(row)
        print(
            f"  {i}. {compact['opportunity_id']} | "
            f"sim={compact['similarity_score']!s:>8} | "
            f"rel={compact['relevancy_score']!s:>3} | "
            f"fresh={compact['freshness_score']!s:>3} | "
            f"{compact['title']}"
        )


def run_query(conn, query: str, *, limit: int) -> dict[str, Any]:
    """
    Run one query through vector search and API formatting.

    vector_ranked: raw rows from pgvector similarity (pre-API).
    api_response: same grants after _format_semantic_search_results (post-API).
    """
    vector_ranked = search_tribal_grants_semantic(conn, query, limit=limit)
    api_response = _format_semantic_search_results(vector_ranked)

    vector_ids = _ranking_ids(vector_ranked)
    api_ids = _ranking_ids(api_response)
    ranking_changed = vector_ids != api_ids

    return {
        "query": query,
        "result_count": len(vector_ranked),
        "vector_ranked": [_compact_row(row) for row in vector_ranked],
        "api_response": [_compact_row(row) for row in api_response],
        "ranking_changed_after_api_formatting": ranking_changed,
        "notes": (
            "API formatting only rounds/coerces fields; ranking is unchanged."
            if not ranking_changed
            else "Ranking differed after API formatting (unexpected)."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test semantic search functions (no HTTP)."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max results per query (default: 10).",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Optional JSON file to write full results.",
    )
    args = parser.parse_args()

    if not (os.getenv("DATABASE_URL") or "").strip():
        print("DATABASE_URL is required.", file=sys.stderr)
        return 1
    if not (os.getenv("OPENAI_API_KEY") or "").strip():
        print("OPENAI_API_KEY is required.", file=sys.stderr)
        return 1

    conn = get_db_connection(test_mode=is_test_mode())
    report: dict[str, Any] = {
        "ran_at": datetime.now(timezone.utc).isoformat(),
        "limit_per_query": args.limit,
        "queries": [],
    }

    try:
        for i, query in enumerate(SAMPLE_QUERIES, start=1):
            print(f"\n{'=' * 72}")
            print(f"Query {i}: {query}")
            print("=" * 72)

            result = run_query(conn, query, limit=args.limit)
            report["queries"].append(result)

            _print_result_block(
                "\nVector-ranked (search_tribal_grants_semantic / pgvector):",
                result["vector_ranked"],
            )
            _print_result_block(
                "\nAPI response (_format_semantic_search_results):",
                result["api_response"],
            )
            print(f"\n{result['notes']}")
    finally:
        conn.close()

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\nWrote JSON report to {args.output!r}")

    print(f"\nCompleted {len(SAMPLE_QUERIES)} sample queries.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
