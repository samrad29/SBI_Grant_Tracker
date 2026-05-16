"""
Scan grants in the database, use Groq to interpret deadline descriptions,
and write opportunities whose deadline has passed to a CSV file.

Usage (from repo root):

  python -m scripts.check_past_deadlines
  python -m scripts.check_past_deadlines --output past_deadlines.csv --limit 20
  python -m scripts.check_past_deadlines --reference-date 2026-05-15

Requires GROQ_API_KEY (and optionally GROQ_MODEL). Uses DATABASE_URL when set,
otherwise local SQLite (grants.db).

After classification, updates ``grants.status`` to ``closed`` in the database for
each opportunity whose deadline has passed (use ``--no-update-db`` to skip).
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import date, datetime

from db.db_util import get_db_connection
from pipelines.ai_utils.deadline_check import classify_deadline
from pipelines.ai_utils.llm_clients import create_llm_service
from pipelines.gran_gov.deadline_closure import (
    fetch_grants_for_deadline_check,
    mark_grants_status_closed,
)


def _parse_reference_date(value: str | None) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


CSV_FIELDS = [
    "opportunity_id",
    "number",
    "title",
    "agency",
    "status",
    "deadline_date",
    "deadline_description",
    "effective_deadline",
    "confidence",
    "reasoning",
    "used_llm",
    "grant_gov_url",
]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export grants whose deadline has passed (Groq + DB scan)."
    )
    parser.add_argument(
        "--output",
        "-o",
        default="past_deadlines.csv",
        help="Output CSV path (default: past_deadlines.csv).",
    )
    parser.add_argument(
        "--reference-date",
        help="Treat this YYYY-MM-DD as 'today' for comparisons (default: actual today).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of grants to evaluate (for testing).",
    )
    parser.add_argument(
        "--status",
        default=None,
        help="Only evaluate grants with this status value.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Seconds to sleep between Groq calls (default: 0.25).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List grants to scan without calling Groq or writing CSV.",
    )
    parser.add_argument(
        "--no-update-db",
        action="store_true",
        help="Do not set grants.status to closed in the database (CSV only).",
    )
    args = parser.parse_args()

    reference = _parse_reference_date(args.reference_date)
    conn = get_db_connection()

    try:
        grants = fetch_grants_for_deadline_check(
            conn,
            limit=args.limit,
            status_equals=args.status,
        )
    finally:
        conn.close()

    print(f"Found {len(grants)} grant(s) with deadline fields to evaluate.")
    print(f"Reference date: {reference.isoformat()}")

    if args.dry_run:
        for g in grants[:20]:
            print(f"  {g['opportunity_id']}: {g.get('title', '')[:60]}")
        if len(grants) > 20:
            print(f"  ... and {len(grants) - 20} more")
        return 0

    passed_rows: list[dict] = []
    llm_calls = 0
    llm_service = create_llm_service(job_id=-1)

    for i, grant in enumerate(grants, start=1):
        oid = grant["opportunity_id"]
        title = (grant.get("title") or "")[:50]
        print(f"[{i}/{len(grants)}] {oid} — {title}")

        try:
            verdict = classify_deadline(
                llm_service,
                deadline_date=grant.get("deadline_date"),
                deadline_description=grant.get("deadline_description"),
                reference=reference,
            )
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            continue

        if verdict.used_llm:
            llm_calls += 1
            if args.delay > 0:
                time.sleep(args.delay)

        if verdict.deadline_passed:
            passed_rows.append(
                {
                    **grant,
                    "status": "closed",
                    "effective_deadline": verdict.effective_deadline or "",
                    "confidence": verdict.confidence,
                    "reasoning": verdict.reasoning,
                    "used_llm": verdict.used_llm,
                }
            )
            print(f"  -> PASSED ({verdict.confidence}) → will mark closed in DB")
        else:
            print("  -> open / not passed")

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(passed_rows)

    if passed_rows and not args.no_update_db:
        conn = get_db_connection()
        try:
            ids = [str(r["opportunity_id"]) for r in passed_rows]
            n = mark_grants_status_closed(conn, ids)
            print(f"\nDatabase: set status='closed' on {n} grant row(s).")
        finally:
            conn.close()
    elif passed_rows and args.no_update_db:
        print("\nDatabase: skipped (--no-update-db).")

    print(
        f"\nWrote {len(passed_rows)} passed-deadline grant(s) to {args.output!r} "
        f"({llm_calls} Groq call(s))."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
