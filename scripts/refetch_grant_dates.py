"""
Refetch Grants.gov opportunity payloads and repopulate date columns on ``grants``.

Use after a bad ``normalize_grant_dates`` run nulled ``posted_date`` because the
parser did not understand API values like ``Nov 13, 2023 12:00:00 AM EST``.

Usage (from repo root, venv active):

  python -m scripts.refetch_grant_dates
  python -m scripts.refetch_grant_dates --limit 100 --delay 0.5

Or hit the HTTP endpoint (app running):

  GET /api/db_migration/refetch_grant_dates?limit=100&delay=0.25
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time

from db.db_util import get_db_connection, row_get
from pipelines.gran_gov.ingestion_utils import (
    extract_grant_dates_from_api_data,
    fetch_opportunity,
)


def _placeholders(conn) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def main() -> int:
    parser = argparse.ArgumentParser(description="Refetch grant dates from Grants.gov API.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--delay", type=float, default=0.25)
    args = parser.parse_args()

    conn = get_db_connection()
    ph = _placeholders(conn)
    cur = conn.cursor()

    sql = "SELECT opportunity_id FROM grants ORDER BY opportunity_id"
    if args.limit is not None:
        sql += f" LIMIT {int(args.limit)}"
    cur.execute(sql)
    oids = [str(row_get(r, "opportunity_id", 0)) for r in cur.fetchall()]

    print(f"Refetching dates for {len(oids)} grant(s)...")
    ok = errors = posted_set = 0

    try:
        for i, oid in enumerate(oids, start=1):
            try:
                raw = fetch_opportunity(int(oid))
                dates = extract_grant_dates_from_api_data(raw)
                cur.execute(
                    f"""
                    UPDATE grants
                    SET posted_date = {ph},
                        close_date = {ph},
                        deadline_date = {ph},
                        last_updated_date = {ph},
                        updated_at = CURRENT_TIMESTAMP
                    WHERE opportunity_id = {ph}
                    """,
                    (
                        dates["posted_date"],
                        dates["close_date"],
                        dates["deadline_date"],
                        dates["last_updated_date"],
                        oid,
                    ),
                )
                ok += 1
                if dates["posted_date"]:
                    posted_set += 1
                print(f"[{i}/{len(oids)}] {oid} posted_date={dates['posted_date']!r}")
            except Exception as e:
                errors += 1
                print(f"[{i}/{len(oids)}] {oid} ERROR: {e}", file=sys.stderr)

            if args.delay > 0 and i < len(oids):
                time.sleep(args.delay)

        conn.commit()
    finally:
        conn.close()

    print(
        f"\nDone: api_ok={ok}, api_errors={errors}, "
        f"posted_date_set={posted_set}, still_null={ok - posted_set}"
    )
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
