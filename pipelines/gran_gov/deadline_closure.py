"""
Helpers for the manual past-deadline workflow (``scripts.check_past_deadlines``).

Ingestion keeps a grant ``closed`` once set: ``upsert_grant_current`` does not
overwrite ``status`` when the existing row is already ``closed``.
"""
from __future__ import annotations

import sqlite3
from typing import Any

from db.db_util import row_get


def _placeholders(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def fetch_grants_for_deadline_check(
    conn: Any,
    *,
    limit: int | None = None,
    status_equals: str | None = None,
    statuses_in: tuple[str, ...] | None = None,
) -> list[dict]:
    """
    Rows with a deadline date or description. Optional filters:
    ``status_equals`` (single value) or ``statuses_in`` (tuple); if both are set,
    both AND constraints apply.
    """
    ph = _placeholders(conn)
    clauses = [
        "("
        "(deadline_description IS NOT NULL AND TRIM(deadline_description) != '') "
        f"OR (deadline_date IS NOT NULL AND TRIM(deadline_date) != '')"
        ")"
    ]
    params: list[object] = []

    if status_equals:
        clauses.append(f"status = {ph}")
        params.append(status_equals)

    if statuses_in:
        in_ph = ", ".join([ph] * len(statuses_in))
        clauses.append(f"status IN ({in_ph})")
        params.extend(statuses_in)

    where = " AND ".join(clauses)
    sql = f"""
        SELECT
            opportunity_id,
            number,
            title,
            agency,
            status,
            deadline_date,
            deadline_description,
            grant_gov_url
        FROM grants
        WHERE {where}
        ORDER BY opportunity_id
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    cur = conn.cursor()
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()

    out: list[dict] = []
    for row in rows:
        out.append(
            {
                "opportunity_id": row_get(row, "opportunity_id", 0),
                "number": row_get(row, "number", 1),
                "title": row_get(row, "title", 2),
                "agency": row_get(row, "agency", 3),
                "status": row_get(row, "status", 4),
                "deadline_date": row_get(row, "deadline_date", 5),
                "deadline_description": row_get(row, "deadline_description", 6),
                "grant_gov_url": row_get(row, "grant_gov_url", 7),
            }
        )
    return out


def mark_grants_status_closed(conn: Any, opportunity_ids: list[str]) -> int:
    """Set status to 'closed' for the given opportunity_ids. Returns rows affected."""
    if not opportunity_ids:
        return 0
    ph = _placeholders(conn)
    sql = (
        f"UPDATE grants SET status = {ph}, updated_at = CURRENT_TIMESTAMP "
        f"WHERE opportunity_id = {ph}"
    )
    cur = conn.cursor()
    total = 0
    for oid in opportunity_ids:
        cur.execute(sql, ("closed", oid))
        rc = cur.rowcount
        if rc is not None and rc > 0:
            total += int(rc)
    conn.commit()
    return total
