"""
API Routes used by the frontend/UI
"""
from flask import Blueprint, jsonify
from flask import request
from db.db_util import get_db_connection, is_test_mode

api_bp = Blueprint("api", __name__)

def _rows_to_dicts(cursor):
    """
    Convert DB rows into JSON-friendly dicts.
    psycopg dict_row already returns dict-like rows; do not zip(cols, row) on those
    (iterating a dict yields keys, which would map every column to its own name).
    """
    rows = cursor.fetchall()
    if not rows:
        return []
    if isinstance(rows[0], dict):
        return [dict(r) for r in rows]
    cols = [desc[0] for desc in (cursor.description or [])]
    return [dict(zip(cols, row)) for row in rows]


def _row_to_dict(cursor):
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    cols = [desc[0] for desc in (cursor.description or [])]
    return dict(zip(cols, row))


def _as_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _aggregate_tagged_opportunities(rows: list[dict]) -> list[dict]:
    """
    Collapse one SQL row per (opportunity, tag) into one dict per opportunity.

    Each item: opportunity_id, title, agency, status, total_score, tag_scores
    where tag_scores is [{ "tag", "tag_score" }, ...] sorted by tag_score desc.
    """
    by_oid: dict[str, dict] = {}
    for r in rows:
        oid = r.get("opportunity_id")
        oid_s = str(oid) if oid is not None else ""
        if not oid_s:
            continue
        if oid_s not in by_oid:
            by_oid[oid_s] = {
                "opportunity_id": oid,
                "title": r.get("title"),
                "agency": r.get("agency"),
                "status": r.get("status"),
                "total_score": _as_float(r.get("total_score")),
                "_tag_best": {},
            }
        g = by_oid[oid_s]
        if r.get("total_score") is not None:
            g["total_score"] = _as_float(r.get("total_score"))
        tag = r.get("tag")
        if tag is None:
            continue
        tag_s = str(tag)
        ts = _as_float(r.get("tag_score"))
        prev = g["_tag_best"].get(tag_s)
        if prev is None or (ts is not None and (prev is None or ts > prev)):
            g["_tag_best"][tag_s] = ts
    out: list[dict] = []
    for g in by_oid.values():
        tag_best = g.pop("_tag_best")
        tag_scores = [{"tag": t, "tag_score": s} for t, s in tag_best.items()]
        tag_scores.sort(
            key=lambda x: (
                x["tag_score"] if x["tag_score"] is not None else 0.0,
                x["tag"],
            ),
            reverse=True,
        )
        g["tag_scores"] = tag_scores
        out.append(g)
    out.sort(key=lambda x: x.get("total_score") or 0.0, reverse=True)
    return out


@api_bp.route("/api/opportunities")
def get_opportunities():
    """
    List opportunities from the grants table.

    Without ``tags``: up to 50 rows with opportunity_id, title, agency, status.

    With ``tags`` (comma-separated): one object per opportunity that matches any
    listed tag (case-insensitive), ordered by ``total_score`` descending. Each object includes
    ``total_score`` (sum of matching tag scores for that opportunity) and
    ``tag_scores``: all matching tags with their scores for that grant.
    """
    conn = get_db_connection(test_mode=is_test_mode())
    tags_raw = request.args.get("tags", "")
    tag_list = [t.strip() for t in tags_raw.split(",") if t.strip()]
    if tag_list:
        tag_list_lower = [t.lower() for t in tag_list]
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                grants.opportunity_id,
                grants.title,
                grants.agency,
                grants.status,
                grant_tags.tag,
                grant_tags.tag_score,
                grant_tags.total_score
            FROM grants
            INNER JOIN (
                SELECT
                    opportunity_id,
                    tag,
                    tag_score,
                    sum(tag_score) OVER (PARTITION BY opportunity_id) AS total_score
                FROM grant_tags
                WHERE LOWER(tag) = ANY(%s)
            ) AS grant_tags
                ON grants.opportunity_id = grant_tags.opportunity_id
            WHERE grant_tags.total_score > 0
            ORDER BY grant_tags.total_score DESC
            """,
            (tag_list_lower,),
        )
        raw = _rows_to_dicts(cursor)
        opportunities = _aggregate_tagged_opportunities(raw)
    else:
        cursor = conn.cursor()
        cursor.execute("SELECT opportunity_id, title, agency, status FROM grants limit 50")
        opportunities = _rows_to_dicts(cursor)
    return jsonify(opportunities)

@api_bp.route("/api/opportunities/<opportunity_id>")
def get_opportunity_by_id(opportunity_id):
    """
    Get an opportunity by id from the database (grants table)
    Returns:
        opportunity
    """
    conn = get_db_connection(test_mode=is_test_mode())
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM grants WHERE opportunity_id = %s", (opportunity_id,))
    opportunity = _row_to_dict(cursor)
    if opportunity is None:
        return jsonify({}), 404
    return jsonify(opportunity)

@api_bp.route("/api/alerts")
def get_alerts():
    """
    Get 50 most recent alerts from the database (grant_alerts table)
    Returns:
        list of alerts
    """
    conn = get_db_connection(test_mode=is_test_mode())
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM grant_alerts order by detected_at desc limit 50")
    alerts = _rows_to_dicts(cursor)
    return jsonify(alerts)