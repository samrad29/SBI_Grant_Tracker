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

@api_bp.route("/api/opportunities")
def get_opportunities():
    """
    Get opportunities from the database (grants table), sorted by tags provided in the request
    Returns:
        list of opportunities, sorted by tags provided in the request
    """
    conn = get_db_connection(test_mode=is_test_mode())
    tags_raw = request.args.get("tags", "")
    tag_list = [t.strip() for t in tags_raw.split(",") if t.strip()]
    if tag_list:
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
                WHERE tag = ANY(%s)
            ) AS grant_tags
                ON grants.opportunity_id = grant_tags.opportunity_id
            WHERE grant_tags.total_score > 0
            ORDER BY grant_tags.total_score DESC
            """,
            (tag_list,),
        )
        opportunities = _rows_to_dicts(cursor)
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