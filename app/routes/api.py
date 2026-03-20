"""
API Routes used by the frontend/UI
"""
from flask import Blueprint, jsonify
from flask import request
from db.db_util import get_db_connection
import os
TEST_MODE = os.getenv("TEST_MODE", "False")
if TEST_MODE == "True":
    TEST_MODE = True
else:
    TEST_MODE = False

api_bp = Blueprint("api", __name__)

def _rows_to_dicts(cursor):
    """
    Convert DB tuples from `cursor.fetchall()` / `cursor.fetchone()` into JSON-friendly dicts.
    """
    cols = [desc[0] for desc in (cursor.description or [])]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def _row_to_dict(cursor):
    cols = [desc[0] for desc in (cursor.description or [])]
    row = cursor.fetchone()
    if row is None:
        return None
    return dict(zip(cols, row))

@api_bp.route("/api/opportunities")
def get_opportunities():
    """
    Get opportunities from the database (grants table)
    Returns:
        list of opportunities
    """
    conn = get_db_connection(test_mode=TEST_MODE)
    cursor = conn.cursor()
    cursor.execute("SELECT opportunity_id, title, agency, status FROM grants limit 50")
    opportunities = _rows_to_dicts(cursor)
    return jsonify(opportunities)

@api_bp.route("/api/opportunities/<opportunity_id>")
def get_opportunity_by_id(opportunity_id):
    """
    Get an opportunity from the database (grants table)
    Returns:
        opportunity
    """
    conn = get_db_connection(test_mode=TEST_MODE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM grants WHERE opportunity_id = ?", (opportunity_id,))
    opportunity = _row_to_dict(cursor)
    if opportunity is None:
        return jsonify({}), 404
    return jsonify(opportunity)

@api_bp.route("/api/alerts")
def get_alerts():
    """
    Get alerts from the database (grant_alerts table)
    Returns:
        list of alerts
    """
    conn = get_db_connection(test_mode=TEST_MODE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM grant_alerts limit 50")
    alerts = _rows_to_dicts(cursor)
    return jsonify(alerts)