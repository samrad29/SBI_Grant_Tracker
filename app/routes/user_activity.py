"""
API Routes used for user activity
"""
from flask import Blueprint, jsonify
from flask import request
from db.db_util import get_db_connection, is_test_mode
from app.routes.api import _rows_to_dicts

user_activity_bp = Blueprint("user_activity", __name__)


def _parse_bool_arg(name: str) -> bool:
    raw = (request.args.get(name) or "").strip().lower()
    return raw in ("1", "true", "yes", "t", "on")


@user_activity_bp.route("/api/user_activity/update_user_grant_status")
def mark_grant():
    """
    Log user activity for a grant by the current user
    """
    conn = get_db_connection(test_mode=is_test_mode())
    user_id = request.args.get("user_id")
    opportunity_id = request.args.get("opportunity_id")
    status = request.args.get("status")
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO user_grant_activity (user_id, opportunity_id, status, is_bookmarked)
        VALUES (%s, %s, %s, FALSE)
        ON CONFLICT (user_id, opportunity_id) DO UPDATE
        SET status = EXCLUDED.status
        """,
        (user_id, opportunity_id, status),
    )
    conn.commit()
    return jsonify({"message": "Grant marked successfully"})


@user_activity_bp.route("/api/user_activity/bookmark_grant")
def bookmark_grant():
    """
    Bookmark a grant for the current user
    """
    conn = get_db_connection(test_mode=is_test_mode())
    user_id = request.args.get("user_id")
    opportunity_id = request.args.get("opportunity_id")
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO user_grant_activity (user_id, opportunity_id, status, is_bookmarked)
        VALUES (%s, %s, 'saved', TRUE)
        ON CONFLICT (user_id, opportunity_id) DO UPDATE SET is_bookmarked = TRUE
        """,
        (user_id, opportunity_id),
    )
    conn.commit()
    return jsonify({"message": "Grant bookmarked successfully"})


@user_activity_bp.route("/api/user_activity/unbookmark_grant")
def unbookmark_grant():
    """
    Unbookmark a grant for the current user
    """
    conn = get_db_connection(test_mode=is_test_mode())
    user_id = request.args.get("user_id")
    opportunity_id = request.args.get("opportunity_id")
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE user_grant_activity SET is_bookmarked = FALSE WHERE user_id = %s AND opportunity_id = %s",
        (user_id, opportunity_id),
    )
    conn.commit()
    return jsonify({"message": "Grant unbookmarked successfully"})


@user_activity_bp.route("/api/user_activity/get_bookmarked_grants")
def get_bookmarked_grants():
    """
    Get all bookmarked grants for the current user
    """
    conn = get_db_connection(test_mode=is_test_mode())
    user_id = request.args.get("user_id")
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM user_grant_activity
        WHERE user_id = %s AND is_bookmarked = TRUE
        ORDER BY created_at DESC
        """,
        (user_id,),
    )
    bookmarked_grants = _rows_to_dicts(cursor)
    return jsonify(bookmarked_grants)


@user_activity_bp.route("/api/user_activity/get_user_alerts")
def get_user_alerts():
    """
    Get all alerts for the current user
    """
    conn = get_db_connection(test_mode=is_test_mode())
    user_id = request.args.get("user_id")
    cursor = conn.cursor()
    cursor.execute(
        """
    SELECT
        user_grant_activity.user_id,
        user_grant_activity.opportunity_id,
        user_grant_activity.status,
        user_grant_activity.created_at,
        grant_alerts.alert_type,
        grant_alerts.field,
        grant_alerts.old_value,
        grant_alerts.new_value,
        grant_alerts.detected_at
    FROM user_grant_activity
    INNER JOIN grant_alerts
        ON user_grant_activity.opportunity_id = grant_alerts.opportunity_id
    WHERE user_grant_activity.user_id = %s
    and user_grant_activity.is_bookmarked = TRUE
    order by grant_alerts.detected_at desc
    limit 50
    """,
        (user_id,),
    )
    alerts = _rows_to_dicts(cursor)
    return jsonify(alerts)


@user_activity_bp.route("/api/user_activity/get_checklist_items")
def get_checklist_items():
    """
    Get all checklist items for the current user
    """
    conn = get_db_connection(test_mode=is_test_mode())
    user_id = request.args.get("user_id")
    opportunity_id = request.args.get("opportunity_id")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM grant_checklist_items WHERE user_id = %s AND opportunity_id = %s ORDER BY item_id",
        (user_id, opportunity_id),
    )
    checklist_items = _rows_to_dicts(cursor)
    return jsonify(checklist_items)


@user_activity_bp.route("/api/user_activity/update_checklist_item")
def update_checklist_item():
    """
    Update a checklist item for the current user
    """
    conn = get_db_connection(test_mode=is_test_mode())
    user_id = request.args.get("user_id")
    opportunity_id = request.args.get("opportunity_id")
    item_id = request.args.get("item_id")
    is_completed = _parse_bool_arg("is_completed")
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE grant_checklist_items SET is_completed = %s WHERE user_id = %s AND opportunity_id = %s AND item_id = %s",
        (is_completed, user_id, opportunity_id, item_id),
    )
    conn.commit()
    return jsonify({"message": "Checklist item updated successfully"})


@user_activity_bp.route("/api/user_activity/add_checklist_item")
def add_checklist_item():
    """
    Add a checklist item for the current user
    """
    conn = get_db_connection(test_mode=is_test_mode())
    user_id = request.args.get("user_id")
    opportunity_id = request.args.get("opportunity_id")
    item_name = request.args.get("item_name")
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO grant_checklist_items (user_id, opportunity_id, item_name) VALUES (%s, %s, %s)",
        (user_id, opportunity_id, item_name),
    )
    conn.commit()
    return jsonify({"message": "Checklist item added successfully"})

@user_activity_bp.route("/api/user_activity/add_is_bookmarked_column")
def add_is_bookmarked_column():
    """
    Add the is_bookmarked column to the user_grant_activity table
    """
    conn = get_db_connection(test_mode=is_test_mode())
    cursor = conn.cursor()
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS uniq_user_grant_activity_user_opportunity ON user_grant_activity(user_id, opportunity_id)")
    conn.commit()
    return jsonify({"message": "is_bookmarked column added successfully"})