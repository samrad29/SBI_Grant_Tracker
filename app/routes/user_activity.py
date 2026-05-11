"""
API Routes used for user activity
"""
from flask import Blueprint, jsonify
from flask import request, session
from db.db_util import get_db_connection, is_test_mode, row_get
from app.routes.api import _row_to_dict, _rows_to_dicts

user_activity_bp = Blueprint("user_activity", __name__)


def _parse_bool_arg(name: str) -> bool:
    raw = (request.args.get(name) or "").strip().lower()
    return raw in ("1", "true", "yes", "t", "on")


@user_activity_bp.route("/api/user_activity/update_user_grant_status")
def mark_grant():
    """
    Log user activity for a grant by the current user
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
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
    except Exception as e:
        return jsonify({"message": "Error marking grant: " + str(e)}), 500
    finally:
        conn.close()

@user_activity_bp.route("/api/user_activity/bookmark_grant")
def bookmark_grant():
    """
    Bookmark a grant for the current user
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
        opportunity_id = request.args.get("opportunity_id")
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO user_grant_activity (user_id, opportunity_id, status, is_bookmarked)
            VALUES (%s, %s, 'saved', TRUE)
            ON CONFLICT (user_id, opportunity_id) DO UPDATE SET is_bookmarked = TRUE, unbookmarked = FALSE
            """,
            (user_id, opportunity_id),
        )
        conn.commit()
        return jsonify({"message": "Grant bookmarked successfully"})
    except Exception as e:
        return jsonify({"message": "Error bookmarking grant: " + str(e)}), 500
    finally:
        conn.close()


@user_activity_bp.route("/api/user_activity/unbookmark_grant")
def unbookmark_grant():
    """
    Unbookmark a grant for the current user
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
        opportunity_id = request.args.get("opportunity_id")
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE user_grant_activity SET is_bookmarked = FALSE, unbookmarked = TRUE, created_at = CURRENT_TIMESTAMP WHERE user_id = %s AND opportunity_id = %s",
            (user_id, opportunity_id),
        )
        conn.commit()
        return jsonify({"message": "Grant unbookmarked successfully"})
    except Exception as e:
        return jsonify({"message": "Error unbookmarking grant: " + str(e)}), 500
    finally:
        conn.close()


@user_activity_bp.route("/api/user_activity/get_bookmarked_grants")
def get_bookmarked_grants():
    """
    Get all bookmarked grants for the current user
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
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
    except Exception as e:
        return jsonify({"message": "Error getting bookmarked grants: " + str(e)}), 500
    finally:
        conn.close()


@user_activity_bp.route("/api/user_activity/get_unbookmarked_grants")
def get_unbookmarked_grants():
    """
    Get all unbookmarked grants for the current user
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM user_grant_activity
            WHERE user_id = %s AND unbookmarked = TRUE
            ORDER BY created_at DESC
            limit 50
            """,
            (user_id,),
        )
        unbookmarked_grants = _rows_to_dicts(cursor)
        return jsonify(unbookmarked_grants)
    except Exception as e:
        return jsonify({"message": "Error getting unbookmarked grants: " + str(e)}), 500
    finally:
        conn.close()

@user_activity_bp.route("/api/user_activity/get_user_alerts")
def get_user_alerts():
    """
    Get all alerts for the current user
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
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
    except Exception as e:
        return jsonify({"message": "Error getting user alerts: " + str(e)}), 500
    finally:
        conn.close()


@user_activity_bp.route("/api/user_activity/get_checklist_items")
def get_checklist_items():
    """
    Get all checklist items for the current user
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
        opportunity_id = request.args.get("opportunity_id")
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM grant_checklist_items WHERE user_id = %s AND opportunity_id = %s ORDER BY item_id",
            (user_id, opportunity_id),
        )
        checklist_items = _rows_to_dicts(cursor)
        return jsonify(checklist_items)
    except Exception as e:
        return jsonify({"message": "Error getting checklist items: " + str(e)}), 500
    finally:
        conn.close()


@user_activity_bp.route("/api/user_activity/update_checklist_item")
def update_checklist_item():
    """
    Update a checklist item for the current user
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
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
    except Exception as e:
        return jsonify({"message": "Error updating checklist item: " + str(e)}), 500
    finally:
        conn.close()


@user_activity_bp.route("/api/user_activity/add_checklist_item")
def add_checklist_item():
    """
    Add a checklist item for the current user
    """
    try: 
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
        opportunity_id = request.args.get("opportunity_id")
        item_name = request.args.get("item_name")
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO grant_checklist_items (user_id, opportunity_id, item_name) VALUES (%s, %s, %s)",
            (user_id, opportunity_id, item_name),
        )
        conn.commit()
        return jsonify({"message": "Checklist item added successfully"})
    except Exception as e:
        return jsonify({"message": "Error adding checklist item: " + str(e)}), 500
    finally: 
        conn.close()


@user_activity_bp.route("/api/user_activity/get_user_info")
def get_user_info():
    """
    Get the current user's information
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT users.user_id, users.user_email, users.role, users.user_name, groups.group_name
            FROM users
            LEFT JOIN groups ON users.group_id = groups.group_id
            WHERE users.user_id = %s
            """,
            (user_id,),
        )
        user = _row_to_dict(cursor)
        if user is None:
            return jsonify({}), 404
        return jsonify(user)
    except Exception as e:
        return jsonify({"message": "Error getting user info: " + str(e)}), 500
    finally:
        conn.close()


@user_activity_bp.route("/api/user_activity/get_user_projects")
def get_user_projects():
    """
    Get the current user's projects
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
        cursor = conn.cursor()
        cursor.execute("SELECT project_id, project_name, project_description, project_status, funding_required, funding_secured FROM projects WHERE project_owner_user_id = %s", (user_id,))
        projects = _rows_to_dicts(cursor)
        return jsonify(projects)
    except Exception as e:
        return jsonify({"message": "Error getting user projects: " + str(e)}), 500
    finally:
        conn.close()

@user_activity_bp.route("/api/user_activity/add_project_task")
def add_project_task():
    """
    Add a task to a project
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        user_id = session["user_id"]
        project_id = request.args.get("project_id")
        task_name = request.args.get("task_name")
        task_description = request.args.get("task_description")
        cursor = conn.cursor()
        cursor.execute("INSERT INTO project_tasks (task_name, task_description, task_owner_user_id, task_project_id) VALUES (%s, %s, %s, %s)", (task_name, task_description, user_id, project_id))
        conn.commit()
        return jsonify({"message": "Task added successfully"})
    except Exception as e:
        return jsonify({"message": "Error adding task: " + str(e)}), 500

@user_activity_bp.route("/api/user_activity/get_project_tasks")
def get_project_tasks():
    """
    Get the tasks for a project
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        project_id = request.args.get("project_id")
        cursor = conn.cursor()
        cursor.execute("SELECT task_id, task_name, task_description, task_owner_user_id, task_project_id FROM project_tasks WHERE task_project_id = %s", (project_id,))
        tasks = _rows_to_dicts(cursor)
        return jsonify(tasks)
    except Exception as e:
        return jsonify({"message": "Error getting tasks: " + str(e)}), 500
    finally:
        conn.close()

@user_activity_bp.route("/api/user_activity/update_project_task")
def update_project_task():
    """
    Update a task for a project
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        project_id = request.args.get("project_id")
        task_id = request.args.get("task_id")
        task_name = request.args.get("task_name")
        task_description = request.args.get("task_description")
        task_status = request.args.get("task_status")
        cursor = conn.cursor()
        cursor.execute("UPDATE project_tasks SET task_name = %s, task_description = %s, task_status = %s WHERE task_id = %s", (task_name, task_description, task_status, task_id))
        conn.commit()
        return jsonify({"message": "Task updated successfully"})
    except Exception as e:
        return jsonify({"message": "Error updating task: " + str(e)}), 500


@user_activity_bp.route("/api/user_activity/delete_project_task")
def delete_project_task():
    """
    Delete a task for a project
    """
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        project_id = request.args.get("project_id")
        task_id = request.args.get("task_id")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM project_tasks WHERE task_id = %s", (task_id,))
        conn.commit()
        return jsonify({"message": "Task deleted successfully"})
    except Exception as e:
        return jsonify({"message": "Error deleting task: " + str(e)}), 500
    finally:
        conn.close()

@user_activity_bp.route("/api/reset_oei_data")
def reset_oei_data():
    """
    Remove all grants rows sourced from the WI PSC OEI pipeline, and any rows in
    tables that FK to grants(opportunity_id) for those ids (Postgres may not CASCADE).
    """
    oei_source = "wi_psc_oei"
    dependent_tables = (
        "grant_checklist_items",
        "user_grant_activity",
        "grant_tags",
        "grant_alerts",
        "grant_snapshots",
        "tribal_eligibility",
    )
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        cursor = conn.cursor()
        cursor.execute(
            "SELECT opportunity_id FROM grants WHERE opportunity_source = %s",
            (oei_source,),
        )
        rows = cursor.fetchall()
        ids = [row_get(r, "opportunity_id", 0) for r in rows if row_get(r, "opportunity_id", 0)]

        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            params = tuple(ids)
            for table in dependent_tables:
                cursor.execute(
                    f"DELETE FROM {table} WHERE opportunity_id IN ({placeholders})",
                    params,
                )

        cursor.execute(
            "DELETE FROM grants WHERE opportunity_source = %s",
            (oei_source,),
        )
        deleted = cursor.rowcount
        conn.commit()
        return jsonify(
            {
                "message": "WI PSC OEI grants reset successfully",
                "deleted_grant_rows": deleted,
                "cleared_opportunity_ids": len(ids),
            }
        )
    except Exception as e:
        return jsonify({"message": "Error resetting OEI grant data: " + str(e)}), 500
    finally:
        conn.close()
