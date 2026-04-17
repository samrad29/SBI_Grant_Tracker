"""
Routes for the mission control dashboard
"""
from flask import Blueprint, abort, render_template
from db.db_util import get_db_connection, is_test_mode, scalar_from_row

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.route("/dashboard")
def dashboard():
    conn = get_db_connection(test_mode=is_test_mode())

    runs = conn.execute("""
        SELECT *
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 20
    """).fetchall()

    return render_template("mission_control.html", runs=runs)


@dashboard_bp.route("/dashboard/run/<int:run_id>")
def view_run(run_id):
    conn = get_db_connection(test_mode=is_test_mode())

    run = conn.execute("""
        SELECT *
        FROM pipeline_runs
        WHERE id = %s
    """, (run_id,)).fetchone()
    if run is None:
        abort(404)

    logs = conn.execute("""
        SELECT *
        FROM pipeline_logs
        WHERE job_id = %s
        ORDER BY created_at ASC
    """, (run_id,)).fetchall()

    return render_template(
        "run_detail.html",
        run=run,
        logs=logs
    )


@dashboard_bp.route("/dashboard/grant_tags")
def grant_tags_page():
    conn = get_db_connection(test_mode=is_test_mode())
    grant_tags = conn.execute("""
        SELECT tag, COUNT(*) AS tag_count
        FROM grant_tags
        GROUP BY tag
        ORDER BY tag_count DESC
    """).fetchall()
    total_grants = scalar_from_row(
        conn.execute("""
        SELECT COUNT(DISTINCT opportunity_id) AS total_grants
        FROM grant_tags
    """).fetchone()
    )
    return render_template("grant_tags.html", grant_tags=grant_tags, total_grants=total_grants)

@dashboard_bp.route("/dashboard/grant_tags/<tag>")
def grant_tag_detail_page(tag):
    conn = get_db_connection(test_mode=is_test_mode())
    grant_tags = conn.execute("""
        SELECT *
        FROM grant_tags
        WHERE tag = %s
    """, (tag,)).fetchall()
    return render_template("grant_tag_details.html", grant_tags=grant_tags)

@dashboard_bp.route("/grants")
def grants_page():
    return render_template("grants.html")

@dashboard_bp.route("/grants/<opportunity_id>")
def grant_detail_page(opportunity_id):
    return render_template("grant_detail.html", opportunity_id=opportunity_id)

@dashboard_bp.route("/alerts")
def alerts_page():
    return render_template("alerts.html")


@dashboard_bp.route("/portal")
def client_portal_page():
    return render_template("client_portal.html")


@dashboard_bp.route("/api-docs")
def api_docs_page():
    return render_template("api_docs.html")
