"""
Routes for the mission control dashboard
"""
from flask import Blueprint, render_template, request
from db.db_util import get_db_connection

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.route("/dashboard")
def dashboard():
    conn = get_db_connection(test_mode=True)

    runs = conn.execute("""
        SELECT *
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 20
    """).fetchall()

    return render_template("mission_control.html", runs=runs)


@dashboard_bp.route("/dashboard/run/<int:run_id>")
def view_run(run_id):
    conn = get_db_connection(test_mode=True)

    run = conn.execute("""
        SELECT *
        FROM pipeline_runs
        WHERE id = ?
    """, (run_id,)).fetchone()

    logs = conn.execute("""
        SELECT *
        FROM pipeline_logs
        WHERE job_id = ?
        ORDER BY created_at ASC
    """, (run_id,)).fetchall()

    return render_template(
        "run_detail.html",
        run=run,
        logs=logs
    )



@dashboard_bp.route("/grants")
def grants_page():
    return render_template("grants.html")

@dashboard_bp.route("/grants/<opportunity_id>")
def grant_detail_page(opportunity_id):
    return render_template("grant_detail.html", opportunity_id=opportunity_id)

@dashboard_bp.route("/alerts")
def alerts_page():
    return render_template("alerts.html")
