from pipelines.gran_gov.main import grants_main
from db.db_util import get_db_connection
from jobs.init_tables import create_pipeline_tables
from jobs.log_utils import create_pipeline_run, mark_runs_completed
from jobs.log_utils import update_pipeline_run
from jobs.log_utils import log
from datetime import datetime
from config.runtime import get_runtime_settings

def run_daily_jobs() -> None:
    print("Starting daily jobs...")
    print("Connecting to database...")
    daily_start_time = datetime.now()
    daily_start_time_str = daily_start_time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Daily jobs started at {daily_start_time_str}")
    settings = get_runtime_settings()
    conn = get_db_connection(test_mode=settings.test_mode)
    print("Creating tables...")
    create_pipeline_tables(conn)
    print("--------------------------------")
    print("Creating pipeline run for grants daily job...")
    job_id = create_pipeline_run(conn, "grants", "daily")
    print(f"Grants daily job pipeline run created with ID: {job_id}")
    print("Starting grants daily job...")
    grants_daily_start_time = datetime.now()
    grants_main(conn, job_id)
    grants_daily_end_time = datetime.now()
    grants_daily_end_time_str = grants_daily_end_time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Grants daily job completed at {grants_daily_end_time_str}")
    print(f"Grants daily job took {grants_daily_end_time - grants_daily_start_time}")
    log(conn, job_id, f"Grants daily job took {grants_daily_end_time - grants_daily_start_time}", "INFO")
    print("Updating pipeline run for grants daily job...")
    update_pipeline_run(conn, job_id, status="completed", finished_at=datetime.now())
    mark_runs_completed(conn)
    print(f"Grants daily job pipeline run completed with ID: {job_id}")
    print(f"Daily jobs took {datetime.now() - daily_start_time}")
    print("--------------------------------")
    print("Closing database connection...")
    conn.close()
    print("Daily jobs completed successfully.")


if __name__ == "__main__":
    run_daily_jobs()
