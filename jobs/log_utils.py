"""
This module contains the functions to log the pipeline runs
"""
from datetime import datetime

from db.db_util import row_get


def create_pipeline_run(conn, pipeline_name, run_type):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO pipeline_runs (pipeline_name, run_type, status, started_at)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (pipeline_name, run_type, "running", datetime.now()))
    job_id = row_get(cursor.fetchone(), "id", 0)
    conn.commit()
    cursor.close()
    log(conn, job_id, "Pipeline run started", "INFO")
    return job_id


def update_pipeline_run(conn, job_id: int, **kwargs):
    cursor = conn.cursor()

    fields = []
    values = []

    for key, value in kwargs.items():
        fields.append(f"{key} = %s")
        values.append(value)

    values.append(job_id)

    query = f"""
        UPDATE pipeline_runs
        SET {', '.join(fields)}
        WHERE id = %s
    """
    cursor.execute(query, values)
    conn.commit()
    cursor.close()



def log(conn, job_id: int, message, level="INFO"):
    conn.execute("""
        INSERT INTO pipeline_logs (job_id, log_level, message, created_at)
        VALUES (%s, %s, %s, %s)
    """, (job_id, level, message, datetime.now()))
    conn.commit()