"""
This module contains the functions to log the pipeline runs
"""
from datetime import datetime
import sqlite3

def create_pipeline_run(conn: sqlite3.Connection, pipeline_name, run_type):
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO pipeline_runs (pipeline_name, run_type, status, started_at)
        VALUES (?, ?, ?, ?)
    """, (pipeline_name, run_type, "running", datetime.now()))
    conn.commit()
    cursor.close()
    job_id = cursor.lastrowid
    log(conn, job_id, "Pipeline run started", "INFO")
    return job_id


def update_pipeline_run(conn: sqlite3.Connection, job_id: int, **kwargs):
    cursor = conn.cursor()

    fields = []
    values = []

    for key, value in kwargs.items():
        fields.append(f"{key} = ?")
        values.append(value)

    values.append(job_id)

    query = f"""
        UPDATE pipeline_runs
        SET {', '.join(fields)}
        WHERE id = ?
    """
    cursor.execute(query, values)
    conn.commit()
    cursor.close()



def log(conn: sqlite3.Connection, job_id: int, message, level="INFO"):
    conn.execute("""
        INSERT INTO pipeline_logs (job_id, log_level, message, created_at)
        VALUES (?, ?, ?, ?)
    """, (job_id, level, message, datetime.now()))
    conn.commit()