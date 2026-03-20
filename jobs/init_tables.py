"""
This module contains the tables for the pipeline runs
"""
import sqlite3
def create_pipeline_tables(conn: sqlite3.Connection):
    """
    Create the pipeline runs table
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT,
            run_type TEXT,
            status TEXT,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            records_processed INTEGER,
            new_records INTEGER,
            updated_records INTEGER,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS pipeline_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            log_level TEXT,
            message TEXT,
            created_at TIMESTAMP
        );
    """)
    conn.commit()
