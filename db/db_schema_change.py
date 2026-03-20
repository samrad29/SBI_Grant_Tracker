import sqlite3
from db.db_util import get_db_connection
from datetime import datetime


def add_last_seen_at_column(conn: sqlite3.Connection) -> None:
    """
    Add the last_seen_at column to the grants table
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(f"ALTER TABLE grants ADD COLUMN last_seen_at TEXT NOT NULL DEFAULT '{now}'")

if __name__ == "__main__":
    conn = get_db_connection(test_mode=False)
    add_last_seen_at_column(conn)
    conn.commit()
    conn.close()