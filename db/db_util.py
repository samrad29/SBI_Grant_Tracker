import sqlite3
import os

def is_test_mode() -> bool:
    """
    Resolve TEST_MODE from environment.
    Truthy: true, 1, yes, y (case-insensitive)
    """
    raw = (os.getenv("TEST_MODE") or "").strip().lower()
    return raw in {"true", "1", "yes", "y"}

def get_db_connection(test_mode: bool = False):
    """
    Get a connection to the database
    """
    if test_mode:
        print("Connecting to grants_test.db")
        conn = sqlite3.connect("grants_test.db")
        conn.row_factory = sqlite3.Row
        return conn
    else:
        print("Connecting to grants.db")
        conn = sqlite3.connect("grants.db")
        conn.row_factory = sqlite3.Row
        return conn