import sqlite3

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