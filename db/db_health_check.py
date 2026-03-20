from db.db_util import get_db_connection

def db_health_check():
    """
    Check the health of the database
    """
    conn = get_db_connection(test_mode=True)
    cursor = conn.cursor()
    # Print table names
    print("Table names:")
    for table in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';"):
        print(table[0])
    # For each table print the number of rows
    print("Number of rows:")
    for table in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';"):
        print(f"{table[0]}: {cursor.execute(f'SELECT COUNT(*) FROM {table[0]};').fetchone()[0]}")
    # For each table print the columns
    print("Columns:")
    for table in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';"):
        print(f"{table[0]}: {cursor.execute(f'PRAGMA table_info({table[0]});').fetchall()}")

    conn.close()
    return True

if __name__ == "__main__":
    db_health_check()