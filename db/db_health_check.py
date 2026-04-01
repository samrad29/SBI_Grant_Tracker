from db.db_util import get_db_connection, is_test_mode, scalar_from_row, row_get

def db_health_check():
    """
    Check the health of the database
    """
    conn = get_db_connection(test_mode=is_test_mode())
    cursor = conn.cursor()
    # Print table names (public schema)
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    tables = [row_get(row, "table_name", 0) for row in cursor.fetchall()]
    print("Table names:")
    for table in tables:
        print(table)
    # For each table print the number of rows
    print("Number of rows:")
    for table in tables:
        count = scalar_from_row(
            conn.execute(f"SELECT COUNT(*) AS cnt FROM {table};").fetchone()
        )
        print(f"{table}: {count}")
    # For each table print the columns
    print("Columns:")
    for table in tables:
        cols = conn.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table,)).fetchall()
        print(f"{table}: {cols}")

    conn.close()
    return True

if __name__ == "__main__":
    db_health_check()