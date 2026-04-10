import os
import sqlite3

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None
    dict_row = None

def scalar_from_row(row):
    """
    First column of a single-column SELECT. Works for sqlite3.Row/tuple and
    psycopg dict_row (dict), where row[0] raises KeyError.
    """
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


def row_get(row, key: str, index: int = 0):
    """Named column access; falls back to index for plain tuples."""
    if row is None:
        return None
    if isinstance(row, dict):
        return row[key]
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        return row[index]


def ensure_postgres_id_defaults(conn, fixes: tuple[tuple[str, str], ...]) -> None:
    """
    If tables were created without BIGSERIAL/identity on id, INSERT omits id and Postgres
    stores NULL. CREATE TABLE IF NOT EXISTS does not fix existing tables. For each
    (table_name, sequence_name), attach a sequence + DEFAULT when id has no default.
    """
    if isinstance(conn, sqlite3.Connection):
        return
    with conn.cursor() as cur:
        for table, seq in fixes:
            cur.execute(
                """
                SELECT column_default
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                  AND column_name = 'id'
                """,
                (table,),
            )
            row = cur.fetchone()
            if row is None:
                continue
            default = row["column_default"] if isinstance(row, dict) else row[0]
            if default:
                continue
            cur.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq}")
            cur.execute(
                f"ALTER TABLE {table} ALTER COLUMN id "
                f"SET DEFAULT nextval('{seq}'::regclass)"
            )
            cur.execute(f"SELECT COALESCE(MAX(id), 0) AS m FROM {table}")
            m_row = cur.fetchone()
            m = m_row["m"] if isinstance(m_row, dict) else m_row[0]
            if m < 1:
                cur.execute("SELECT setval(%s, 1, false)", (seq,))
            else:
                cur.execute("SELECT setval(%s, %s, true)", (seq, m))
            cur.execute(f"ALTER SEQUENCE {seq} OWNED BY {table}.id")


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
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if database_url:
        if psycopg is None:
            raise RuntimeError("psycopg is required for DATABASE_URL connections.")
        print("Connecting to Postgres via DATABASE_URL")
        # dict_row: rows behave like sqlite3.Row for template key access (row['col']).
        return psycopg.connect(database_url, row_factory=dict_row)

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