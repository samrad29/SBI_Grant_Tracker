"""
Enable the pgvector extension on Postgres (one-time setup).

Uses DATABASE_URL from .env. Safe to re-run.

Usage (from repo root, venv active):

  python -m scripts.enable_pgvector

Then create grants_ai and run extraction:

  python -m scripts.extract_embed --save-db --limit 250
"""

from __future__ import annotations

import os
import sys

from db.db_util import get_db_connection, row_get
from pipelines.gran_gov.init_tables import ensure_pgvector_extension


def main() -> int:
    if not (os.getenv("DATABASE_URL") or "").strip():
        print("DATABASE_URL is required for pgvector setup.", file=sys.stderr)
        return 1

    conn = get_db_connection()
    try:
        print("Enabling pgvector (CREATE EXTENSION IF NOT EXISTS vector)...")
        ensure_pgvector_extension(conn)
        conn.commit()

        cur = conn.cursor()
        cur.execute(
            "SELECT extname, extversion FROM pg_extension WHERE extname = 'vector'"
        )
        row = cur.fetchone()
        if not row:
            print("CREATE EXTENSION ran but vector was not found.", file=sys.stderr)
            return 1

        name = row_get(row, "extname", 0)
        version = row_get(row, "extversion", 1)
        print(f"pgvector is enabled ({name} {version}).")
        return 0
    except Exception as e:
        print(f"Failed to enable pgvector: {e}", file=sys.stderr)
        msg = str(e).lower()
        if "is not available" in msg or "could not open extension" in msg:
            print(
                "\nOn Render: use Postgres 13+ or email support@render.com "
                "to enable pgvector on this database.",
                file=sys.stderr,
            )
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
