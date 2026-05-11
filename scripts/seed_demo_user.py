"""
One-off: insert or update a row in ``users`` with a Werkzeug-hashed password
for testing ``POST /api/auth/login`` and the client portal.

Usage (from repo root):

  python -m scripts.seed_demo_user --email you@example.com --password 'secret'

Or pass the password via env (avoid shell history):

  set SEED_DEMO_USER_PASSWORD=secret
  python -m scripts.seed_demo_user --email you@example.com

Uses ``DATABASE_URL`` when set (Postgres); otherwise local SQLite and ``TEST_MODE``.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

from werkzeug.security import generate_password_hash

from db.db_util import get_db_connection, is_test_mode, row_get


def _placeholders(conn) -> tuple[str, str]:
    if isinstance(conn, sqlite3.Connection):
        return "?", "?"
    return "%s", "%s"


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed or update a demo user for auth.")
    parser.add_argument("--email", default=os.getenv("SEED_DEMO_USER_EMAIL", "demo@example.com"))
    parser.add_argument(
        "--password",
        default=os.getenv("SEED_DEMO_USER_PASSWORD", "test12345!"),
        help="Plain password to hash. Or set SEED_DEMO_USER_PASSWORD.",
    )
    parser.add_argument(
        "--user-id",
        default=os.getenv("SEED_DEMO_USER_ID", "demo-user"),
        help="Stable id stored in session (users.user_id).",
    )
    parser.add_argument("--user-name", default=os.getenv("SEED_DEMO_USER_NAME", "Demo User"))
    parser.add_argument("--role", default=os.getenv("SEED_DEMO_USER_ROLE", "client"))
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Use test DB flags (same as TEST_MODE=1 for sqlite fallback).",
    )
    args = parser.parse_args()
    if not args.password:
        print("Error: provide --password or set SEED_DEMO_USER_PASSWORD.", file=sys.stderr)
        return 1

    conn = get_db_connection()
    ph, _ = _placeholders(conn)
    pwd_hash = generate_password_hash(args.password)

    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT user_id FROM users WHERE user_email = {ph} LIMIT 1",
            (args.email.strip(),),
        )
        row = cur.fetchone()
        if row is not None:
            cur.execute(
                f"""
                UPDATE users
                SET user_password = {ph}, user_name = {ph}, role = {ph}
                WHERE user_email = {ph}
                """,
                (pwd_hash, args.user_name, args.role, args.email.strip()),
            )
            action = "updated"
        else:
            cur.execute(
                f"""
                INSERT INTO users (user_id, user_name, user_email, role, user_password)
                VALUES ({ph}, {ph}, {ph}, {ph}, {ph})
                """,
                (
                    args.user_id,
                    args.user_name,
                    args.email.strip(),
                    args.role,
                    pwd_hash,
                ),
            )
            action = "inserted"
        conn.commit()
        uid = args.user_id if action == "inserted" else row_get(row, "user_id", 0)
        print(f"OK: {action} user email={args.email!r} user_id={uid!r}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
