import sqlite3

from flask import Blueprint, jsonify

from db.db_util import get_db_connection, is_test_mode, row_get
from pipelines.gran_gov.ingestion_utils import compute_grant_public_url, normalize_grant_date
from pipelines.gran_gov.relevancy import score_relevancy, save_relevancy_score
from pipelines.gran_gov.ingestion_utils import fetch_opportunity


try:
    from psycopg import sql
except ImportError:  # pragma: no cover
    sql = None


db_migration_bp = Blueprint("db_migration", __name__)


@db_migration_bp.route("/api/db_migration/add_unbookmarked_grants", methods=["POST", "GET"])
def add_unbookmarked_grants():
    """
    Idempotently add ``unbookmarked`` to ``user_grant_activity`` if missing.
    """
    conn = None
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        if isinstance(conn, sqlite3.Connection):
            cur = conn.cursor()
            try:
                cur.execute(
                    "ALTER TABLE user_grant_activity ADD COLUMN unbookmarked INTEGER NOT NULL DEFAULT 0"
                )
                added = True
            except sqlite3.OperationalError as e:
                if "duplicate column" in str(e).lower():
                    added = False
                else:
                    raise
            conn.commit()
            msg = (
                "Column unbookmarked added to user_grant_activity"
                if added
                else "Column unbookmarked already exists (no change)"
            )
            return jsonify({"message": msg, "added": added}), 200

        if sql is None:
            return jsonify({"message": "psycopg.sql required for Postgres migrations"}), 500
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE user_grant_activity ADD COLUMN IF NOT EXISTS "
                "unbookmarked BOOLEAN NOT NULL DEFAULT FALSE"
            )
        conn.commit()
        return jsonify(
            {
                "message": "Column unbookmarked ensured on user_grant_activity (Postgres IF NOT EXISTS)",
                "added": True,
            }
        ), 200
    except Exception as e:
        return jsonify({"message": "Error adding unbookmarked column: " + str(e)}), 500
    finally:
        if conn is not None:
            conn.close()


@db_migration_bp.route("/api/db_migration/reset_tables", methods=["POST", "GET"])
def reset_tables():
    """
    Drop all application tables. Destructive: use only in dev or with backups.
    """
    conn = None
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        if isinstance(conn, sqlite3.Connection):
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row_get(r, "name", 0) for r in cur.fetchall()]
            for name in tables:
                # cur.execute(f'DROP TABLE IF EXISTS "{name}"')
                print(name)
            conn.commit()
            return jsonify({"message": f"Tables reset successfully ({len(tables)} dropped)"}), 200

        if sql is None:
            return jsonify({"message": "psycopg.sql required for Postgres reset"}), 500
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            tables = [row_get(r, "table_name", 0) for r in cur.fetchall()]
            for name in tables:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(name))
                )
        conn.commit()
        return jsonify({"message": f"Tables reset successfully ({len(tables)} dropped)"}), 200
    except Exception as e:
        return jsonify({"message": "Error resetting tables: " + str(e)}), 500
    finally:
        if conn is not None:
            conn.close()


@db_migration_bp.route("/api/db_migration/normalize_grant_dates", methods=["POST", "GET"])
def normalize_grant_dates():
    """
    Backfill ``posted_date``, ``close_date``, and ``last_updated_date`` to ISO
    ``YYYY-MM-DD`` using ``normalize_grant_date`` (fixes lexical sort on spelled-out months).
    """
    conn = None
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        cur = conn.cursor()
        cur.execute(
            "SELECT opportunity_id, posted_date, close_date, last_updated_date FROM grants"
        )
        rows = cur.fetchall()
        ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"
        updated = 0
        for row in rows:
            oid = row_get(row, "opportunity_id", 0)
            posted = normalize_grant_date(row_get(row, "posted_date", 1))
            close = normalize_grant_date(row_get(row, "close_date", 2))
            last_up = normalize_grant_date(row_get(row, "last_updated_date", 3))
            cur.execute(
                f"""
                UPDATE grants
                SET posted_date = {ph}, close_date = {ph}, last_updated_date = {ph}
                WHERE opportunity_id = {ph}
                """,
                (posted, close, last_up, oid),
            )
            updated += 1
        conn.commit()
        return jsonify(
            {
                "message": "Grant dates normalized to YYYY-MM-DD where parseable",
                "rows_updated": updated,
                "rows_total": len(rows),
            }
        ), 200
    except Exception as e:
        return jsonify({"message": "Error normalizing grant dates: " + str(e)}), 500
    finally:
        if conn is not None:
            conn.close()


@db_migration_bp.route("/api/db_migration/update_grant_gov_url", methods=["POST", "GET"])
def update_grant_gov_url():
    """
    Backfill ``grants.grant_gov_url`` using ``compute_grant_public_url`` (same as
    ingestion): funding ``link_url`` when set, else NULL.
    """
    conn = None
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        cur = conn.cursor()
        cur.execute("SELECT opportunity_id, status, number, link_url FROM grants")
        rows = cur.fetchall()
        ph = "?" if isinstance(conn, sqlite3.Connection) else "%s"
        for row in rows:
            opportunity_id = row_get(row, "opportunity_id", 0)
            status = row_get(row, "status", 1)
            number = row_get(row, "number", 2)
            link_url = row_get(row, "link_url", 3)
            public_url = compute_grant_public_url(
                link_url, status, opportunity_id, number
            )
            cur.execute(
                f"UPDATE grants SET grant_gov_url = {ph} WHERE opportunity_id = {ph}",
                (public_url, opportunity_id),
            )
        conn.commit()
        return jsonify(
            {
                "message": "grant_gov_url refreshed from link_url (null when no public URL)",
                "rows_updated": len(rows),
            }
        ), 200
    except Exception as e:
        return jsonify({"message": "Error updating grant gov url: " + str(e)}), 500
    finally:
        if conn is not None:
            conn.close()


def add_relevancy_score():
    """
    Add relevancy score to tribal_eligibility table.
    """
    conn = None
    try:
        conn = get_db_connection(test_mode=is_test_mode())
        cur = conn.cursor()
        cur.execute(
                "ALTER TABLE tribal_eligibility ADD COLUMN IF NOT EXISTS "
                "relevancy_score INTEGER"
            )
        conn.commit()
        cur.execute("SELECT * FROM grants")
        rows = cur.fetchall()
        for row in rows:
            opportunity_id = row_get(row, "opportunity_id", 0)
            relevancy_score = score_relevancy(row)
            save_relevancy_score(conn, opportunity_id, relevancy_score)
        conn.commit()
        return jsonify({"message": "Relevancy score added to tribal_eligibility table"}), 200
    except Exception as e:
        return jsonify({"message": "Error adding relevancy score: " + str(e)}), 500
    finally:
        if conn is not None:
            conn.close()