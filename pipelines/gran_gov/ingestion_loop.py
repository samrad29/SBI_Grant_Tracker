import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional
from pipelines.gran_gov.ingestion_utils import fetch_opportunity, normalize_opportunity, update_grant_classification
from pipelines.gran_gov.change_detection import detect_changes
from pipelines.gran_gov.ai_utils import classify_grant, get_groq_client
from pipelines.gran_gov.quick_classification import quick_classification
from jobs.log_utils import log

def canonical_json(obj: Any) -> str:
    # Sort keys + compact separators for stable hashing
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _sql_text(value: Any) -> Optional[str]:
    """Bind-safe TEXT: bool/dict/list/objects from the API -> str/JSON; None stays None."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _sql_real(value: Any) -> Optional[float]:
    """Bind-safe REAL for award/estimate fields (numbers or numeric strings from API)."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip().replace(",", "").replace("$", "")
        if not s or s.lower() in ("n/a", "na", "none", "tbd"):
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _json_text(value: Any) -> Optional[str]:
    """
    Convert a value to the JSON-text format we persist in sqlite TEXT columns.

    - If `value` is already a JSON string (as produced by `normalize_opportunity`),
      store it as-is to avoid double-encoding.
    - If it's a list/dict, dump to JSON.
    - If it's None, return None.
    """

    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def get_previous_snapshot(conn: sqlite3.Connection, opportunity_id: str):
    row = conn.execute(
        """
        SELECT data_json, hash, fetched_at
        FROM grant_snapshots
        WHERE opportunity_id = ?
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (opportunity_id,),
    ).fetchone()
    if not row:
        return None
    return {"data_json": row[0], "hash": row[1], "fetched_at": row[2]}

def insert_snapshot(conn: sqlite3.Connection, opportunity_id: str, normalized: dict[str, Any]):
    # Important: sort/unique list fields BEFORE hashing/compare to reduce false positives.
    # (Do this in your normalization function ideally.)
    can = canonical_json(normalized)
    h = sha256_text(can)

    conn.execute(
        """
        INSERT OR IGNORE INTO grant_snapshots (opportunity_id, fetched_at, data_json, hash)
        VALUES (?, datetime('now'), ?, ?)
        """,
        (opportunity_id, can, h),
    )
    return h

def upsert_grant_current(conn: sqlite3.Connection, normalized: dict[str, Any]):
    # Assumes normalized has fields matching your schema.
    try:
        conn.execute(
            """
            INSERT INTO grants (
            opportunity_id, number, title, agency, agency_code, status,
            posted_date, close_date,
            deadline_date, deadline_description, last_updated_date,
            award_floor, award_ceiling, estimated_funding, cost_sharing,
            category, eligibility_description, alns, eligibilities, funding_categories, description, attachments
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(opportunity_id) DO UPDATE SET
            number=excluded.number,
            title=excluded.title,
            agency=excluded.agency,
            agency_code=excluded.agency_code,
            status=excluded.status,
            posted_date=excluded.posted_date,
            close_date=excluded.close_date,
            deadline_date=excluded.deadline_date,
            deadline_description=excluded.deadline_description,
            last_updated_date=excluded.last_updated_date,
            award_floor=excluded.award_floor,
            award_ceiling=excluded.award_ceiling,
            estimated_funding=excluded.estimated_funding,
            cost_sharing=excluded.cost_sharing,
            category=excluded.category,
            eligibility_description=excluded.eligibility_description,
            alns=excluded.alns,
            eligibilities=excluded.eligibilities,
            funding_categories=excluded.funding_categories,
            description=excluded.description,
            attachments=excluded.attachments,
            updated_at=datetime('now')
            """,
            (
                _sql_text(normalized.get("id")),
                _sql_text(normalized.get("number")),
                _sql_text(normalized.get("title")),
                _sql_text(normalized.get("agency")),
                _sql_text(normalized.get("agency_code")),
                _sql_text(normalized.get("status")),
                _sql_text(normalized.get("posted_date")),
                _sql_text(normalized.get("close_date")),
                _sql_text(normalized.get("deadline_date")),
                _sql_text(normalized.get("deadline_description")),
                _sql_text(normalized.get("last_updated_date")),
                _sql_real(normalized.get("award_floor")),
                _sql_real(normalized.get("award_ceiling")),
                _sql_real(normalized.get("estimated_funding")),
                _sql_text(normalized.get("cost_sharing")),
                _sql_text(normalized.get("category")),
                _sql_text(normalized.get("eligibility_description")),
                _json_text(normalized.get("alns", [])),
                _json_text(normalized.get("eligibilities", [])),
                _json_text(normalized.get("funding_categories", [])),
                _sql_text(normalized.get("description")),
                _json_text(normalized.get("attachments", [])),
            ),
        )
    except Exception as e:
        print(f"Error upserting grant current: {e}")
        raise

def daily_ingestion(conn: sqlite3.Connection, opportunity_ids: list[str], job_id: int):
    """
    takes in a list of opportunity ids and checks for any updates
    """
    conn.execute("BEGIN")
    try:
        groq_client = get_groq_client()
        ingestion_count = 0
        new_grants = 0
        new_relevant_grants = 0
        grants_with_alerts = 0
        for oid in opportunity_ids:
            try: 
                raw = fetch_opportunity(oid)                 # call fetchOpportunity
                normalized = normalize_opportunity(raw)    # map to your dict shape
                normalized["id"] = str(oid)          # ensure matches schema

                # Upsert current grant record
                upsert_grant_current(conn, normalized)
                log(conn, job_id, f"Upserted grant current for opportunity id: {oid}", "INFO")

                # Load previous snapshot (if any)
                prev = get_previous_snapshot(conn, str(oid))
                # Insert new snapshot and compute hash for dedupe
                new_hash = insert_snapshot(conn, str(oid), normalized)

                # If there is no previous snapshot, we can skip the diffing process. However, we will need to classify the grant as relevant or not.
                if prev is None:
                    # Classify the grant as relevant or not
                    new_grants += 1
                    quick_check_result = quick_classification(normalized)
                    if quick_check_result["is_relevant"]:
                        update_grant_classification(conn, str(oid), quick_check_result)
                        log(conn, job_id, f"Identified as new grant and classified as relevant for opportunity id: {oid}", "INFO")
                        new_relevant_grants += 1
                        continue
                    else: 
                        classification = classify_grant(groq_client, normalized)
                        update_grant_classification(conn, str(oid), classification)
                        log(conn, job_id, f"Identified as new grant and classified as not relevant for opportunity id: {oid}", "INFO")
                        continue

                old_hash = prev["hash"]
                if old_hash == new_hash:
                    continue

                old_data = json.loads(prev["data_json"])
                new_data = normalized

                alerts = detect_changes(old_data, new_data)

                for a in alerts:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO grant_alerts (
                        opportunity_id, alert_type, field,
                        old_value, new_value,
                        old_snapshot_hash, new_snapshot_hash,
                        fetched_at_old, fetched_at_new
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                        """,
                        (
                            str(oid),
                            a["type"],
                            a["field"],
                            json.dumps(a["old_value"], ensure_ascii=False) if isinstance(a["old_value"], (list, dict)) else str(a["old_value"]),
                            json.dumps(a["new_value"], ensure_ascii=False) if isinstance(a["new_value"], (list, dict)) else str(a["new_value"]),
                            old_hash,
                            new_hash,
                            prev["fetched_at"],
                        ),
                    )
                if alerts:
                    log(conn, job_id, f"Inserted {len(alerts)} alerts for opportunity id: {oid}", "INFO")
                    grants_with_alerts += 1
                else:
                    log(conn, job_id, f"No alerts changes for opportunity id: {oid}", "INFO")
            except Exception as e:
                log(conn, job_id, f"Error in daily ingestion for opportunity id: {oid}: {e}", "ERROR")
                continue
        log(conn, job_id, f"Ingestion completed with {ingestion_count} grants, {new_grants} new grants, {new_relevant_grants} new relevant grants, and {grants_with_alerts} grants with alerts.", "INFO")
        conn.commit()
    except Exception as e:
        log(conn, job_id, f"Error in daily ingestion: {e}", "ERROR")
        conn.rollback()
        raise e
