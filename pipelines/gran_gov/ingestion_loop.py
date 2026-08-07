import hashlib
import json
from datetime import datetime, timezone
import time
from typing import Any, Optional
from pipelines.gran_gov.ingestion_utils import fetch_opportunity, normalize_opportunity, update_tribal_eligibility, update_grant_tags
from pipelines.gran_gov.change_detection import detect_changes
from pipelines.gran_gov.quick_classification import quick_classification
from jobs.log_utils import log
from db.db_util import row_get
from pipelines.ai_utils.llm_utils import TokenTracker
from pipelines.ai_utils.prompts import ai_tribal_eligibility_check, ai_grant_tagging
from groq import Groq
import os
from openai import OpenAI
from pipelines.ai_utils.llm_clients import LLMService
from pipelines.ai_utils.llm_clients import GroqProvider, OpenAIProvider
from pipelines.gran_gov.relevancy import apply_content_relevancy, refresh_all_freshness_scores
from pipelines.ai_utils.extraction import (
    grant_from_normalized,
    should_sync_grants_ai,
    sync_grants_ai_for_grant,
    is_grant_tribally_eligible,
)
from pipelines.ai_utils.embed import embed_grants_ai_rows, fetch_grants_ai_by_opportunity_ids

def canonical_json(obj: Any) -> str:
    # Sort keys + compact separators for stable hashing
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _daily_sync_grants_ai_enabled() -> bool:
    return os.getenv("DAILY_INGESTION_SYNC_GRANTS_AI", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )


def _daily_embed_grants_ai_enabled() -> bool:
    return os.getenv("DAILY_INGESTION_EMBED_GRANTS_AI", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )


def _maybe_queue_grants_ai_sync(
    pending: list[dict],
    conn,
    normalized: dict,
    old_data: dict | None,
    opportunity_id: str,
    *,
    tribal_eligible: bool | None = None,
) -> None:
    """Queue tribal-eligible grants that need LLM extraction (deferred batch)."""
    if not _daily_sync_grants_ai_enabled():
        return
    if not should_sync_grants_ai(normalized, old_data):
        return
    if tribal_eligible is None:
        tribal_eligible = is_grant_tribally_eligible(conn, opportunity_id)
    if not tribal_eligible:
        return
    pending.append(grant_from_normalized(normalized))


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


def get_previous_snapshot(conn, opportunity_id: str):
    row = conn.execute(
        """
        SELECT data_json, hash, fetched_at
        FROM grant_snapshots
        WHERE opportunity_id = %s
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (opportunity_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "data_json": row_get(row, "data_json", 0),
        "hash": row_get(row, "hash", 1),
        "fetched_at": row_get(row, "fetched_at", 2),
    }

def insert_snapshot(conn, opportunity_id: str, normalized: dict[str, Any]):
    # Important: sort/unique list fields BEFORE hashing/compare to reduce false positives.
    # (Do this in your normalization function ideally.)
    can = canonical_json(normalized)
    h = sha256_text(can)
    conn.execute(
        """
        INSERT INTO grant_snapshots (opportunity_id, fetched_at, data_json, hash)
        VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
        ON CONFLICT(opportunity_id, hash) DO NOTHING
        """,
        (opportunity_id, can, h),
    )
    return h

def upsert_grant_current(conn, normalized: dict[str, Any]):
    # Assumes normalized has fields matching your schema.
    try:
        conn.execute(
            """
            INSERT INTO grants (
            opportunity_id, number, title, agency, agency_code, status,
            posted_date, close_date,
            deadline_date, deadline_description, last_updated_date,
            award_floor, award_ceiling, estimated_funding, cost_sharing,
            link_url, link_description, grant_gov_url,
            category, eligibility_description, alns, eligibilities, funding_categories, description, attachments
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(opportunity_id) DO UPDATE SET
            number=excluded.number,
            title=excluded.title,
            agency=excluded.agency,
            agency_code=excluded.agency_code,
            status=CASE
                WHEN LOWER(TRIM(COALESCE(grants.status, ''))) = 'closed' THEN grants.status
                ELSE excluded.status
            END,
            posted_date=excluded.posted_date,
            close_date=excluded.close_date,
            deadline_date=excluded.deadline_date,
            deadline_description=excluded.deadline_description,
            last_updated_date=excluded.last_updated_date,
            award_floor=excluded.award_floor,
            award_ceiling=excluded.award_ceiling,
            estimated_funding=excluded.estimated_funding,
            cost_sharing=excluded.cost_sharing,
            link_url=excluded.link_url,
            link_description=excluded.link_description,
            grant_gov_url=excluded.grant_gov_url,
            category=excluded.category,
            eligibility_description=excluded.eligibility_description,
            alns=excluded.alns,
            eligibilities=excluded.eligibilities,
            funding_categories=excluded.funding_categories,
            description=excluded.description,
            attachments=excluded.attachments,
            updated_at=CURRENT_TIMESTAMP
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
                _sql_text(normalized.get("link_url")),
                _sql_text(normalized.get("link_description")),
                _sql_text(normalized.get("grant_gov_url")),
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

def daily_ingestion(conn, opportunity_ids: list[str], job_id: int):
    """
    takes in a list of opportunity ids and checks for any updates.

    Returns a stats dict with:
      - records_processed: grants successfully fetched + upserted this run
      - new_records: grants that had no previous snapshot (first time seen)
      - updated_records: grants where the snapshot hash changed vs. the prior snapshot
    """
    try:
        token_tracker = TokenTracker(job_id, conn=conn)
        groq_provider = GroqProvider(client=Groq(api_key=os.getenv("GROQ_API_KEY")))
        openai_provider = OpenAIProvider(client=OpenAI(api_key=os.getenv("OPENAI_API_KEY")))
        llm_service = LLMService(groq_provider=groq_provider, openai_provider=openai_provider, token_tracker=token_tracker)
        ingestion_count = 0
        new_grants = 0
        updated_grants = 0
        new_relevant_grants = 0
        grants_with_alerts = 0
        grants_ai_synced = 0
        grants_ai_embedded = 0
        pending_embed_ids: list[str] = []
        pending_ai_grants: list[dict] = []
        i = 0
        while i < len(opportunity_ids):
            oid = opportunity_ids[i]
            try:
                raw = fetch_opportunity(oid)                 # call fetchOpportunity
                normalized = normalize_opportunity(raw)    # map to your dict shape
                normalized["id"] = str(oid)          # ensure matches schema

                # Upsert current grant record
                upsert_grant_current(conn, normalized)
                ingestion_count += 1
                log(conn, job_id, f"Upserted grant current for opportunity id: {oid}", "INFO")

                # Load previous snapshot (if any)
                prev = get_previous_snapshot(conn, str(oid))
                old_data = json.loads(prev["data_json"]) if prev else None
                # Insert new snapshot and compute hash for dedupe
                new_hash = insert_snapshot(conn, str(oid), normalized)

                # If there is no previous snapshot, we can skip the diffing process. However, we will need to classify the grant as relevant or not.
                is_tribal_eligible = False
                if prev is None:
                    # Classify the grant as relevant or not
                    new_grants += 1
                    quick_check_result = quick_classification(normalized)
                    if quick_check_result["is_tribal_eligible"]:
                        is_tribal_eligible = True
                        update_tribal_eligibility(conn, str(oid), quick_check_result)
                        log(conn, job_id, f"Identified as new grant and classified as tribal eligible by quick classification for opportunity id: {oid}", "INFO")
                        new_relevant_grants += 1
                    else: 
                        classification = ai_tribal_eligibility_check(llm_service, normalized)
                        if classification is not None and classification["is_tribal_eligible"]:
                            is_tribal_eligible = True
                            update_tribal_eligibility(conn, str(oid), classification)
                            log(conn, job_id, f"Identified as new grant and classified as tribal eligible by AIfor opportunity id: {oid}", "INFO")
                            new_relevant_grants += 1
                        else:
                            update_tribal_eligibility(conn, str(oid), quick_check_result)
                            log(conn, job_id, f"Identified as new grant and classified as not relevant for opportunity id: {oid}", "INFO")
                    # Add tags to new grants (categorization)
                    ai_result = ai_grant_tagging(llm_service, normalized)
                    if ai_result is not None:
                        update_grant_tags(conn, str(oid), ai_result, job_id)
                        log(conn, job_id, f"Tagged new grant with tags: {ai_result['tags']} for opportunity id: {oid}", "INFO")
                    if apply_content_relevancy(conn, normalized):
                        log(conn, job_id, f"Relevancy score updated for new grant {oid}", "INFO")
                    _maybe_queue_grants_ai_sync(
                        pending_ai_grants,
                        conn,
                        normalized,
                        old_data,
                        str(oid),
                        tribal_eligible=is_tribal_eligible,
                    )

                if prev is not None:
                    old_hash = prev["hash"]
                else:
                    old_hash = None
                if old_hash == new_hash or old_hash is None:
                    i += 1
                    continue

                # If we reach this point, this grant had a previous snapshot
                # AND its content hash changed — i.e. it's an updated record.
                updated_grants += 1

                old_data = json.loads(prev["data_json"])
                new_data = normalized

                alerts = detect_changes(old_data, new_data)

                for a in alerts:
                    conn.execute(
                        """
                        INSERT INTO grant_alerts (
                        opportunity_id, alert_type, field,
                        old_value, new_value,
                        old_snapshot_hash, new_snapshot_hash,
                        fetched_at_old, fetched_at_new
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT(opportunity_id, alert_type, field, old_snapshot_hash, new_snapshot_hash) DO NOTHING
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
                if len(alerts) == 0:
                    log(conn, job_id, f"No alerts changes for opportunity id: {oid}", "INFO")
                else:
                    log(conn, job_id, f"Inserted {len(alerts)} alerts for opportunity id: {oid}", "INFO")
                    grants_with_alerts += 1
                    # Add tags to grants with alerts (categorization) because they could have changed                   
                    ai_result = ai_grant_tagging(llm_service, normalized)
                    if ai_result is not None:
                        update_grant_tags(conn, str(oid), ai_result, job_id)
                        log(conn, job_id, f"Tagged new grant with tags: {ai_result['tags']} for opportunity id: {oid}", "INFO")
                    if apply_content_relevancy(conn, normalized):
                        log(conn, job_id, f"Relevancy score updated for changed grant {oid}", "INFO")
                _maybe_queue_grants_ai_sync(
                    pending_ai_grants,
                    conn,
                    normalized,
                    old_data,
                    str(oid),
                )
                i += 1
            except Exception as e:
                log(conn, job_id, f"Error in daily ingestion for opportunity id: {oid}: {e}", "ERROR")
                i += 1
                continue
        freshness_updated = refresh_all_freshness_scores(conn)
        log(
            conn,
            job_id,
            f"Refreshed freshness_score for {freshness_updated} tribal_eligibility rows.",
            "INFO",
        )

        if pending_ai_grants:
            sync_started = time.time()
            for grant in pending_ai_grants:
                oid = grant["opportunity_id"]
                try:
                    if sync_grants_ai_for_grant(conn, grant, llm_service, commit=False):
                        grants_ai_synced += 1
                        pending_embed_ids.append(str(oid))
                    else:
                        log(
                            conn,
                            job_id,
                            f"Failed grants_ai extraction for opportunity id: {oid}",
                            "ERROR",
                        )
                except Exception as e:
                    conn.rollback()
                    log(
                        conn,
                        job_id,
                        f"Error in grants_ai extraction for opportunity id: {oid}: {e}",
                        "ERROR",
                    )
            try:
                conn.commit()
            except Exception as e:
                conn.rollback()
                log(conn, job_id, f"Error committing grants_ai extractions: {e}", "ERROR")
            elapsed = time.time() - sync_started
            log(
                conn,
                job_id,
                f"Batch grants_ai extraction: {grants_ai_synced}/{len(pending_ai_grants)} "
                f"in {elapsed:.1f}s.",
                "INFO",
            )

        if pending_embed_ids and _daily_embed_grants_ai_enabled():
            embed_rows = fetch_grants_ai_by_opportunity_ids(conn, pending_embed_ids)
            saved, failed = embed_grants_ai_rows(conn, embed_rows)
            grants_ai_embedded = saved
            log(
                conn,
                job_id,
                f"Embedded {saved} grant(s) from daily ingestion ({failed} failed).",
                "INFO",
            )

        log(
            conn,
            job_id,
            f"Ingestion completed with {ingestion_count} grants, {new_grants} new grants, "
            f"{updated_grants} updated grants, {new_relevant_grants} new relevant grants, "
            f"{grants_with_alerts} grants with alerts, {grants_ai_synced} grants_ai extractions, "
            f"and {grants_ai_embedded} embeddings.",
            "INFO",
        )
        conn.commit()
        return {
            "records_processed": ingestion_count,
            "new_records": new_grants,
            "updated_records": updated_grants,
            "grants_ai_synced": grants_ai_synced,
            "grants_ai_embedded": grants_ai_embedded,
        }
    except Exception as e:
        log(conn, job_id, f"Error in daily ingestion: {e}", "ERROR")
        conn.rollback()
        raise e
