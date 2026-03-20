import requests
import json
import sqlite3
from jobs.log_utils import log
OPPORTUNITY_URL = "https://api.grants.gov/v1/api/fetchOpportunity"


def _as_dict(value) -> dict:
    """Grants.gov sometimes returns synopsis (or nested objects) as non-dicts; normalize safely."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return {}


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return []


def fetch_opportunity(opportunity_id: int) -> dict:
    """
    Fetch the full details for a single opportunity.
    """
    payload = {"opportunityId": opportunity_id}
    headers = {"Content-Type": "application/json"}
    response = requests.post(OPPORTUNITY_URL, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    if data.get("errorcode", -1) != 0:
        raise RuntimeError(f"API error: {data.get('msg', 'Unknown error')}")
    return data.get("data", {})


def normalize_opportunity(data: dict) -> dict:
    """
    Normalize the opportunity details to make comparison easier
    """
    syn_avail = True
    forecast_avail = False
    synopsis = _as_dict(data.get("synopsis"))
    if not synopsis or synopsis == {}:
        syn_avail = False
        forecast_avail = True
        synopsis = _as_dict(data.get("forecast"))
    if not synopsis or synopsis == {}:
        print("No synopsis or forecast found")
        forecast_avail = False

    alns_out = []
    for c in _as_list(data.get("cfdas")):
        if not isinstance(c, dict):
            continue
        alns_out.append({"number": c.get("cfdaNumber"), "title": c.get("programTitle")})

    elig_out = []
    for e in _as_list(synopsis.get("applicantTypes")):
        if not isinstance(e, dict):
            continue
        elig_out.append({"id": e.get("id"), "description": e.get("description")})

    fund_out = []
    for c in _as_list(synopsis.get("fundingActivityCategories")):
        if not isinstance(c, dict):
            continue
        fund_out.append({"id": c.get("id"), "description": c.get("description")})

    att_out = []
    for folder in _as_list(data.get("synopsisAttachmentFolders")):
        if not isinstance(folder, dict):
            continue
        for a in _as_list(folder.get("synopsisAttachments")):
            if not isinstance(a, dict):
                continue
            att_out.append(
                {
                    "filename": a.get("fileName"),
                    "file_description": a.get("fileDescription"),
                    "mime_type": a.get("mimeType"),
                    "url": a.get("fileUrl"),
                }
            )

    return {
        # Core
        "id": data.get("opportunityId"),
        "number": data.get("opportunityNumber"),
        "title": data.get("opportunityTitle"),
        "agency": synopsis.get("agencyName"),
        "agency_code": data.get("owningAgencyCode"),

        # Status & dates
        "status": "posted" if syn_avail else "forecasted",
        "posted_date": synopsis.get("postingDate"),
        "close_date": data.get("archiveDate"),
        "deadline_date": synopsis.get("responseDateStr") if syn_avail else data.get("estApplicationResponseDate"),
        "deadline_description": synopsis.get("responseDateDesc") if syn_avail else data.get("estApplicationResponseDateDesc"),
        "last_updated_date": synopsis.get("lastUpdatedDate"),

        # Funding
        "award_floor": synopsis.get("awardFloor"),
        "award_ceiling": synopsis.get("awardCeiling"),
        "estimated_funding": synopsis.get("estimatedFunding"),
        "cost_sharing": synopsis.get("costSharing"),
        "eligibility_description": synopsis.get("applicantEligibilityDesc"),
        "description": synopsis.get("synopsisDesc") if syn_avail else synopsis.get("forecastDesc"),
        "category": data.get("opportunityCategory"),

        "alns": json.dumps(alns_out, ensure_ascii=False),
        "eligibilities": json.dumps(elig_out, ensure_ascii=False),
        "funding_categories": json.dumps(fund_out, ensure_ascii=False),
        "attachments": json.dumps(att_out, ensure_ascii=False),
    }

def trim_opportunity_ids(opportunity_ids: list[str]) -> list[str]:
    """
    Remove irrelevant opportunity ids from the list
    """
    try:
        conn = sqlite3.connect("grants.db")
        conn.execute("BEGIN")
        query = """
        SELECT opportunity_id
        FROM grant_classifications
        WHERE IS_RELEVANT = FALSE
        """
        db_ids = {row[0] for row in conn.execute(query).fetchall()}
        return [oid for oid in opportunity_ids if oid not in db_ids]
    except Exception as e:
        print(f"Error trimming opportunity ids: {e}")
        return None

def update_grant_classification(conn: sqlite3.Connection, opportunity_id: str, classification: dict):
    """
    Update the classification for a given opportunity.
    """
    try:
        tags = classification.get("tags", [])
        tags_json = json.dumps(tags, ensure_ascii=False) if not isinstance(tags, str) else tags

        ir = classification.get("is_relevant")
        if ir is True:
            is_rel = 1
        elif ir is False:
            is_rel = 0
        elif isinstance(ir, str) and ir.strip().lower() in ("true", "1", "yes"):
            is_rel = 1
        elif isinstance(ir, str) and ir.strip().lower() in ("false", "0", "no"):
            is_rel = 0
        else:
            is_rel = 0

        conn.execute(
            """
            INSERT INTO grant_classifications (
              opportunity_id, TAGS, MODEL, RELEVANCE_SCORE, REASONING, IS_RELEVANT
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(opportunity_id) DO UPDATE SET
              TAGS=excluded.TAGS,
              MODEL=excluded.MODEL,
              RELEVANCE_SCORE=excluded.RELEVANCE_SCORE,
              REASONING=excluded.REASONING,
              IS_RELEVANT=excluded.IS_RELEVANT,
              UPDATED_AT=datetime('now')
            """,
            (
                str(opportunity_id),
                tags_json,
                classification.get("MODEL", ""),
                int(classification.get("relevance_score", 0)),
                classification.get("reasoning", ""),
                is_rel,
            ),
        )
    except Exception as e:
        print(f"Error updating grant classification: {e}")
        conn.rollback()
        raise

def update_last_seen_at(opportunity_ids: list[str], conn: sqlite3.Connection, job_id: int) -> None:
    """
    Update the last_seen_at column for a given opportunity id
    """
    try:
        for oid in opportunity_ids:
            conn.execute("UPDATE grants SET last_seen_at = datetime('now') WHERE opportunity_id = ?", (oid,))
        conn.commit()
    except Exception as e:
        print(f"Error updating last_seen_at column: {e}")
        log(conn, job_id, f"Error updating last_seen_at column: {e}", "ERROR")
        conn.rollback()
        raise

def archive_old_grants(conn: sqlite3.Connection, job_id: int) -> int:
    try: 
        conn.execute("BEGIN")
        query = """
            SELECT COUNT(*) FROM grants
            WHERE last_seen_at < datetime('now', '-5 days')
        """
        archived_grants = conn.execute(query).fetchone()[0]
        query = """
            UPDATE grants
            SET status = 'archived'
            WHERE last_seen_at < datetime('now', '-5 days')
        """
        conn.execute(query)
        conn.commit()
        return int(archived_grants)
    except Exception as e:
        print(f"Error archiving old grants: {e}")
        log(conn, job_id, f"Error archiving old grants: {e}", "ERROR")
        return 0