import json
import re
from datetime import date, datetime, timezone
from urllib.parse import quote

import requests

from db.db_util import scalar_from_row
from jobs.log_utils import log

OPPORTUNITY_URL = "https://api.grants.gov/v1/api/fetchOpportunity"

_ISO_DATE_PREFIX = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")

# Grants.gov often sends human-readable dates (e.g. "September 15, 2024").
_STRPTIME_FORMATS = (
    "%m/%d/%Y",
    "%m/%d/%y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%B %d %Y",
    "%b %d %Y",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d-%b-%Y",
    "%d %b %Y",
)


def normalize_grant_date(value) -> str | None:
    """
    Parse Grants.gov date strings into ISO ``YYYY-MM-DD`` for storage and sorting.

    Returns None when the value is missing or cannot be parsed.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            ts = float(value)
            if ts > 1e12:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
        except (OSError, OverflowError, ValueError):
            return None
    s = re.sub(r"\s+", " ", str(value).strip())
    if not s:
        return None
    m = _ISO_DATE_PREFIX.match(s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except ValueError:
            pass
    for fmt in _STRPTIME_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return None


def compute_grant_public_url(
    link_url: str | None,
    status: str | None,
    opportunity_id: str | int | None,
    opportunity_number: str | None,
) -> str | None:
    """
    Best public "more information" URL for a grant (same rules as normalize_opportunity).

    1. Use Grants.gov funding description link when present.
    2. Else if status is posted: Grants.gov opportunity detail page by id.
    3. Else if we have an opportunity number: Grants.gov search by that number.
    4. Else None.
    """
    if link_url and str(link_url).strip():
        return str(link_url).strip()
    # oid_s = str(opportunity_id).strip() if opportunity_id is not None else ""
    # if (status or "").strip().lower() == "posted" and oid_s:
    #     return f"https://www.grants.gov/search-results-detail/{oid_s}"
    # num = (opportunity_number or "").strip() if opportunity_number else ""
    # if num:
    #     return f"https://www.grants.gov/search-grants?keywords={quote(num)}"
    else:
        return None


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
    opportunity_id = data.get("opportunityId")
    status_out = "posted" if syn_avail else "forecasted"
    public_url = compute_grant_public_url(
        synopsis.get("fundingDescLinkUrl"),
        status_out,
        opportunity_id,
        data.get("opportunityNumber"),
    )

    return {
        # Core
        "id": opportunity_id,
        "number": data.get("opportunityNumber"),
        "title": data.get("opportunityTitle"),
        "agency": synopsis.get("agencyDetails").get("agencyName") if synopsis.get("agencyDetails") is not None else synopsis.get("agencyName"),
        "agency_code": synopsis.get("agencyDetails").get("agencyCode") if synopsis.get("agencyDetails") is not None else data.get("owningAgencyCode"),

        # Status & dates
        "status": status_out,
        "posted_date": normalize_grant_date(synopsis.get("postingDate")),
        "close_date": normalize_grant_date(data.get("archiveDate")),
        "deadline_date": normalize_grant_date(
            synopsis.get("responseDateStr") if syn_avail else data.get("estApplicationResponseDate")
        ),
        "deadline_description": synopsis.get("responseDateDesc") if syn_avail else data.get("estApplicationResponseDateDesc"),
        "last_updated_date": normalize_grant_date(synopsis.get("lastUpdatedDate")),

        # Funding
        "award_floor": synopsis.get("awardFloor"),
        "award_ceiling": synopsis.get("awardCeiling"),
        "estimated_funding": synopsis.get("estimatedFunding"),
        "cost_sharing": synopsis.get("costSharing"),
        "eligibility_description": synopsis.get("applicantEligibilityDesc"),
        "description": synopsis.get("synopsisDesc") if syn_avail else synopsis.get("forecastDesc"),
        "category": data.get("opportunityCategory"),

        "link_url": synopsis.get("fundingDescLinkUrl"),
        "link_description": synopsis.get("fundingDescLinkDesc"),
        "grant_gov_url": public_url,

        "alns": json.dumps(alns_out, ensure_ascii=False),
        "eligibilities": json.dumps(elig_out, ensure_ascii=False),
        "funding_categories": json.dumps(fund_out, ensure_ascii=False),
        "attachments": json.dumps(att_out, ensure_ascii=False),
    }

def trim_opportunity_ids(opportunity_ids: list, conn) -> list:
    """
    Remove opportunity ids marked not tribally eligible in tribal_eligibility.
    Uses the same connection as the pipeline (Postgres rows are dicts — row[0] raises KeyError).
    """
    try:
        query = """
        SELECT opportunity_id
        FROM tribal_eligibility
        WHERE IS_TRIBAL_ELIGIBLE = FALSE
        """
        rows = conn.execute(query).fetchall()
        db_ids = {str(scalar_from_row(row)) for row in rows}
        return [oid for oid in opportunity_ids if str(oid) not in db_ids]
    except Exception as e:
        print(f"Error trimming opportunity ids: {e}")
        return opportunity_ids

def update_tribal_eligibility(conn, opportunity_id: str, tribal_eligibility: dict):
    """
    Update the tribal eligibility for a given opportunity.
    """
    try:
        is_tribal_eligible = bool(tribal_eligibility.get("is_tribal_eligible", False))
        try:
            eligibility_score = int(tribal_eligibility.get("eligibility_score", 0))
        except (TypeError, ValueError):
            eligibility_score = 0
        eligibility_score = max(0, min(100, eligibility_score))
        eligibility_reasoning = tribal_eligibility.get("eligibility_reasoning")
        if eligibility_reasoning is None:
            eligibility_reasoning = ""
        else:
            eligibility_reasoning = str(eligibility_reasoning)
        model = tribal_eligibility.get("model") or "unknown_model"

        conn.execute(
            """
            INSERT INTO tribal_eligibility (
              opportunity_id, model, eligibility_score, eligibility_reasoning, is_tribal_eligible
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(opportunity_id) DO UPDATE SET
              model=excluded.model,
              eligibility_score=excluded.eligibility_score,
              eligibility_reasoning=excluded.eligibility_reasoning,
              is_tribal_eligible=excluded.is_tribal_eligible,
              updated_at=CURRENT_TIMESTAMP
            """,
            (
                str(opportunity_id),
                model,
                eligibility_score,
                eligibility_reasoning,
                is_tribal_eligible,
            ),
        )
    except Exception as e:
        print(f"Error updating tribal eligibility: {e}")
        conn.rollback()
        raise

def update_grant_tags(conn, opportunity_id: str, ai_result: dict, job_id: int):
    """
    Update the tags for a given opportunity.
    If job_id is -1, it means no job_id (for backlog ingestion)
    """
    try:
        tags = ai_result.get("tags", [])
        for tag in tags:
            conn.execute("""
                INSERT INTO grant_tags (
                  opportunity_id, tag, tag_score, created_at
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(opportunity_id, tag) DO UPDATE SET
                tag_score=excluded.tag_score,
                created_at=excluded.created_at
            """,
            (
                str(opportunity_id),
                tag.get("tag"),
                tag.get("score"),
                datetime.now().isoformat(),
            ),)
            if job_id != -1:
                log(conn, job_id, f"New grant tag: {tag.get('tag')} for opportunity id: {opportunity_id}", "INFO")
        conn.commit()

        new_tags = ai_result.get("new_tags", [])
        for tag in new_tags:
            conn.execute("""
                INSERT INTO grant_tags (
                  opportunity_id, tag, tag_score, created_at
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(opportunity_id, tag) DO UPDATE SET
                tag_score=excluded.tag_score,
                created_at=excluded.created_at
            """,
            (
                str(opportunity_id),
                tag.get("tag"),
                tag.get("score"),
                datetime.now().isoformat(),
            ),)
            if job_id != -1:
                log(conn, job_id, f"Non-standard grant tag: {tag.get('tag')} for opportunity id: {opportunity_id}", "INFO")
        conn.commit()
    except Exception as e:
        print(f"Error updating grant tags: {e}")
        if job_id != -1:
            log(conn, job_id, f"Error updating grant tags: {e}", "ERROR")
        conn.rollback()
        raise
def update_last_seen_at(opportunity_ids: list[str], conn, job_id: int) -> None:
    """
    Update the last_seen_at column for a given opportunity id
    """
    try:
        for oid in opportunity_ids:
            conn.execute("UPDATE grants SET last_seen_at = CURRENT_TIMESTAMP WHERE opportunity_id = %s", (oid,))
        conn.commit()
    except Exception as e:
        print(f"Error updating last_seen_at column: {e}")
        log(conn, job_id, f"Error updating last_seen_at column: {e}", "ERROR")
        conn.rollback()
        raise

def archive_old_grants(conn, job_id: int) -> int:
    try: 
        conn.execute("BEGIN")
        # Cast for DBs where last_seen_at was added as TEXT (legacy) vs TIMESTAMP.
        cutoff = "CURRENT_TIMESTAMP - INTERVAL '5 days'"
        query = f"""
            SELECT COUNT(*) AS cnt FROM grants
            WHERE last_seen_at::timestamptz < {cutoff}
        """
        archived_grants = scalar_from_row(conn.execute(query).fetchone())
        query = f"""
            UPDATE grants
            SET status = 'archived'
            WHERE last_seen_at::timestamptz < {cutoff}
        """
        conn.execute(query)
        conn.commit()
        return int(archived_grants)
    except Exception as e:
        print(f"Error archiving old grants: {e}")
        conn.rollback()
        log(conn, job_id, f"Error archiving old grants: {e}", "ERROR")
        return 0