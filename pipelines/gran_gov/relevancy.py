import json
from datetime import date, datetime, timezone
from typing import Any

from db.db_util import row_get
from pipelines.gran_gov.ingestion_utils import normalize_grant_date

TRIBAL_KEYWORDS = {
    "tribal": 20,
    "tribe": 20,
    "native american": 18,
    "indigenous": 15,
    "reservation": 15,
    "bureau of indian affairs": 25,
    "rural": 5,
    "community development": 8,
    "housing": 10,
    "infrastructure": 10,
    "broadband": 10,
    "healthcare": 8,
    "behavioral health": 8,
    "substance abuse": 7,
    "education": 7,
    "language preservation": 12,
    "workforce development": 8,
    "economic development": 10,
    "water system": 10,
    "renewable energy": 8,
}

NEGATIVE_KEYWORDS = {
    "phd": -20,
    "doctoral": -20,
    "laboratory": -15,
    "particle physics": -30,
    "genome": -20,
    "clinical trial": -15,
    "research university": -20,
    "postdoctoral": -25,
    "advanced scientific research": -25,
    "quantum": -20,
    "nuclear physics": -25,
    "astronomy": -20,
}

AGENCY_BOOSTS = {
    "bureau of indian affairs": 40,
    "indian health service": 25,
    "hud": 15,
    "usda": 12,
    "epa": 10,
    "hhs": 10,
    "department of energy": 8,
    "department of transportation": 8,
}


def _field(grant: Any, name: str, default: str = "") -> str:
    if isinstance(grant, dict):
        value = grant.get(name, default)
    else:
        value = row_get(grant, name, 0)
        if value is None:
            value = default
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def calculate_freshness_score(posted_date_str) -> int:
    iso = normalize_grant_date(posted_date_str)
    if not iso:
        return 0
    try:
        posted_date = date.fromisoformat(iso)
    except ValueError:
        return 0

    now = datetime.now(timezone.utc).date()
    days_old = (now - posted_date).days

    if days_old <= 7:
        return 15
    if days_old <= 30:
        return 10
    if days_old <= 90:
        return 5

    return 0


def score_relevancy(grant) -> int:
    """Keyword-based content relevancy (excludes freshness; stored separately)."""
    try:
        score = 0

        searchable_text = " ".join(
            [
                _field(grant, "title"),
                _field(grant, "description"),
                _field(grant, "agency"),
                _field(grant, "eligibility_description"),
            ]
        ).lower()

        for keyword, value in TRIBAL_KEYWORDS.items():
            if keyword in searchable_text:
                score += value

        for keyword, value in NEGATIVE_KEYWORDS.items():
            if keyword in searchable_text:
                score += value

        agency = _field(grant, "agency").lower()
        for keyword, value in AGENCY_BOOSTS.items():
            if keyword in agency:
                score += value

        eligibilities = _field(grant, "eligibilities").lower()
        if "07" in eligibilities:
            score += 20
        if "99" in eligibilities:
            score += 5
        if "11" in eligibilities:
            score += 5

        return score
    except Exception as e:
        print(f"Error calculating relevancy score: {e}")
        return 0


def tribal_rank_score(relevancy_score, freshness_score) -> int:
    return int(relevancy_score or 0) + int(freshness_score or 0)


def save_tribal_scoring(
    conn,
    opportunity_id: str,
    *,
    relevancy_score: int | None = None,
    freshness_score: int | None = None,
) -> bool:
    """
    Update scoring columns on an existing ``tribal_eligibility`` row.

    Does not insert (other columns are NOT NULL). Returns True if a row was updated.
    """
    sets: list[str] = []
    params: list[Any] = []
    if relevancy_score is not None:
        sets.append("relevancy_score = %s")
        params.append(relevancy_score)
    if freshness_score is not None:
        sets.append("freshness_score = %s")
        params.append(freshness_score)
    if not sets:
        return False

    sets.append("updated_at = CURRENT_TIMESTAMP")
    params.append(str(opportunity_id))
    cur = conn.execute(
        f"""
        UPDATE tribal_eligibility
        SET {", ".join(sets)}
        WHERE opportunity_id = %s
        """,
        tuple(params),
    )
    updated = cur.rowcount is not None and cur.rowcount > 0
    if updated:
        print(
            f"Tribal scoring saved for {opportunity_id}: "
            f"relevancy={relevancy_score!r} freshness={freshness_score!r}"
        )
    return updated


def save_relevancy_score(conn, opportunity_id, relevancy_score) -> bool:
    """Backward-compatible wrapper."""
    return save_tribal_scoring(conn, opportunity_id, relevancy_score=relevancy_score)


def apply_content_relevancy(conn, grant: dict) -> bool:
    """Recompute and persist content ``relevancy_score`` for one grant."""
    oid = str(grant.get("id") or grant.get("opportunity_id") or "")
    if not oid:
        return False
    return save_tribal_scoring(conn, oid, relevancy_score=score_relevancy(grant))


def refresh_all_freshness_scores(conn) -> int:
    """
    Recompute ``freshness_score`` from ``grants.posted_date`` for every grant
    that has a ``tribal_eligibility`` row (run once per daily job).
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT grants.opportunity_id, grants.posted_date
        FROM grants
        INNER JOIN tribal_eligibility
            ON grants.opportunity_id = tribal_eligibility.opportunity_id
        """
    )
    rows = cur.fetchall()
    updated = 0
    for row in rows:
        oid = row_get(row, "opportunity_id", 0)
        posted = row_get(row, "posted_date", 1)
        freshness = calculate_freshness_score(posted)
        if save_tribal_scoring(conn, str(oid), freshness_score=freshness):
            updated += 1
    return updated
