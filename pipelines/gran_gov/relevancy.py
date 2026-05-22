import sqlite3
from datetime import date, datetime, timezone

from pipelines.gran_gov.ingestion_utils import normalize_grant_date

DB_PATH = "grants.db"

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

def calculate_freshness_score(posted_date_str):
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
    elif days_old <= 30:
        return 10
    elif days_old <= 90:
        return 5

    return 0


def calculate_score(grant):
    try:
        score = 0

        searchable_text = " ".join([
            grant["title"] or "",
            grant["description"] or "",
            grant["agency"] or "",
            grant["eligibility_description"] or "",
        ]).lower()

        # Positive keyword scoring
        for keyword, value in TRIBAL_KEYWORDS.items():
            if keyword in searchable_text:
                score += value

        # Negative keyword scoring
        for keyword, value in NEGATIVE_KEYWORDS.items():
            if keyword in searchable_text:
                score += value

        # Agency boosts
        agency = (grant["agency"] or "").lower()

        for keyword, value in AGENCY_BOOSTS.items():
            if keyword in agency:
                score += value

        # Eligibility boost
        eligibilities = (grant["eligibilities"] or "").lower()

        if "07" in eligibilities:
            score += 20

        if "99" in eligibilities:
            score += 5

        if "11" in eligibilities:
            score += 5
        # Freshness
        score += calculate_freshness_score(grant["posted_date"])

        return score
    except Exception as e:
        print(f"Error calculating relevancy score: {e}")
        return 0


def save_relevancy_score(conn, opportunity_id, relevancy_score):
    query = """
    INSERT INTO tribal_eligibility (opportunity_id, score)
    VALUES (%s, %s)
    ON CONFLICT (opportunity_id) DO UPDATE SET
    score = EXCLUDED.score
    """
    conn.execute(query, (opportunity_id, relevancy_score))
    conn.commit()