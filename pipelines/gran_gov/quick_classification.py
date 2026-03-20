"""
Heuristic (non-AI) grant relevance scoring.

Kept separate from backlog_ingestion.py to avoid circular imports:
backlog_ingestion -> main -> ingestion_loop -> (must not import backlog_ingestion).
"""
import json


def safe_json_load(s):
    try:
        return json.loads(s) if s else []
    except Exception:
        return []


def text_contains_keywords(text, keywords):
    if not text:
        return 0
    text = text.lower()
    keywords_found = sum(1 for keyword in keywords if keyword in text)
    return keywords_found


def quick_classification(normalized: dict):
    TRIBAL_CODES = {"07", "11", "09"}

    RELEVANT_CATEGORIES = {"EN", "NR", "ES", "HO", "RD", "CD"}

    WEAK_KEYWORDS = [
        "tribal",
        "native",
        "indian",
        "community",
        "infrastructure",
        "resilience",
        "climate",
        "forest",
        "wood",
        "biomass",
        "gaming",
        "housing",
        "solar",
        "renewable",
        "energy",
        "electric",
        "grid",
        "battery",
        "geothermal",
        "wind",
    ]

    tags = []
    score = 0

    title = (normalized.get("title") or "").lower()
    desc = (normalized.get("description") or "").lower()
    full_text = title + " " + desc

    eligibilities = safe_json_load(normalized.get("eligibilities", ""))
    categories = safe_json_load(normalized.get("funding_categories", ""))

    # --- 1. Eligibility check ---
    has_tribal = any(e.get("id") in TRIBAL_CODES for e in eligibilities)
    has_tribal_description = text_contains_keywords(
        (normalized.get("eligibility_description") or ""), ["tribal", "tribes", "native"]
    )

    if has_tribal or has_tribal_description:
        score += 50
        tags.append("tribal_eligible")

    # --- 2. Keywords match ---
    if text_contains_keywords(full_text, WEAK_KEYWORDS) > 0:
        score += text_contains_keywords(full_text, WEAK_KEYWORDS) * 10
        tags.append("keywords_match")

    # --- 4. Funding category ---
    category_ids = {c.get("id") for c in categories}

    if category_ids & RELEVANT_CATEGORIES:
        score += 30
        tags.append("relevant_category")

    deadline_description = normalized.get("deadline_description", "")
    if text_contains_keywords(deadline_description, ["already", "closed", "expired", "archived", "past"]):
        score -= 75
        tags.append("deadline_expired")

    # --- DECISION LOGIC ---
    if score >= 50:
        is_relevant = True
        needs_ai = False
    elif score <= 20:
        is_relevant = False
        needs_ai = True  # still send to AI to double-check
    else:
        is_relevant = None  # uncertain
        needs_ai = True

    return {
        "relevance_score": score,
        "reasoning": f"quick classification relevance score: {score}",
        "is_relevant": is_relevant,
        "MODEL": "quick_classification",
        "tags": tags,
        "needs_ai": needs_ai,
    }
