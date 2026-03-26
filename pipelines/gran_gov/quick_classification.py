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
    try: 
        TRIBAL_CODES = {"07", "11", "09"}
        is_tribal_eligible = False
        eligibility_score = 0
        eligibility_reasoning = ""
        model = "quick_classification"
        needs_ai = True

        eligibilities = safe_json_load(normalized.get("eligibilities", ""))

        # --- 1. Eligibility check ---
        has_tribal = any(e.get("id") in TRIBAL_CODES for e in eligibilities)
        has_tribal_description = text_contains_keywords(
            (normalized.get("eligibility_description") or ""), ["tribal", "tribes", "native"]
        )

        if has_tribal:
            is_tribal_eligible = True
            eligibility_score = 100
            eligibility_reasoning = "Eligibility Codes contain tribal code"
            needs_ai = False
        elif has_tribal_description:
            is_tribal_eligible = True
            eligibility_score = 100
            eligibility_reasoning = "Eligibility description contains tribal codes"
            needs_ai = False

        return {
            "eligibility_score": eligibility_score,
            "eligibility_reasoning": eligibility_reasoning,
            "is_tribal_eligible": is_tribal_eligible,
            "model": model,
            "needs_ai": needs_ai
        }
    except Exception as e:
        print(f"Error in quick_classification: {e}")
        return {
            "eligibility_score": 0,
            "eligibility_reasoning": "",
            "is_tribal_eligible": False,
            "model": model,
            "needs_ai": True
        }
