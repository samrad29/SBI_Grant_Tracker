"""
AI Utils
This module will contain the functions to use the AI models to help with the RFPs
General Structure is to send to llama on groq to classify if the text is an rfp or now. 
Then, if it is, we can use gpt-4o-mini to extract the data.
"""
from __future__ import annotations

from operator import truediv
import os
import json
from dotenv import load_dotenv

from pipelines.ai_utils.req_resp_obj import LLMRequest, LLMMessage, LLMResponse
from pipelines.ai_utils.llm_clients import LLMService

load_dotenv()
GROQ_MODEL_NAME = os.getenv("GROQ_MODEL_NAME")
OPENAI_MODEL_NAME = os.getenv("OPENAI_MODEL_NAME")


def get_openai_model_name() -> str:
    """Resolve chat model from env (OPENAI_MODEL or OPENAI_MODEL_NAME)."""
    return (
        os.getenv("OPENAI_MODEL")
        or os.getenv("OPENAI_MODEL_NAME")
        or OPENAI_MODEL_NAME
        or "gpt-4o-mini"
    )


def get_groq_model_name() -> str:
    """Resolve Groq chat model from env."""
    return (
        os.getenv("GROQ_MODEL")
        or os.getenv("GROQ_MODEL_NAME")
        or GROQ_MODEL_NAME
        or "llama-3.3-70b-versatile"
    )


def _extract_json_payload(content: str) -> str:
    """
    Normalize LLM output into a JSON string.
    Handles plain JSON and markdown fenced JSON blocks.
    """
    if not content:
        return ""

    stripped = content.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()

    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end != -1 and end > start:
        return stripped[start : end + 1]

    return stripped


def ai_classify_rfp(text: str, llm: LLMService) -> bool:
    """
    Classify if the text is an rfp or not
    """
    system_content = (
                    "You are a strict classification system.\n"
                    "Your task is to determine whether a document is a Request for Proposal (RFP), "
                    "procurement notice, grant solicitation, or similar funding opportunity.\n\n"
                    "Return ONLY one of the following outputs:\n"
                    "- RFP\n"
                    "- NOT_RFP\n\n"
                    "Do not explain your answer. Do not add punctuation. Do not include any extra text."
                )
    user_content = f"Classify this document:\n\n--- DOCUMENT START ---\n{text[:8000]}\n--- DOCUMENT END ---"

    req = LLMRequest(
        model=get_groq_model_name(),
        provider="groq",
        messages=[
            LLMMessage(role="system", content=system_content),
            LLMMessage(role="user", content=user_content),
        ],
    )

    result = llm.generate(req)
    print(f"Context Tokens for RFP classification: {result.prompt_tokens}")
    print(f"Completion Tokens for RFP classification: {result.completion_tokens}")
    print(f"Total Tokens for RFP classification: {result.total_tokens}")
    print(f"Result content: {result.content}")
    result_content = result.content.lower()
    if result_content not in ["rfp", "rfq", "not_rfp"]:
        print(f"UNEXPECTED AI CLASSIFICATION RESULT: {result_content}")
        return False

    if ("rfp" in result_content or "rfq" in result_content) and "not_rfp" not in result_content:
        return True
    else:
        return False  

def _normalize_tribal_result(raw: dict) -> dict:
    """
    Guarantee required keys for tribal_eligibility inserts.
    """
    is_tribal_eligible = bool(raw.get("is_tribal_eligible", False))
    score = raw.get("eligibility_score", 0)
    try:
        score = int(score)
    except (TypeError, ValueError):
        score = 0
    score = max(0, min(100, score))
    reasoning = raw.get("eligibility_reasoning")
    if reasoning is None:
        reasoning = ""
    else:
        reasoning = str(reasoning)
    return {
        "model": "groq",
        "is_tribal_eligible": is_tribal_eligible,
        "eligibility_score": score,
        "eligibility_reasoning": reasoning,
    }


def ai_grant_tagging(llm_service, grant):
    system_content = (
        "You are a grant categorization and scoring system. Your results will be viewed by member of federally recognized Native American tribal governments to help them find grants that are relevant to them.\n"
        "Your task is to score how well a government grant fits into predefined categories. A grant can fit into multiple categories, so you must assign relevance scores (0-100) to each category.\n"
        "You must assign relevance scores (0-100) to the predefined categories.\n"
        "Remember, the end user is a tribal government member, so a high score in a category like housing should mean that the grant is relevant to housing and is likely to be of interest to a tribal community."
        "The predefined categories are: Housing, Historic Preservation, Gaming, Energy, Sustainability, Infrastructure, and Workforce Development.\n"
        "You can optionally suggest up to 3 NEW categories if the predefined ones are insufficient.\n"
        "Return ONLY a valid JSON object in the following format:\n"
        '{"tags": [{"tag": "category", "score": 0-100}], "new_tags": [{"tag": "new_category", "score": 0-100}]}\n'
    )
    user_content = (
        f"Evaluate this grant:\n\n--- GRANT Title ---\n{grant['title']}\n"
        f"\n\n--- GRANT Description ---\n{grant.get('description', '')[:1500]}\n"
        f"\n\n--- GRANT Eligibility Codes ---\n{grant.get('eligibilities', [])}\n"
        f"\n\n--- GRANT Eligibility Description ---\n{grant.get('eligibility_description', '')}\n"
        f"\n\n--- GRANT Deadline Date ---\n{grant.get('deadline_date', '')}\n"
        f"\n\n--- GRANT Deadline Description ---\n{grant.get('deadline_description', '')}\n"
        "\n\n--- GRANT END ---"
    )
    req = LLMRequest(
        model=get_openai_model_name(),
        provider="openai",
        messages=[
            LLMMessage(role="system", content=system_content),
            LLMMessage(role="user", content=user_content),
        ],
    )
    result = llm_service.generate(req)
    if not result.content:
        print("No content returned from AI")
        return None
    try:
        parsed = _extract_json_payload(result.content)
        return json.loads(parsed)
    except Exception as e:
        print(result.content)
        print("Error parsing JSON in ai_grant_tagging:", e)
        return None

def ai_tribal_eligibility_check(llm_service, grant):
    system_content = (
        "You are a tribal eligibility evaluation system.\n"
        "Your task is to determine whether a Native American tribal government is eligible for a federal grant. "
        "Return ONLY one of the following outputs in valid JSON format:\n"
        '{"model": "groq", "is_tribal_eligible": true/false, "eligibility_score": 0-100, "eligibility_reasoning": ""}\n'
        "Do not explain your answer. Do not add punctuation. Do not include any extra text."
    )
    user_content = (
        f"Evaluate this grant:\n\n--- GRANT Title ---\n{grant['title']}\n"
        f"\n\n--- GRANT Description ---\n{grant.get('description', '')[:1500]}\n"
        f"\n\n--- GRANT Eligibility Codes ---\n{grant.get('eligibilities', [])}\n"
        f"\n\n--- GRANT Eligibility Description ---\n{grant.get('eligibility_description', '')}\n"
        f"\n\n--- GRANT Deadline Date ---\n{grant.get('deadline_date', '')}\n"
        f"\n\n--- GRANT Deadline Description ---\n{grant.get('deadline_description', '')}\n"
        "\n\n--- GRANT END ---"
    )
    req = LLMRequest(
        model=get_groq_model_name(),
        provider="groq",
        messages=[
            LLMMessage(role="system", content=system_content),
            LLMMessage(role="user", content=user_content),
        ],
    )
    result = llm_service.generate(req)
    result_content = result.content

    if not result_content:
        print("No content returned from AI")
        return None
    try:
        parsed = json.loads(result_content)
        if not isinstance(parsed, dict):
            print("Unexpected JSON shape in ai_tribal_eligibility_check;")
            return _normalize_tribal_result({})
        return _normalize_tribal_result(parsed)
    except Exception as e:
        print("Error parsing JSON in ai_tribal_eligibility_check:", e)
        return None


def ai_classify_deadline_passed(
    llm_service: LLMService,
    *,
    deadline_date: str | None,
    deadline_description: str | None,
    reference_date: str,
) -> dict | None:
    """
    Use Groq to decide whether a grant deadline has passed as of reference_date.
    Returns parsed JSON dict or None on failure.
    """
    desc = (deadline_description or "").strip() or "(none)"
    ddate = (deadline_date or "").strip() or "(none)"
    system_content = (
        "You evaluate grant application deadlines.\n"
        "Return ONLY valid JSON in this format:\n"
        '{"deadline_passed": boolean, "effective_deadline": "YYYY-MM-DD or null", '
        '"confidence": "high|medium|low", "reasoning": "brief explanation"}\n'
        "Do not include any text outside the JSON object."
    )
    user_content = (
        f"Today's reference date is {reference_date} (YYYY-MM-DD).\n\n"
        f"deadline_date field: {ddate}\n"
        f"deadline_description field:\n{desc}\n\n"
        "Decide whether the grant application or submission deadline has PASSED "
        f"as of {reference_date}.\n\n"
        "Rules:\n"
        "- Rolling, ongoing, open until filled, or no fixed deadline → deadline_passed=false.\n"
        "- If multiple dates, use the final application/submission deadline.\n"
        "- If only a calendar date with no time, treat the deadline as end of that day.\n"
        "- If information is missing or too ambiguous, deadline_passed=false and confidence=low."
    )
    req = LLMRequest(
        model=get_groq_model_name(),
        provider="groq",
        messages=[
            LLMMessage(role="system", content=system_content),
            LLMMessage(role="user", content=user_content),
        ],
    )
    result: LLMResponse = llm_service.generate(req)
    if not result.content:
        print("No content returned from AI in ai_classify_deadline_passed")
        return None
    try:
        parsed = _extract_json_payload(result.content)
        data = json.loads(parsed)
        if not isinstance(data, dict):
            print("Unexpected JSON shape in ai_classify_deadline_passed")
            return None
        return data
    except Exception as e:
        print(result.content)
        print("Error parsing JSON in ai_classify_deadline_passed:", e)
        return None

def _grant_eligibility_text(grant: dict) -> str:
    elig = (grant.get("eligibility_description") or "").strip()
    if elig:
        return elig
    raw = grant.get("eligibilities")
    if raw and str(raw).strip():
        return str(raw).strip()
    return "(none)"


def extract_grants_ai_required_fields(grant: dict, llm_service: LLMService) -> dict | None:
    """
    Extract the required fields from the grant to be used in the grants_ai table.
    Returns parsed JSON dict or None on failure.
    """
    title = (grant.get("title") or "").strip() or "(none)"
    agency = (grant.get("agency") or "").strip() or "(none)"
    description = (grant.get("description") or "").strip() or "(none)"
    eligibility = _grant_eligibility_text(grant)

    system_content = (
        "You are an expert grant analyst specializing in U.S. federal grant opportunities.\n"
        "Your job is to transform raw grant data into a structured, normalized representation that will later be used for:\n"
        "- semantic search\n"
        "- AI recommendations\n"
        "- grant summaries\n"
        "- retrieval evaluation\n\n"
        "Do NOT invent information.\n"
        "If information is not supported by the grant, return an empty array or null.\n"
        "Prefer concise, factual language.\n"
        "Return ONLY valid JSON."
    )
    user_content = (
        "Below is a grant opportunity from Grants.gov.\n"
        "Normalize this grant into the requested JSON format.\n\n"
        "Return JSON with exactly these keys:\n"
        "{\n"
        '  "purpose": "string",\n'
        '  "funding_topics": ["string"],\n'
        '  "eligible_applicants": ["string"],\n'
        '  "project_examples": ["string"],\n'
        '  "problems_addressed": ["string"],\n'
        '  "desired_outcomes": ["string"],\n'
        '  "common_search_queries": ["string"]\n'
        "}\n\n"
        "Guidelines:\n"
        "1. Purpose\n"
        "  - Write a concise 1-3 sentence summary.\n"
        "  - Focus on what the grant funds.\n"
        "  - Avoid administrative details.\n\n"
        "2. Funding Topics\n"
        "  - Extract 5-15 major funding concepts.\n"
        "  - Examples:\n"
        "    - broadband\n"
        "    - housing\n"
        "    - behavioral health\n"
        "    - renewable energy\n"
        "    - workforce development\n\n"
        "3. Eligible Applicants\n"
        "  - Normalize eligibility into a clean list.\n"
        "  - Preserve important distinctions.\n\n"
        "4. Project Examples\n"
        "  - Generate 10-20 examples of projects that would reasonably be funded.\n"
        "  - These should be realistic examples supported by the grant.\n"
        "  - Examples:\n"
        "    - Install solar arrays\n"
        "    - Upgrade water treatment facilities\n"
        "    - Restore wetlands\n"
        "  - Do NOT invent unrelated projects.\n\n"
        "5. Problems Addressed\n"
        "  - Identify the real-world problems this grant attempts to solve.\n"
        "  - Examples:\n"
        "    - unreliable internet access\n"
        "    - opioid addiction\n"
        "    - flood damage\n"
        "    - aging infrastructure\n"
        "    - language loss\n"
        "  - Return 5-15 problems.\n\n"
        "6. Expected Outcomes\n"
        "  - Identify likely desired outcomes.\n"
        "  - Examples:\n"
        "    - improved broadband access\n"
        "    - cleaner drinking water\n"
        "    - lower wildfire risk\n"
        "    - increased graduation rates\n"
        "  - Return 5-15 outcomes.\n\n"
        "7. Common Search Queries\n"
        "  - Generate 15-20 realistic search queries someone might type into a grants portal.\n"
        "  - Include:\n"
        "    - short queries\n"
        "    - long queries\n"
        "    - synonyms\n"
        "    - lay language\n"
        "    - problem-focused searches\n"
        "    - project-focused searches\n"
        "  - Examples:\n"
        "    - solar grants\n"
        "    - help with flooding\n"
        "    - money for mental health\n"
        "    - replace lead pipes\n"
        "    - community wildfire protection\n"
        "  - These should reflect how a real user would search.\n\n"
        "Here is the grant:\n"
        "Title:\n"
        f"{title}\n\n"
        "Agency:\n"
        f"{agency}\n\n"
        "Description:\n"
        f"{description[:12000]}\n\n"
        "Eligibility:\n"
        f"{eligibility[:4000]}\n\n"
        "--- GRANT END ---"
    )
    req = LLMRequest(
        model=get_openai_model_name(),
        provider="openai",
        messages=[
            LLMMessage(role="system", content=system_content),
            LLMMessage(role="user", content=user_content),
        ],
    )
    result = llm_service.generate(req)
    if not result.content:
        print("No content returned from AI")
        return None
    try:
        parsed = _extract_json_payload(result.content)
        return json.loads(parsed)
    except Exception as e:
        print(result.content)
        print("Error parsing JSON in extract_grants_ai_required_fields:", e)
        return None