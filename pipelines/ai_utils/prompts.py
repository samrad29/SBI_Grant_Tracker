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
        model=GROQ_MODEL_NAME,
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
    openai_model_name = os.getenv("OPENAI_MODEL")
    req = LLMRequest(
        model=openai_model_name,
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
    groq_model_name = os.getenv("GROQ_MODEL")
    req = LLMRequest(
        model=groq_model_name,
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
    groq_model_name = (
        os.getenv("GROQ_MODEL")
        or os.getenv("GROQ_MODEL_NAME")
        or "llama-3.3-70b-versatile"
    )
    req = LLMRequest(
        model=groq_model_name,
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
