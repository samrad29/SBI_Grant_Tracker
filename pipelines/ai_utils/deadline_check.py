"""
Classify whether a grant opportunity's deadline has passed using the shared
LLMService and Groq deadline prompt in prompts.py.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from pipelines.ai_utils.llm_clients import LLMService
from pipelines.ai_utils.prompts import ai_classify_deadline_passed

_ISO_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


@dataclass
class DeadlineVerdict:
    deadline_passed: bool
    effective_deadline: str | None
    confidence: str
    reasoning: str
    used_llm: bool


def try_parse_deadline_date(value: str | None) -> date | None:
    """Parse a simple ISO deadline_date without calling the LLM."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = _ISO_DATE_RE.match(s[:10])
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _verdict_from_parsed_date(parsed: date, reference: date) -> DeadlineVerdict:
    passed = parsed < reference
    return DeadlineVerdict(
        deadline_passed=passed,
        effective_deadline=parsed.isoformat(),
        confidence="high",
        reasoning=(
            f"Structured deadline_date {parsed.isoformat()} is "
            f"{'before' if passed else 'on or after'} reference date {reference.isoformat()}."
        ),
        used_llm=False,
    )


def _verdict_from_llm_response(data: dict) -> DeadlineVerdict:
    passed = bool(data.get("deadline_passed"))
    effective = data.get("effective_deadline")
    effective_str = None if effective in (None, "") else str(effective).strip()
    confidence = str(data.get("confidence") or "low").strip().lower()
    if confidence not in {"high", "medium", "low"}:
        confidence = "low"
    reasoning = str(data.get("reasoning") or "").strip()
    return DeadlineVerdict(
        deadline_passed=passed,
        effective_deadline=effective_str,
        confidence=confidence,
        reasoning=reasoning,
        used_llm=True,
    )


def classify_deadline(
    llm_service: LLMService,
    *,
    deadline_date: str | None,
    deadline_description: str | None,
    reference: date | None = None,
) -> DeadlineVerdict:
    """
    Determine whether a grant deadline has passed. Uses deterministic date
    comparison when only a parseable ISO deadline_date is present; otherwise
    calls Groq via LLMService.
    """
    ref = reference or date.today()
    desc = (deadline_description or "").strip()
    parsed = try_parse_deadline_date(deadline_date)

    if not desc and parsed is not None:
        return _verdict_from_parsed_date(parsed, ref)

    data = ai_classify_deadline_passed(
        llm_service,
        deadline_date=deadline_date,
        deadline_description=deadline_description,
        reference_date=ref.isoformat(),
    )
    if data is None:
        raise RuntimeError("Deadline classification returned no result from LLM")

    return _verdict_from_llm_response(data)
