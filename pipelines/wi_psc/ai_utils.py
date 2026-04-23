from __future__ import annotations

import json
import os
import re
import time
from collections import defaultdict

from dotenv import load_dotenv
from groq import Groq, RateLimitError

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL") or "llama-3.3-70b-versatile"


def get_groq_client():
    if not GROQ_API_KEY:
        raise RuntimeError(
            "Missing Groq API key. Set GROQ_API_KEY in your environment (or .env)."
        )
    return Groq(api_key=GROQ_API_KEY, max_retries=0)


class GroqLLMClient:
    """Wrapper around the Groq chat completions API."""

    def __init__(self):
        self._client = get_groq_client()

    def complete(self, prompt: str, *, response_json_object: bool = False) -> str:
        max_attempts = 6
        last_err: Exception | None = None
        for attempt in range(max_attempts):
            try:
                kwargs: dict = {
                    "model": GROQ_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0,
                }
                if response_json_object:
                    kwargs["response_format"] = {"type": "json_object"}
                response = self._client.chat.completions.create(**kwargs)
                content = response.choices[0].message.content
                return content or ""
            except RateLimitError as e:
                last_err = e
                msg = str(e)
                wait_s = 5.0
                m = re.search(r"try again in ([\d.]+)s", msg, re.I)
                if m:
                    wait_s = float(m.group(1)) + 0.75
                time.sleep(wait_s)
            except Exception as e:
                last_err = e
                msg = str(e).lower()
                if "tokens per minute" in msg or "tpm" in msg or "rate limit" in msg:
                    time.sleep(6.0 * (attempt + 1))
                    continue
                raise
        if last_err is not None:
            raise RuntimeError("Groq request failed after retries") from last_err
        raise RuntimeError("Groq request failed after retries")


_FENCED_BLOCK_RE = re.compile(
    r"```(?:json)?\s*\n(.*?)```",
    re.DOTALL | re.IGNORECASE,
)


def _try_decode_json_object(fragment: str) -> dict | None:
    """Find the first top-level JSON object in ``fragment`` and parse it."""
    s = fragment.strip()
    if not s:
        return None
    if s.startswith("\ufeff"):
        s = s.lstrip("\ufeff").strip()
    dec = json.JSONDecoder()
    for i, ch in enumerate(s):
        if ch != "{":
            continue
        try:
            obj, _end = dec.raw_decode(s, i)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            return obj
    return None


def _parse_json_object(raw: str) -> dict:
    """
    Parse a single JSON object from a model reply that may include prose,
    markdown fences, or Python code. Prefer Groq ``json_object`` mode when set.
    """
    if raw is None:
        raise ValueError("empty model response")
    s = str(raw).strip()
    if not s:
        raise ValueError("empty model response")

    obj = _try_decode_json_object(s)
    if obj is not None:
        return obj

    for m in _FENCED_BLOCK_RE.finditer(s):
        inner = m.group(1).strip()
        obj = _try_decode_json_object(inner)
        if obj is not None:
            return obj

    raise ValueError("no JSON object found in model response")


# Groq on-demand tiers often cap input TPM; keep prompts small.
MAIN_TEXT_CHAR_LIMIT = 3500
ATTACHMENT_HEAD_CHARS = 500
# Include enough slots that ERF viewdoc + key OEI docs reach the model after prioritize_candidate_links.
MAX_ATTACHMENTS_IN_PROMPT = 5
RAG_CONTEXT_CHAR_LIMIT = 16_000
RAG_FIELD_CHAR_BUDGETS: dict[str, int] = {
    "funding": 6000,
    "eligibility": 4500,
    "deadline": 3000,
    "program_description": 4000,
    "general": 2500,
}

EXTRACTION_JSON_INSTRUCTIONS = """Your entire reply MUST be one valid JSON object and nothing else.
The first character must be `{` and the last must be `}`. Do not use markdown, code fences, or explanations.

Keys:
- program_name (string)
- program_status (string, e.g. open/closed/accepting applications/TBD)
- description (string, concise summary of the program)
- elibilities (array of strings, who can apply)
- estimated_funding (number or null, total pool in USD if stated)
- estimated_funding_description (string or null, description of the estimated funding)
- deadline_date (string YYYY-MM-DD or null)
- attachments (array of strings, important document URLs mentioned or from the input)
Use null where unknown. Do not invent URLs not present in the input."""


# Link-ranking call: keep input small for Groq TPM tiers.
LINK_RANK_MAX_RECORDS = 28
LINK_RANK_ANCHOR_CHARS = 120


def rank_candidate_links_for_fetch(
    program_url: str,
    link_records: list[tuple[str, str]],
    max_pick: int,
    heuristic_order: list[str],
) -> list[str]:
    """
    Ask the model to order up to ``max_pick`` URLs from ``link_records`` for
    downstream fetch + extraction. Every returned URL must appear in the input.

    ``heuristic_order`` should be the same candidate set ordered by rules
    (e.g. ``prioritize_candidate_links``); it is used to pad or replace the
    model output when the call fails or returns invalid URLs.
    """
    if not link_records:
        return []
    if max_pick <= 0:
        return []

    allowed = {u for u, _ in link_records}
    if not allowed:
        return []

    rows = []
    for u, anchor in link_records[:LINK_RANK_MAX_RECORDS]:
        rows.append(
            {"url": u, "anchor": (anchor or "")[:LINK_RANK_ANCHOR_CHARS]}
        )

    instructions = f"""You help a grant analyst choose which linked pages to download first under a strict budget.

Your entire reply MUST be one valid JSON object and nothing else. The first character must be `{{` and the last must be `}}`.
Do not write Python, pseudocode, markdown, or explanations.

Program page URL: {program_url}

Input (JSON array of objects with "url" and "anchor" — anchor is visible link text from that page):
{json.dumps(rows, ensure_ascii=False)}

Rules:
- Output shape exactly: {{"ordered_urls": ["<url>", ...]}}
- "ordered_urls" must contain at most {max_pick} strings.
- Every string must be exactly one of the "url" values from the input array (character-for-character identical).
- Order best-first for understanding this program: formal application instructions / NOFA / RFP / PSC filings, eligibility, deadlines, scoring, then forms and OEI documents. Put generic portals (e.g. bare "login", "home", map viewers) later unless they are the only useful links.
- If unsure, prefer URLs that look like filed documents (e.g. viewdoc, docket), PDFs/DOCX under program document paths, then grants/dockets apps.
"""

    picked: list[str] = []
    try:
        client = GroqLLMClient()
        raw = client.complete(instructions, response_json_object=True)
        data = _parse_json_object(raw)
        raw_list = data.get("ordered_urls")
        if isinstance(raw_list, list):
            for item in raw_list:
                if not isinstance(item, str):
                    continue
                u = item.strip()
                if u in allowed and u not in picked:
                    picked.append(u)
                if len(picked) >= max_pick:
                    break
    except Exception:
        picked = []

    for u in heuristic_order:
        if u in allowed and u not in picked:
            picked.append(u)
        if len(picked) >= max_pick:
            break
    if len(picked) < max_pick:
        for u, _ in link_records:
            if u not in picked:
                picked.append(u)
            if len(picked) >= max_pick:
                break
    return picked[:max_pick]


def _truncate_block(s: str, max_chars: int) -> str:
    s = s or ""
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 3] + "..."


def _budget_rag_context_by_set(rag_context: str, max_total: int) -> str:
    """
    Parse RAG excerpt blocks, then apply per-field character budgets.

    Expected block header format from rag_util:
      --- RAG excerpt N | set=<set_name> | ...
    """
    text = (rag_context or "").strip()
    if not text:
        return ""

    # Split while preserving headers.
    blocks = re.split(r"(?m)^--- RAG excerpt ", text)
    grouped: dict[str, list[str]] = defaultdict(list)

    for b in blocks:
        b = b.strip()
        if not b:
            continue
        block = "--- RAG excerpt " + b
        m = re.search(r"\|\s*set=([a-zA-Z0-9_]+)\s*\|", block)
        set_name = m.group(1).lower() if m else "general"
        grouped[set_name].append(block)

    if not grouped:
        return _truncate_block(text, max_total)

    set_order = ["funding", "eligibility", "deadline", "program_description", "general"]
    parts: list[str] = []
    used_total = 0

    for set_name in set_order:
        entries = grouped.get(set_name, [])
        if not entries:
            continue
        budget = RAG_FIELD_CHAR_BUDGETS.get(set_name, RAG_FIELD_CHAR_BUDGETS["general"])
        section = "\n\n".join(entries).strip()
        section = _truncate_block(section, budget)
        section = f"### {set_name}\n{section}".strip()

        room = max_total - used_total
        if room <= 0:
            break
        if len(section) > room:
            section = _truncate_block(section, room)
        parts.append(section)
        used_total += len(section) + 2

    if not parts:
        return _truncate_block(text, max_total)
    return "\n\n".join(parts).strip()


def build_extraction_prompt(
    program_url: str,
    main_text: str,
    attachment_blocks: list[tuple[str, str]],
    retrieval_context: str | None = None,
) -> str:
    """
    Call Groq to produce structured fields for oei_programs.

    ``retrieval_context``: RAG excerpts from the program page text plus attachment
    bodies (already capped server-side). ``attachment_blocks`` are short heads
    from the same attachments for table-of-contents style context.
    """
    parts = [
        f"Program page URL: {program_url}\n",
        "--- Main page text (excerpt) ---\n",
        main_text[:MAIN_TEXT_CHAR_LIMIT],
    ]
    if retrieval_context and retrieval_context.strip():
        parts.append(
            "\n--- Retrieved excerpts (RAG: program page + attachments) ---\n"
        )
        parts.append(
            _budget_rag_context_by_set(retrieval_context.strip(), RAG_CONTEXT_CHAR_LIMIT)
        )

    if attachment_blocks:
        parts.append("\n--- Attachment openings (first lines of each file) ---\n")
    for att_url, snippet in attachment_blocks[:MAX_ATTACHMENTS_IN_PROMPT]:
        parts.append(f"\n--- Attachment head: {att_url} ---\n")
        parts.append(snippet[:ATTACHMENT_HEAD_CHARS])
    parts.append("\n" + EXTRACTION_JSON_INSTRUCTIONS)

    return "".join(parts)


def run_extraction_prompt(prompt: str) -> tuple[str, dict]:
    client = GroqLLMClient()
    raw = client.complete(prompt, response_json_object=True)
    parsed = _parse_json_object(raw)
    return raw, parsed


def ai_extract_program(
    program_url: str,
    main_text: str,
    attachment_blocks: list[tuple[str, str]],
    retrieval_context: str | None = None,
) -> dict:
    prompt = build_extraction_prompt(
        program_url,
        main_text,
        attachment_blocks,
        retrieval_context=retrieval_context,
    )
    _raw, parsed = run_extraction_prompt(prompt)
    return parsed
