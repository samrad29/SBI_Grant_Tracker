from __future__ import annotations

import json
import time
import traceback

import requests
from dotenv import load_dotenv

load_dotenv()

from db.db_util import get_db_connection, is_test_mode
from jobs.log_utils import log

from pipelines.wi_psc.ai_utils import (
    build_extraction_prompt,
    rank_candidate_links_for_fetch,
    run_extraction_prompt,
)
from pipelines.wi_psc.db_util import (
    get_stored_hash,
    init_tables,
    save_ai_extraction,
    save_ai_extraction_log,
)
from pipelines.wi_psc.rag_util import (
    DEFAULT_RAG_QUERY_SETS,
    ensure_indexed,
    get_embedding_model_name,
    retrieve_for_program,
)
from pipelines.wi_psc.web_scraping_utils import (
    MAX_ATTACHMENT_FETCHES,
    collect_attachment_snippets,
    extract_candidate_link_records,
    extract_main_content,
    fetch_attachment_full_text,
    fetch_html,
    hash_webpage_text,
    parse_html,
    prioritize_candidate_links,
)

PROGRAM_URLS = [
    "https://psc.wi.gov/Pages/ServiceType/OEI/EnergyInnovationGrantProgram.aspx",
    "https://psc.wi.gov/Pages/ServiceType/OEI/RuralEnergyStartupProgram.aspx",
    "https://psc.wi.gov/Pages/ServiceType/OEI/GridResilience.aspx",
    "https://psc.wi.gov/Pages/ServiceType/OEI/TrainingResidentialEnergyContractors.aspx",
]


def wis_psc_main(conn, job_id):
    """
    Phase 1: scrape fixed OEI program pages, detect changes by hash, extract text
    and linked documents, RAG over program page + attachments, Groq extraction.
    """
    embed_model = get_embedding_model_name()
    session = requests.Session()
    stats = {
        "grants_processed": 0,
        "new_grants": 0,
        "updated_grants": 0,
    }

    for url in PROGRAM_URLS:
        print(f"[fetch] {url}")
        try:
            html = fetch_html(url)
        except Exception:
            traceback.print_exc()
            continue

        soup = parse_html(html)
        main_text = extract_main_content(soup)
        content_hash = hash_webpage_text(main_text)

        prev = get_stored_hash(conn, url)
        if prev == content_hash:
            log(conn, job_id, f"[skip] unchanged content hash for {url}", "INFO")
            stats["grants_processed"] += 1
            continue
        elif prev is None:
            stats["new_grants"] += 1

        link_records = extract_candidate_link_records(soup, url)
        urls_only = [u for u, _ in link_records]
        heuristic = prioritize_candidate_links(urls_only)
        try:
            candidates = rank_candidate_links_for_fetch(
                url,
                link_records,
                MAX_ATTACHMENT_FETCHES,
                heuristic,
            )
        except Exception:
            traceback.print_exc()
            candidates = heuristic[:MAX_ATTACHMENT_FETCHES]
        log(conn, job_id, f"[links] {len(link_records)} candidates")
        log(conn, job_id, f"fetch order ({len(candidates)}): LLM-ranked with heuristic fallback", "INFO")
        
        time.sleep(2.0)

        document_ids: list[int] = []
        rag_context = ""
        try:
            page_doc_id = ensure_indexed(conn, url, main_text, embed_model)
            if page_doc_id is not None:
                document_ids.append(page_doc_id)

            for att_url in candidates:
                full_txt = fetch_attachment_full_text(att_url, session)
                did = ensure_indexed(conn, att_url, full_txt, embed_model)
                if did is not None:
                    document_ids.append(did)

            unique_doc_ids: list[int] = []
            seen_ids: set[int] = set()
            for d in document_ids:
                if d not in seen_ids:
                    seen_ids.add(d)
                    unique_doc_ids.append(d)

            if unique_doc_ids:
                rag_context = retrieve_for_program(
                    conn,
                    url,
                    unique_doc_ids,
                    query_sets=DEFAULT_RAG_QUERY_SETS,
                    embedding_model=embed_model,
                )
                time.sleep(1.5)
        except Exception:
            log(conn, job_id, "[rag] indexing or retrieval failed; continuing without RAG context", "ERROR")
            traceback.print_exc()
            rag_context = ""

        attachment_blocks = collect_attachment_snippets(candidates)
        log(conn, job_id, f"[attachments] short heads from {len(attachment_blocks)} URLs", "INFO")

        prompt = build_extraction_prompt(
            url,
            main_text,
            attachment_blocks,
            retrieval_context=rag_context or None,
        )
        raw_response: str | None = None
        structured: dict | None = None
        try:
            raw_response, structured = run_extraction_prompt(prompt)
        except Exception:
            log(conn, job_id, f"[ai] failed for {url}", "ERROR")
            traceback.print_exc()
            try:
                save_ai_extraction_log(
                    conn,
                    url=url,
                    prompt=prompt,
                    raw_response=raw_response,
                    extracted_payload=None,
                )
            except Exception:
                log(conn, job_id, "[ai-log] failed to persist failure log", "ERROR")
                traceback.print_exc()
            continue

        try:
            save_ai_extraction_log(
                conn,
                url=url,
                prompt=prompt,
                raw_response=raw_response,
                extracted_payload=structured,
            )
        except Exception:
            log(conn, job_id, "[ai-log] failed to persist extraction log", "ERROR")
            traceback.print_exc()

        save_ai_extraction(conn, structured or {}, url, content_hash)
        log(conn, job_id, f"[saved] {url}", "INFO")
        stats["grants_processed"] += 1
        time.sleep(2.5)

        return stats


ENERGY_INNOVATION_URL = PROGRAM_URLS[0]


def _preview(text: str | None, max_chars: int = 700) -> str:
    if not text:
        return "<empty>"
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def print_latest_ai_log(conn, url: str) -> None:
    """
    Print the latest ai_extraction_logs row for a URL with compact previews.
    """
    row = conn.execute(
        """
        SELECT id, url, created_at, prompt, raw_response, extracted_json
        FROM ai_extraction_logs
        WHERE url = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (url,),
    ).fetchone()
    if row is None:
        print(f"\n[ai-log] No ai_extraction_logs rows for:\n  {url}\n")
        return

    d = dict(row)
    parsed_preview = d.get("extracted_json")
    if parsed_preview:
        try:
            parsed_obj = json.loads(parsed_preview)
            parsed_preview = json.dumps(parsed_obj, indent=2)
        except (json.JSONDecodeError, TypeError):
            pass

    print("\n" + "=" * 72)
    print("Latest AI extraction log")
    print("=" * 72)
    print(f"\nlog_id:\n{d.get('id')}")
    print(f"\nurl:\n{d.get('url')}")
    print(f"\ncreated_at:\n{d.get('created_at')}")
    print(f"\nprompt_preview:\n{_preview(d.get('prompt'))}")
    print(f"\nraw_response_preview:\n{_preview(d.get('raw_response'))}")
    print(f"\nextracted_json_preview:\n{_preview(parsed_preview)}")
    print("\n" + "=" * 72 + "\n")


def print_program_details(conn, url: str) -> None:
    """Pretty-print one oei_programs row (JSON columns decoded for readability)."""
    row = conn.execute(
        "SELECT * FROM oei_programs WHERE url = %s",
        (url,),
    ).fetchone()
    if row is None:
        print(f"\n[view] No database row for:\n  {url}\n")
        return

    d = dict(row)
    for key in ("attachments", "elibilities"):
        raw = d.get(key)
        if raw:
            try:
                d[key] = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                pass

    print("\n" + "=" * 72)
    print("Energy Innovation Grant Program — stored row (oei_programs)")
    print("=" * 72)
    for key in (
        "url",
        "program_name",
        "program_status",
        "deadline_date",
        "estimated_funding",
        "estimated_funding_description",
        "description",
        "elibilities",
        "attachments",
        "webpage_text_hash",
        "updated_at",
    ):
        if key not in d:
            continue
        val = d[key]
        if isinstance(val, (dict, list)):
            val = json.dumps(val, indent=2)
        print(f"\n{key}:\n{val}")
    print("\n" + "=" * 72 + "\n")


