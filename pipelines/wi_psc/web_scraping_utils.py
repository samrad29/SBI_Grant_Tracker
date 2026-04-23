from __future__ import annotations

import hashlib
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

ALLOWED_NETLOCS = frozenset(
    {
        "psc.wi.gov",
        "www.psc.wi.gov",
        "apps.psc.wi.gov",
        "maps.psc.wi.gov",
    }
)

HIGH_VALUE_PATH_PREFIXES = (
    "/documents/oei/",
    "/pages/servicetype/oei/",
    "/pages/grantssystem",
    "/apps/grants",
    "/apps/dockets",
    "/pages/cmsdetail",
    "/erf/",
)

REQUEST_TIMEOUT_S = 30
MAX_RETRIES = 2
RETRY_BACKOFF_S = 1.5

# Caps for attachment pass (per program page)
MAX_CANDIDATE_LINKS = 40
MAX_ATTACHMENT_FETCHES = 8
MAX_DOWNLOAD_BYTES = 5_000_000
MAX_HTML_TEXT_CHARS = 24_000
MAX_PDF_PAGES = 20
MAX_PDF_TEXT_CHARS = 32_000
MAX_DOCX_TEXT_CHARS = 24_000

# RAG ingest: higher caps for chunking + embedding (still bounded by MAX_DOWNLOAD_BYTES).
RAG_MAX_HTML_TEXT_CHARS = 200_000
RAG_MAX_PDF_PAGES = 60
RAG_MAX_PDF_TEXT_CHARS = 400_000
RAG_MAX_DOCX_TEXT_CHARS = 200_000


def extract_clean_text(content_div):
    paragraphs = content_div.find_all(["p", "li", "td", "th"])
    text = "\n".join(p.get_text(" ", strip=True) for p in paragraphs if p.get_text(strip=True))
    return text


def normalize_text(text):
    collapsed = (
        text.lower()
        .strip()
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("\t", " ")
    )
    while "  " in collapsed:
        collapsed = collapsed.replace("  ", " ")
    return collapsed


def hash_webpage_text(webpage_text: str) -> str:
    """SHA-256 of UTF-8 normalized page text (for change detection)."""
    normalized = normalize_text(webpage_text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def hash_attachment_text(text: str) -> str:
    """Content hash for attachment or extracted program-page text (same normalization as page hash)."""
    return hash_webpage_text(text)


def fix_embedded_absolute_url(url: str) -> str:
    """Repair hrefs like https://psc.wi.gov/.../https://apps.psc.wi.gov/..."""
    for scheme in ("https://", "http://"):
        if url.count(scheme) <= 1:
            continue
        last = url.rfind(scheme)
        if last > len(scheme):
            return url[last:]
    return url


def fetch_html(url: str, session: requests.Session | None = None) -> str:
    sess = session or requests.Session()
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = sess.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=REQUEST_TIMEOUT_S,
                allow_redirects=True,
            )
            resp.raise_for_status()
            return resp.text
        except (requests.RequestException, OSError) as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_S * (attempt + 1))
            else:
                raise last_exc from None
    raise RuntimeError("unreachable")


def fetch_bytes(url: str, session: requests.Session | None = None) -> tuple[bytes, str | None]:
    sess = session or requests.Session()
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = sess.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=REQUEST_TIMEOUT_S,
                allow_redirects=True,
                stream=True,
            )
            resp.raise_for_status()
            chunks: list[bytes] = []
            total = 0
            for chunk in resp.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_DOWNLOAD_BYTES:
                    raise ValueError(f"response exceeds {MAX_DOWNLOAD_BYTES} bytes: {url}")
                chunks.append(chunk)
            data = b"".join(chunks)
            ctype = resp.headers.get("Content-Type")
            return data, ctype
        except (requests.RequestException, OSError, ValueError) as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_S * (attempt + 1))
            else:
                raise last_exc from None
    raise RuntimeError("unreachable")


def parse_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def _largest_text_div(soup: BeautifulSoup):
    best = None
    best_len = 0
    for div in soup.find_all("div"):
        t = div.get_text(" ", strip=True)
        n = len(t)
        if n > best_len:
            best_len = n
            best = div
    return best


def extract_main_content(soup: BeautifulSoup) -> str:
    root = (
        soup.select_one("#DeltaPlaceHolderMain")
        or soup.select_one("#MainContent")
        or soup.select_one("#content")
        or soup.select_one("main")
        or soup.select_one('[role="main"]')
    )
    if root is None:
        root = _largest_text_div(soup) or soup.body
    if root is None:
        return ""
    text = extract_clean_text(root)
    if not text.strip():
        text = root.get_text("\n", strip=True)
    return text.strip()


def _is_candidate_link(abs_url: str) -> bool:
    try:
        fixed = fix_embedded_absolute_url(abs_url.strip())
        parsed = urlparse(fixed)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    path_lower = (parsed.path or "").lower()
    netloc = (parsed.netloc or "").lower()
    if netloc in ALLOWED_NETLOCS:
        if netloc == "maps.psc.wi.gov":
            return True
        if any(path_lower.startswith(p) for p in HIGH_VALUE_PATH_PREFIXES):
            return True
        if path_lower.endswith((".pdf", ".docx", ".xlsx", ".xls", ".doc")):
            return True
    return False


def extract_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href_raw = a.get("href")
        if not isinstance(href_raw, str):
            continue
        href = href_raw.strip()
        if not href or href.startswith("#"):
            continue
        if href.lower().startswith("mailto:"):
            continue
        joined = urljoin(base_url, href)
        fixed = fix_embedded_absolute_url(joined)
        if fixed not in seen:
            seen.add(fixed)
            out.append(fixed)
    return out


def extract_candidate_link_records(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    """
    Unique candidate (url, anchor_text) pairs in document order.

    Anchor text is taken from the first <a> that introduces each URL. Used for
    LLM ranking of which links to fetch under a hard cap.
    """
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href_raw = a.get("href")
        if not isinstance(href_raw, str):
            continue
        href = href_raw.strip()
        if not href or href.startswith("#"):
            continue
        if href.lower().startswith("mailto:"):
            continue
        joined = urljoin(base_url, href)
        fixed = fix_embedded_absolute_url(joined)
        if not _is_candidate_link(fixed):
            continue
        if fixed in seen:
            continue
        seen.add(fixed)
        anchor = a.get_text(" ", strip=True)
        if len(anchor) > 400:
            anchor = anchor[:397] + "..."
        out.append((fixed, anchor))
        if len(out) >= MAX_CANDIDATE_LINKS:
            break
    return out


def filter_candidate_links(links: list[str]) -> list[str]:
    candidates = [u for u in links if _is_candidate_link(u)]
    return candidates[:MAX_CANDIDATE_LINKS]


def prioritize_candidate_links(urls: list[str]) -> list[str]:
    """
    Re-order so high-signal URLs are fetched first and appear first in LLM input.

    ERF viewdoc pages (formal PSC filings) and OEI document paths should win over
    generic portal pages (e.g. ERF home, grants landing) when attachment slots
    or the AI prompt only include the first N snippets.
    """
    if not urls:
        return []

    def bucket(u: str) -> int:
        ul = u.lower()
        if "viewdoc" in ul and "docid=" in ul:
            return 0
        if "/documents/oei/" in ul and ul.endswith((".pdf", ".docx", ".xlsx", ".xls", ".doc")):
            return 1
        if "/documents/oei/" in ul:
            return 2
        if "/apps/dockets" in ul or "/apps/grants" in ul:
            return 3
        return 4

    return sorted(urls, key=lambda u: (bucket(u), urls.index(u)))


def _truncate(s: str, max_chars: int) -> str:
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 3] + "..."


def fetch_document_text(url: str, session: requests.Session | None = None) -> str:
    """Fetch a linked document and return plain text (truncated), or empty on failure."""
    sess = session or requests.Session()
    try:
        data, ctype = fetch_bytes(url, sess)
    except Exception:
        return ""
    path = urlparse(url).path.lower()
    ctype_l = (ctype or "").lower()

    if path.endswith(".pdf") or "application/pdf" in ctype_l:
        return _pdf_to_text(data, MAX_PDF_PAGES, MAX_PDF_TEXT_CHARS)
    if path.endswith(".docx") or "wordprocessingml" in ctype_l:
        return _docx_to_text(data, MAX_DOCX_TEXT_CHARS)
    if path.endswith((".html", ".htm")) or "text/html" in ctype_l:
        soup = BeautifulSoup(data, "html.parser")
        t = soup.get_text("\n", strip=True)
        return _truncate(t, MAX_HTML_TEXT_CHARS)
    if "text/plain" in ctype_l:
        try:
            t = data.decode("utf-8", errors="replace")
        except Exception:
            return ""
        return _truncate(t, MAX_HTML_TEXT_CHARS)
    return ""


def fetch_attachment_full_text(url: str, session: requests.Session | None = None) -> str:
    """
    Fetch attachment body and extract plain text using RAG-sized caps (for chunking).
    """
    sess = session or requests.Session()
    try:
        data, ctype = fetch_bytes(url, sess)
    except Exception:
        return ""
    path = urlparse(url).path.lower()
    ctype_l = (ctype or "").lower()

    if path.endswith(".pdf") or "application/pdf" in ctype_l:
        return _pdf_to_text(data, RAG_MAX_PDF_PAGES, RAG_MAX_PDF_TEXT_CHARS)
    if path.endswith(".docx") or "wordprocessingml" in ctype_l:
        return _docx_to_text(data, RAG_MAX_DOCX_TEXT_CHARS)
    if path.endswith((".html", ".htm")) or "text/html" in ctype_l:
        soup = BeautifulSoup(data, "html.parser")
        t = soup.get_text("\n", strip=True)
        return _truncate(t, RAG_MAX_HTML_TEXT_CHARS)
    if "text/plain" in ctype_l:
        try:
            t = data.decode("utf-8", errors="replace")
        except Exception:
            return ""
        return _truncate(t, RAG_MAX_HTML_TEXT_CHARS)
    return ""


def _pdf_to_text(data: bytes, max_pages: int, max_chars: int) -> str:
    try:
        from pypdf import PdfReader
        from io import BytesIO

        reader = PdfReader(BytesIO(data))
        parts: list[str] = []
        total_chars = 0
        for i, page in enumerate(reader.pages):
            if i >= max_pages:
                break
            try:
                chunk = page.extract_text() or ""
            except Exception:
                chunk = ""
            if not chunk.strip():
                continue
            parts.append(chunk)
            total_chars += len(chunk)
            if total_chars >= max_chars:
                break
        text = "\n".join(parts)
        return _truncate(text, max_chars)
    except Exception:
        return ""


def _docx_to_text(data: bytes, max_chars: int) -> str:
    try:
        from io import BytesIO

        from docx import Document

        doc = Document(BytesIO(data))
        paras = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
        text = "\n".join(paras)
        return _truncate(text, max_chars)
    except Exception:
        return ""


def collect_attachment_snippets(
    candidate_urls: list[str],
    session: requests.Session | None = None,
) -> list[tuple[str, str]]:
    """Return list of (url, text_snippet) for successfully parsed attachments."""
    sess = session or requests.Session()
    results: list[tuple[str, str]] = []
    for url in candidate_urls[:MAX_ATTACHMENT_FETCHES]:
        snippet = fetch_document_text(url, sess)
        if snippet.strip():
            results.append((url, snippet))
    return results
