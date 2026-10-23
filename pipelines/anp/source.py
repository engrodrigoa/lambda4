"""ANP portal: fetch the page, turn its links into the catalog of SourceFiles (pipelines/anp/contract.py).

The page is the only listing the source offers. Parsing is by URL pattern + link text, never by page layout
(headings, lists), so a redesign of the page does not silently change what is classified.
"""
from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass, field
from datetime import date, datetime

import requests

from pipelines.anp.contract import DATA_LINK, PAGE_URL, SourceFile, classify, is_ignored, strip_accents
from pipelines.commons.http import Fetcher
from pipelines.commons.settings import get_settings

DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) lambda4-anp-pipeline/1.0"
_WEEKLY_DATE = re.compile(r"(?:quatro|4)\s+ultimas\s+semanas.{0,120}?atualizad[oa]\s+em\s*(\d{1,2}/\d{1,2}/\d{4})",
                          re.IGNORECASE | re.DOTALL)


class SourceError(Exception):
    """The page could not be fetched (HTTP status or no answer)."""


@dataclass
class Catalog:
    files: list[SourceFile] = field(default_factory=list)
    unclassified: list[tuple[str, str]] = field(default_factory=list)   # (url, link text) data links not understood
    duplicates: list[tuple[str, str]] = field(default_factory=list)     # (file_key, url) second link to the same key
    ignored: list[str] = field(default_factory=list)
    weekly_date: date | None = None


def page_url() -> str:
    return get_settings().get("ANP_PAGE_URL", PAGE_URL)


def make_fetcher() -> Fetcher:
    s = get_settings()
    session = requests.Session()
    session.headers["User-Agent"] = s.get("ANP_USER_AGENT", DEFAULT_USER_AGENT)
    # gov.br resets connections under bursts: sequential, polite pause between files, generous retries
    return Fetcher(session=session, retries=6, pause_s=float(s.get("ANP_HTTP_PAUSE_S", "1.5")),
                   timeout=(30, 600))


def fetch_page(fetcher: Fetcher, url: str | None = None) -> str:
    url = url or page_url()
    last = "no answer"
    for attempt in range(fetcher.retries):
        try:
            r = fetcher.session.get(url, timeout=fetcher.timeout[0])
            if r.status_code == 200:
                return r.text
            last = f"HTTP {r.status_code}"
            if r.status_code not in fetcher.retryable:
                break
        except requests.RequestException as exc:
            last = f"{type(exc).__name__}: {exc}"[:300]
        if attempt < fetcher.retries - 1:
            fetcher._wait(attempt)
    raise SourceError(f"source page unavailable ({last}): {url}")


def weekly_date_of(text: str) -> date | None:
    m = _WEEKLY_DATE.search(strip_accents(text))
    return datetime.strptime(m.group(1), "%d/%m/%Y").date() if m else None


def parse_page(html: str, base_url: str = PAGE_URL) -> Catalog:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    catalog = Catalog(weekly_date=weekly_date_of(soup.get_text(" ", strip=True)))
    seen_urls: set[str] = set()
    by_key: dict[str, SourceFile] = {}
    for a in soup.find_all("a", href=True):
        url = urllib.parse.urljoin(base_url, a["href"].strip())
        if not DATA_LINK.search(urllib.parse.urlparse(url).path) or url in seen_urls:
            continue
        seen_urls.add(url)
        if is_ignored(url):
            catalog.ignored.append(url)
            continue
        text = a.get_text(" ", strip=True)
        item = classify(url, text, catalog.weekly_date)
        if item is None:
            catalog.unclassified.append((url, text))
        elif item.file_key in by_key:
            catalog.duplicates.append((item.file_key, url))
        else:
            by_key[item.file_key] = item
    catalog.files = sorted(by_key.values(), key=lambda f: f.file_key)
    return catalog
