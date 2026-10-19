"""The Receita Federal public share (Nextcloud WebDAV) and how to tell its failure modes apart.

What we know about the source (datalabs ADR 0009, re-measured Oct/2026):
* months live under /Dados/Cadastros/CNPJ/<YYYY-MM>; the public link is configuration, not a secret
* PROPFIND (folder listing) answers 200 + empty body for ANY path: useless, so we never list
* a 1-byte ranged GET tells the truth: 206/200 present, 401/403 link rejected, 404 missing
* the server is flaky under bursts: the same file can answer 404 then 200 minutes later, so 404 is retried too
"""
from __future__ import annotations

import re
from urllib.parse import quote

from pipelines.commons.http import DEFAULT_RETRYABLE, Fetcher
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.settings import get_settings
from pipelines.rfb.contract import PROBE_FILE

HOST = "https://arquivos.receitafederal.gov.br"
WEBDAV = f"{HOST}/public.php/webdav"
DEFAULT_SHARE_URL = "https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK"
DEFAULT_SHARE_DIR = "/Dados/Cadastros/CNPJ"
RETRYABLE = DEFAULT_RETRYABLE | {404}


class SourceError(RuntimeError):
    """A named source failure. `event` is the stable code logged and stored in ctrl.download_ref.last_error."""

    def __init__(self, event: str, message: str):
        super().__init__(message)
        self.event = event


def share_token(url: str | None = None) -> str:
    """Token of the public link: RFB_SHARE_URL (paste the browser link as is) or the built-in default."""
    url = (url if url is not None else get_settings().get("RFB_SHARE_URL", DEFAULT_SHARE_URL) or DEFAULT_SHARE_URL)
    m = re.search(r"/s/([A-Za-z0-9]+)", url.strip().strip("'\""))
    if not m:
        raise SourceError("source_config_invalid", f"RFB_SHARE_URL has no '/s/<token>': {url}")
    return m.group(1)


def share_dir() -> str:
    d = (get_settings().get("RFB_SHARE_DIR", DEFAULT_SHARE_DIR) or DEFAULT_SHARE_DIR).strip()
    return "/" + d.strip("/")


def file_url(base_dir: str, ref: RefMonth, name: str) -> str:
    return f"{WEBDAV}{quote(base_dir)}/{ref.iso}/{name}"


def make_fetcher(token: str, **kw) -> Fetcher:
    s = get_settings()
    return Fetcher(
        auth=(token, ""),
        retries=int(s.get("RFB_HTTP_RETRIES", "6")),
        pause_s=float(s.get("RFB_REQUEST_INTERVAL_S", "1.0")),
        backoff_max_s=float(s.get("RFB_BACKOFF_MAX_S", "60")),
        retryable=RETRYABLE,
        **kw,
    )


def probe(fetcher: Fetcher, base_dir: str, ref: RefMonth) -> int:
    return fetcher.status("GET", file_url(base_dir, ref, PROBE_FILE), headers={"Range": "bytes=0-0"}, stream=True)


def check_source(fetcher: Fetcher, base_dir: str, ref: RefMonth) -> None:
    """Return if the month is there; otherwise raise SourceError naming the real cause."""
    status = probe(fetcher, base_dir, ref)
    if status in (200, 206):
        return
    if status == 0:
        raise SourceError("source_unreachable", f"no answer from {HOST} after {fetcher.retries} attempts")
    if status in (401, 403):
        raise SourceError("source_auth_failed", f"HTTP {status}: the share link was rejected; the Receita may "
                                                "have re-created it - paste the new link in RFB_SHARE_URL")
    if status == 404:
        prev = ref.prev()
        if probe(fetcher, base_dir, prev) in (200, 206):
            raise SourceError("source_month_not_published", f"{ref} is not published under '{base_dir}' yet "
                                                            f"({prev} is)")
        raise SourceError("source_dir_not_found", f"neither {ref} nor {prev} found under '{base_dir}': check "
                                                  "RFB_SHARE_DIR / RFB_SHARE_URL")
    raise SourceError("source_unreachable", f"HTTP {status} from {HOST} after {fetcher.retries} attempts")
