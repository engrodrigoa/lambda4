"""HTTP against flaky public sources: one session, retries with exponential backoff, resumable-safe downloads.

A download is written to `<dest>.part` and only renamed to `dest` after it is complete (Content-Length matches) and
passes the caller's validator (e.g. "is a readable ZIP"). The SHA-256 is computed while streaming, so the file is
never read twice.
"""
from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import requests

DEFAULT_RETRYABLE = frozenset({408, 425, 429, 500, 502, 503, 504})
CHUNK = 1024 * 1024


@dataclass
class DownloadResult:
    ok: bool
    status: int            # last HTTP status (0 = no answer)
    reason: str            # empty when ok
    bytes: int = 0
    expected_bytes: int | None = None
    sha256: str | None = None
    attempts: int = 0
    seconds: float = 0.0


class Fetcher:
    def __init__(self, *, auth: tuple[str, str] | None = None, session: requests.Session | None = None,
                 retries: int = 6, pause_s: float = 1.0, backoff_max_s: float = 60.0,
                 retryable: frozenset[int] = DEFAULT_RETRYABLE, timeout: tuple[float, float] = (30, 300),
                 sleep: Callable[[float], None] = time.sleep):
        self.auth = auth
        self.session = session or requests.Session()
        self.retries = max(1, retries)
        self.pause_s = pause_s
        self.backoff_max_s = backoff_max_s
        self.retryable = retryable
        self.timeout = timeout
        self.sleep = sleep

    def _wait(self, attempt: int) -> None:
        self.sleep(min(2 ** attempt, self.backoff_max_s))

    def pause(self) -> None:
        """Polite pause between files (bursts make some sources flaky)."""
        if self.pause_s > 0:
            self.sleep(self.pause_s)

    def status(self, method: str, url: str, **kw) -> int:
        """Status that survives flakiness: retries anything retryable and returns the last answer (0 = none)."""
        status = 0
        for attempt in range(self.retries):
            try:
                with self.session.request(method, url, auth=self.auth, timeout=self.timeout[0],
                                          allow_redirects=False, **kw) as r:
                    status = r.status_code
            except requests.RequestException:
                status = 0
            if status != 0 and status not in self.retryable:
                return status
            if attempt < self.retries - 1:
                self._wait(attempt)
        return status

    def download(self, url: str, dest: Path, validate: Callable[[Path], bool] | None = None) -> DownloadResult:
        dest = Path(dest)
        tmp = dest.with_name(dest.name + ".part")
        result = DownloadResult(ok=False, status=0, reason="")
        t0 = time.monotonic()
        for attempt in range(self.retries):
            result.attempts = attempt + 1
            try:
                with self.session.get(url, auth=self.auth, stream=True, timeout=self.timeout) as r:
                    result.status = r.status_code
                    ctype = r.headers.get("Content-Type", "")
                    if r.status_code == 200 and "html" not in ctype:
                        expected = int(r.headers.get("Content-Length") or 0) or None
                        result.expected_bytes = expected
                        digest, written = hashlib.sha256(), 0
                        with tmp.open("wb") as f:
                            for chunk in r.iter_content(chunk_size=CHUNK):
                                f.write(chunk)
                                digest.update(chunk)
                                written += len(chunk)
                        if expected and written != expected:
                            result.reason = f"truncated: {written} of {expected} bytes"
                        elif validate is not None and not validate(tmp):
                            result.reason = "validation failed"
                        else:
                            os.replace(tmp, dest)
                            result.ok, result.reason = True, ""
                            result.bytes, result.sha256 = written, digest.hexdigest()
                            result.seconds = time.monotonic() - t0
                            return result
                    else:
                        result.reason = f"HTTP {r.status_code} {ctype}".strip()
            except requests.RequestException as exc:
                result.status, result.reason = 0, f"{type(exc).__name__}: {exc}"[:300]
            if tmp.exists():
                tmp.unlink()
            # definitive answers (401/403/...) will not change with retries
            if result.status not in self.retryable and result.status not in (0, 200):
                break
            if attempt < self.retries - 1:
                self._wait(attempt)
        result.seconds = time.monotonic() - t0
        return result


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()
