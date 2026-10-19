"""Landing zone: where raw source files arrive, addressed by LANDING_URI.

Layout: <root>/<source>/ref=<YYYYMM>/<file>. Only file:// is implemented; the URI keeps the door open for s3://
without touching pipeline code.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from pipelines.commons.refmonth import RefMonth
from pipelines.commons.settings import get_settings


@dataclass(frozen=True)
class LocalLanding:
    root: Path

    @property
    def uri(self) -> str:
        return self.root.as_uri()

    def month_dir(self, source: str, ref: RefMonth, create: bool = True) -> Path:
        d = self.root / source / f"ref={ref.yyyymm}"
        if create:
            d.mkdir(parents=True, exist_ok=True)
        return d

    def free_bytes(self) -> int:
        probe = self.root
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        return shutil.disk_usage(probe).free


def landing_for(uri: str | None = None) -> LocalLanding:
    uri = uri or get_settings().landing_uri
    parsed = urlparse(uri)
    if parsed.scheme in ("", "file"):
        return LocalLanding(Path(parsed.path if parsed.scheme else uri))
    raise NotImplementedError(f"LANDING_URI scheme '{parsed.scheme}' is not implemented yet (only file://): {uri}")
