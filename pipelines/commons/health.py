"""Health probes shared by every preflight task (rfb00, anp00, ...).

A probe is a function returning a detail dict (`_ok: False` marks a failure); `probe()` times it and turns exceptions
into results, `record()` writes audit.health_check and logs. Severity is decided by the caller through dq.py.
"""
from __future__ import annotations

import os
import shutil
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests

from pipelines.commons.audit import get_sink, utcnow_iso
from pipelines.commons.logger import log_event
from pipelines.commons.settings import get_settings
from pipelines.commons.task import TaskRun

GIB = 1024 ** 3


def probe(fn: Callable[[], dict[str, Any]]) -> tuple[bool, float, dict[str, Any]]:
    t0 = time.monotonic()
    try:
        detail = fn() or {}
        ok = bool(detail.pop("_ok", True))
    except Exception as exc:  # noqa: BLE001 - a probe failure is a result, not a crash
        ok, detail = False, {"error": f"{type(exc).__name__}: {exc}"[:500]}
    return ok, round((time.monotonic() - t0) * 1000, 1), detail


def record(task: TaskRun, component: str, ok: bool, latency_ms: float, detail: dict[str, Any],
           warn_only: bool = False) -> str:
    status = "OK" if ok else ("WARN" if warn_only else "FAIL")
    get_sink().write("health_check", {"checked_at": utcnow_iso(), "run_id": task.ctx.run_id, "task_id": task.task_id,
                                      "pipeline": task.pipeline, "component": component, "status": status,
                                      "latency_ms": latency_ms, "detail": detail})
    level = 20 if ok else 30
    log_event(task.log, "health_check", f"{component}: {status} ({latency_ms:.0f} ms)", level=level,
              component=component, status=status, latency_ms=latency_ms,
              **{f"detail_{k}": v for k, v in detail.items()})
    return status


def audit_db_probe() -> dict[str, Any]:
    import psycopg

    with psycopg.connect(get_settings().audit_dsn) as pg:
        return {"task_runs": pg.execute("SELECT count(*) FROM audit.task_run").fetchone()[0]}


def http_ready(url: str) -> dict[str, Any]:
    r = requests.get(url, timeout=5)
    return {"_ok": r.status_code == 200, "http_status": r.status_code}


def mount_probe(path: Path) -> dict[str, Any]:
    probe_path = path
    while not probe_path.exists() and probe_path != probe_path.parent:
        probe_path = probe_path.parent
    mount = probe_path
    while not os.path.ismount(mount):
        mount = mount.parent
    return {"_ok": str(mount) != "/", "mountpoint": str(mount), "path": str(path)}


def free_probe(path: Path, floor_gib: float) -> dict[str, Any]:
    p = path
    while not p.exists() and p != p.parent:
        p = p.parent
    u = shutil.disk_usage(p)
    return {"_ok": u.free >= floor_gib * GIB, "free_gib": round(u.free / GIB, 1), "floor_gib": floor_gib,
            "used_pct": round(100 * u.used / u.total, 1), "path": str(path)}


def observability_probes() -> dict[str, Callable[[], dict[str, Any]]]:
    """Audit DB, logs, metrics and traces: WARN-only components (the run continues, degraded and visible)."""
    s = get_settings()
    pushgateway = s.pushgateway_url if s.pushgateway_url.startswith("http") else f"http://{s.pushgateway_url}"
    return {
        "audit_db": audit_db_probe,
        "loki": lambda: http_ready("http://127.0.0.1:3100/ready"),
        "pushgateway": lambda: http_ready(pushgateway + "/-/ready"),
        # traces: the OTLP collector (Alloy, on demand) and the trace store (Tempo, on demand)
        "alloy": lambda: http_ready("http://127.0.0.1:12345/-/ready"),
        "tempo": lambda: http_ready("http://127.0.0.1:3200/ready"),
    }
