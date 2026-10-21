"""rfb00 - preflight: is the platform healthy enough to run the pipeline? (no Spark, a few seconds)

    python -m pipelines.rfb.rfb00_preflight

Every probe is written to audit.health_check (component, status, latency, detail) and logged; the verdict follows
pipelines/commons/dq.py: S3, Spark master, metastore and the data mounts are BLOCK (the run stops before touching
data), free space and the observability stack are WARN (the run continues, degraded and visible).
"""
from __future__ import annotations

import os
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from pipelines.commons.audit import get_sink, utcnow_iso
from pipelines.commons.lake import spark_defaults
from pipelines.commons.logger import log_event
from pipelines.commons.settings import get_settings
from pipelines.commons.task import TaskRun, run_task

PIPELINE = "rfb"
GIB = 1024 ** 3
FREE_FLOORS_GIB = {"scratch": 40, "datalake": 30, "landing": 15}


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
                                      "pipeline": PIPELINE, "component": component, "status": status,
                                      "latency_ms": latency_ms, "detail": detail})
    level = 20 if ok else 30
    log_event(task.log, "health_check", f"{component}: {status} ({latency_ms:.0f} ms)", level=level,
              component=component, status=status, latency_ms=latency_ms,
              **{f"detail_{k}": v for k, v in detail.items()})
    return status


# ----------------------------------------------------------------------------------------------- probes
def s3_probe(conf: dict[str, str]) -> dict[str, Any]:
    import boto3
    from botocore.config import Config

    s = get_settings()
    client = boto3.client("s3", endpoint_url=conf["spark.hadoop.fs.s3a.endpoint"],
                          aws_access_key_id=conf["spark.hadoop.fs.s3a.access.key"],
                          aws_secret_access_key=conf["spark.hadoop.fs.s3a.secret.key"], region_name="us-east-1",
                          config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 1}))
    have = {b["Name"] for b in client.list_buckets()["Buckets"]}
    need = {s.bucket_bronze, s.bucket_silver, s.bucket_gold, s.bucket_ctrl, s.bucket_audit}
    return {"_ok": need <= have, "missing_buckets": sorted(need - have), "buckets": len(have)}


def spark_probe() -> dict[str, Any]:
    url = os.environ.get("LAMBDA_SPARK_MASTER_UI", "http://127.0.0.1:8088") + "/json/"
    d = requests.get(url, timeout=5).json()
    alive = [w for w in d.get("workers", []) if w.get("state") == "ALIVE"]
    return {"_ok": d.get("status") == "ALIVE" and bool(alive), "status": d.get("status"), "workers": len(alive),
            "cores": sum(w["cores"] for w in alive), "cores_used": sum(w["coresused"] for w in alive),
            "memory_mb": sum(w["memory"] for w in alive), "active_apps": len(d.get("activeapps", []))}


def metastore_probe(conf: dict[str, str]) -> dict[str, Any]:
    import psycopg

    url = urlparse(conf["spark.hadoop.javax.jdo.option.ConnectionURL"].removeprefix("jdbc:"))
    with psycopg.connect(host=url.hostname, port=url.port or 5432, dbname=url.path.lstrip("/"),
                         user=conf["spark.hadoop.javax.jdo.option.ConnectionUserName"],
                         password=conf["spark.hadoop.javax.jdo.option.ConnectionPassword"], connect_timeout=5) as pg:
        tables = pg.execute('SELECT count(*) FROM "TBLS"').fetchone()[0]
        version = pg.execute('SELECT "SCHEMA_VERSION" FROM "VERSION"').fetchone()[0]
    return {"tables": tables, "schema_version": version}


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


def main(argv: list[str] | None = None) -> int:
    def body(task: TaskRun) -> None:
        s = get_settings()
        conf = spark_defaults()
        landing_path = Path(urlparse(s.landing_uri).path)
        results: dict[str, str] = {}

        # BLOCK components: record every probe first, then evaluate (so the audit shows the whole picture)
        blocking = {
            "s3": lambda: s3_probe(conf),
            "spark_master": spark_probe,
            "metastore": lambda: metastore_probe(conf),
            "datalake_mounted": lambda: mount_probe(Path("/mnt/datalake")),
            "landing_mounted": lambda: mount_probe(landing_path),
        }
        verdict = {}
        for name, fn in blocking.items():
            ok, ms, detail = probe(fn)
            results[name] = record(task, name, ok, ms, detail)
            verdict[name] = (ok, detail)

        disks = {"scratch": s.scratch_dir, "datalake": Path("/mnt/datalake"), "landing": landing_path}
        for disk, path in disks.items():
            ok, ms, detail = probe(lambda p=path, d=disk: free_probe(p, FREE_FLOORS_GIB[d]))
            results[f"disk:{disk}"] = record(task, f"disk:{disk}", ok, ms, detail, warn_only=True)
            task.check("preflight.free_space", ok, observed=detail.get("free_gib"), threshold=FREE_FLOORS_GIB[disk],
                       message=f"{disk}: GiB free", disk=disk)

        observability = {
            "audit_db": audit_db_probe,
            "loki": lambda: http_ready("http://127.0.0.1:3100/ready"),
            "pushgateway": lambda: http_ready(
                (s.pushgateway_url if s.pushgateway_url.startswith("http") else f"http://{s.pushgateway_url}")
                + "/-/ready"),
        }
        for name, fn in observability.items():
            ok, ms, detail = probe(fn)
            results[name] = record(task, name, ok, ms, detail, warn_only=True)
            task.check("preflight.observability", ok, message=f"{name}: {'OK' if ok else detail}", component=name)

        task.metric(health=results)
        for name, (ok, detail) in verdict.items():
            task.check(f"preflight.{name}", ok, message=f"{name}: {'OK' if ok else detail}")

    return run_task(body, pipeline=PIPELINE, task_id="rfb00_preflight", layer="other", params={})


if __name__ == "__main__":
    sys.exit(main())
