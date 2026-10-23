"""anp00 - preflight: can the ANP pipeline run? (no Spark, no SeaweedFS: Postgres + landing disk + the portal)

    python -m pipelines.anp.anp00_preflight

Every probe goes to audit.health_check and the logs. BLOCK (pipelines/commons/dq.py): Postgres with the ctrl and
anp_bronze tables (infra/postgres/anp_schema.sql applied), landing disk mounted. WARN: free space, the source page,
the observability stack.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from pipelines.anp import source
from pipelines.anp.contract import PIPELINE
from pipelines.commons.health import free_probe, mount_probe, observability_probes, probe, record
from pipelines.commons.settings import get_settings
from pipelines.commons.task import TaskRun, run_task

FREE_FLOORS_GIB = {"landing": 10, "scratch": 5}
REQUIRED_TABLES = ("ctrl.source_file", "ctrl.source_file_event", "anp_bronze.precos_raw",
                   "anp_bronze.precos_quarantine")


def postgres_probe() -> dict[str, Any]:
    from pipelines.commons import pg

    with pg.connect() as conn:
        found = {r[0] for r in conn.execute(
            # pg_tables lists partitioned parents too (relkind p)
            "SELECT schemaname || '.' || tablename FROM pg_tables WHERE schemaname IN ('ctrl', 'anp_bronze')")}
        rows = conn.execute("SELECT count(*) FROM ctrl.source_file WHERE pipeline = %s", (PIPELINE,)).fetchone()[0] \
            if "ctrl.source_file" in found else None
    missing = [t for t in REQUIRED_TABLES if t not in found]
    return {"_ok": not missing, "missing_tables": missing, "ctrl_files": rows,
            "hint": "apply infra/postgres/anp_schema.sql" if missing else None}


def source_probe() -> dict[str, Any]:
    r = requests.get(source.page_url(), timeout=15,
                     headers={"User-Agent": get_settings().get("ANP_USER_AGENT", source.DEFAULT_USER_AGENT)})
    return {"_ok": r.status_code == 200, "http_status": r.status_code, "bytes": len(r.content)}


def main(argv: list[str] | None = None) -> int:
    def body(task: TaskRun) -> None:
        s = get_settings()
        landing_path = Path(urlparse(s.landing_uri).path)
        results: dict[str, str] = {}

        blocking = {"postgres": postgres_probe, "landing_mounted": lambda: mount_probe(landing_path)}
        verdict = {}
        for name, fn in blocking.items():
            ok, ms, detail = probe(fn)
            results[name] = record(task, name, ok, ms, detail)
            verdict[name] = (ok, detail)

        for disk, path in {"landing": landing_path, "scratch": s.scratch_dir}.items():
            ok, ms, detail = probe(lambda p=path, d=disk: free_probe(p, FREE_FLOORS_GIB[d]))
            results[f"disk:{disk}"] = record(task, f"disk:{disk}", ok, ms, detail, warn_only=True)
            task.check("preflight.free_space", ok, observed=detail.get("free_gib"), threshold=FREE_FLOORS_GIB[disk],
                       message=f"{disk}: GiB free", disk=disk)

        ok, ms, detail = probe(source_probe)
        results["source"] = record(task, "source", ok, ms, detail, warn_only=True)
        task.check("preflight.source_reachable", ok, message=f"ANP page: {'OK' if ok else detail}")

        for name, fn in observability_probes().items():
            ok, ms, detail = probe(fn)
            results[name] = record(task, name, ok, ms, detail, warn_only=True)
            task.check("preflight.observability", ok, message=f"{name}: {'OK' if ok else detail}", component=name)

        task.metric(health=results)
        for name, (ok, detail) in verdict.items():
            task.check(f"preflight.{name}", ok, message=f"{name}: {'OK' if ok else detail}")

    return run_task(body, pipeline=PIPELINE, task_id="anp00_preflight", layer="other", params={})


if __name__ == "__main__":
    sys.exit(main())
