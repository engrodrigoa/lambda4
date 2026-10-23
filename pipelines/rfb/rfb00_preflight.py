"""rfb00 - preflight: is the platform healthy enough to run the pipeline? (no Spark, a few seconds)

    python -m pipelines.rfb.rfb00_preflight

Every probe is written to audit.health_check (component, status, latency, detail) and logged; the verdict follows
pipelines/commons/dq.py: S3, Spark master, metastore and the data mounts are BLOCK (the run stops before touching
data), free space and the observability stack are WARN (the run continues, degraded and visible).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from pipelines.commons.health import free_probe, mount_probe, observability_probes, probe, record
from pipelines.commons.lake import spark_defaults
from pipelines.commons.settings import get_settings
from pipelines.commons.task import TaskRun, run_task

PIPELINE = "rfb"
FREE_FLOORS_GIB = {"scratch": 40, "datalake": 30, "landing": 15}


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

        observability = observability_probes()
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
