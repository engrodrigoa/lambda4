"""Batch-job metrics pushed to the Prometheus Pushgateway (best effort, never raises).

One Pushgateway group per (pipeline, task). We use pushadd (HTTP POST): it replaces only the metric names sent in
this push, so `lambda_task_last_success_timestamp_seconds` survives a failed run (it is only sent on success) and
"time since last success" alerts keep working.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from pipelines.commons.settings import get_settings

_log = logging.getLogger("lambda.metrics")

JOB = "lambda_pipeline"


def push_task_metrics(*, pipeline: str, task_id: str, layer: str | None, status: str, duration_s: float,
                      counters: dict[str, Any], ref_month: int | None, warn_count: int, block_count: int) -> bool:
    s = get_settings()
    if not s.metrics_enabled:
        return False
    try:
        from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

        reg = CollectorRegistry()
        gauges: dict[str, Gauge] = {}
        lv = {"layer": layer or "other"}

        def gauge(name: str, doc: str, value: float, extra: dict[str, str] | None = None) -> None:
            extra = extra or {}
            if name not in gauges:  # label names are fixed per metric name
                gauges[name] = Gauge(name, doc, ["layer", *sorted(extra)], registry=reg)
            gauges[name].labels(**lv, **extra).set(value)

        now = time.time()
        gauge("lambda_task_duration_seconds", "Wall time of the last run", duration_s)
        gauge("lambda_task_last_run_timestamp_seconds", "End of the last run (unix time)", now)
        gauge("lambda_task_success", "1 if the last run succeeded or was skipped, else 0",
              1.0 if status in {"SUCCESS", "SKIPPED"} else 0.0)
        gauge("lambda_task_skipped", "1 if the last run had nothing to do (exit 99)",
              1.0 if status == "SKIPPED" else 0.0)
        if status == "SUCCESS":
            gauge("lambda_task_last_success_timestamp_seconds", "End of the last successful run", now)
        if ref_month:
            gauge("lambda_task_ref_month", "Reference month (YYYYMM) of the last run", float(ref_month))
        for kind in ("in", "out", "rejected"):
            if counters.get(f"rows_{kind}") is not None:
                gauge("lambda_task_rows", "Rows handled by the last run", float(counters[f"rows_{kind}"]),
                      {"kind": kind})
        for kind in ("in", "out"):
            if counters.get(f"bytes_{kind}") is not None:
                gauge("lambda_task_bytes", "Bytes handled by the last run", float(counters[f"bytes_{kind}"]),
                      {"kind": kind})
        for state in ("total", "ok", "failed", "skipped"):
            if counters.get(f"files_{state}") is not None:
                gauge("lambda_task_files", "Files handled by the last run", float(counters[f"files_{state}"]),
                      {"state": state})
        gauge("lambda_task_dq_failures", "Failed data-quality checks in the last run", float(warn_count),
              {"severity": "WARN"})
        gauge("lambda_task_dq_failures", "Failed data-quality checks in the last run", float(block_count),
              {"severity": "BLOCK"})

        pushadd_to_gateway(s.pushgateway_url, job=JOB, registry=reg,
                           grouping_key={"pipeline": pipeline, "task": task_id}, timeout=5)
        return True
    except Exception as exc:  # noqa: BLE001 - metrics must never break the pipeline
        _log.warning("metrics push failed: %s", exc)
        return False
