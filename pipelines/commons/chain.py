"""Chain tracing across asset-scheduled DAGs (e.g. anp_pipeline -> anp_dbt -> anp_dbt_gold).

Airflow links an asset-triggered run to the events that triggered it, but only one hop back and only inside its own
metadata database. This module carries the ORIGIN of a chain (the run that started it, normally a bronze load)
through every hop, so audit can answer "which bronze load does this gold build come from, and where did it stop":

* a consumer run reads its triggering asset events: origin = the event's ``extra["chain_origins"]`` when the producer
  was itself a consumer, else the producing run (a chain start);
* a run with no triggering event (manual trigger) starts its own chain;
* ``audit.pipeline_chain`` gets one row per (origin, run): at start (`chain_start`), when the run publishes its asset
  (`publish`, which also puts the origins in the event extra for the next hop) and, for the chain start, from the
  producer's success callback (`emit_callback`);
* the dbt DAGs have no TaskRun tasks, so `chain_start`/`chain_end` also open and close their ``audit.pipeline_run``
  row and push the ``lambda_pipeline_*`` run metrics (the existing run-failed alert covers them);
* Pushgateway gauges ``lambda_chain_emitted_timestamp_seconds{asset,consumer}`` (producer) and
  ``lambda_chain_consumed_timestamp_seconds{asset,consumer}`` (consumer start): the Prometheus rule
  ``LambdaChainStalled`` fires when an emission is not picked up in time (paused or stuck next DAG).

Runs inside the Airflow workers (env-airflow): stdlib at import time, psycopg/httpx lazily; no prometheus_client.
Everything is best-effort (observability must not break a load) except `chain_end`, which fails a failed run: it is
the DAG's only leaf, and an always-green leaf would make Airflow mark a broken run as success (see finalize_run).
"""
from __future__ import annotations

import logging
import os
import re
import socket
import time
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from pipelines.commons.audit import get_sink
from pipelines.commons.settings import get_settings

_log = logging.getLogger("lambda.chain")

ORIGINS_KEY = "chain_origins"
_LABEL_VALUE = re.compile(r"^[A-Za-z0-9_.:-]+$")      # safe as a Pushgateway URL path segment
FAILED_STATES = frozenset({"failed", "upstream_failed"})


# ---------------------------------------------------------------------------------------------- pure logic (tested)
def normalize_events(triggering_asset_events: Mapping[Any, Iterable[Any]] | None) -> list[dict[str, Any]]:
    """Airflow's TriggeringAssetEventsAccessor (asset -> events) to plain dicts, deduplicated (aliases repeat)."""
    seen: set[tuple] = set()
    out: list[dict[str, Any]] = []
    for events in (triggering_asset_events or {}).values():
        for ev in events:
            key = (ev.asset.name, ev.source_dag_id, ev.source_run_id, ev.timestamp)
            if key in seen:
                continue
            seen.add(key)
            out.append({"asset": ev.asset.name, "source_dag_id": ev.source_dag_id, "source_run_id": ev.source_run_id,
                        "extra": dict(ev.extra or {}), "timestamp": ev.timestamp})
    return out


def chain_links(events: list[dict[str, Any]], dag_id: str, run_id: str) -> list[dict[str, Any]]:
    """One link per (origin, upstream event) of this run; this run itself is the origin when nothing triggered it.

    An event whose extra carries `chain_origins` passes those origins on; an event without it (the producer was a
    chain start, or an older producer) makes the producing run the origin.
    """
    links: dict[tuple[str, str], dict[str, Any]] = {}
    for ev in sorted(events, key=lambda e: e["timestamp"]):
        origins = ev["extra"].get(ORIGINS_KEY) or [{"dag_id": ev["source_dag_id"], "run_id": ev["source_run_id"]}]
        for o in origins:
            if not o.get("dag_id") or not o.get("run_id"):
                continue
            # the earliest event per origin is what made this run wait; later ones arrived while it was queued
            links.setdefault((o["dag_id"], o["run_id"]), {
                "origin_dag_id": o["dag_id"], "origin_run_id": o["run_id"], "dag_id": dag_id, "run_id": run_id,
                "upstream_asset": ev["asset"], "upstream_dag_id": ev["source_dag_id"],
                "upstream_run_id": ev["source_run_id"], "triggered_at": ev["timestamp"]})
    if not links:
        return [{"origin_dag_id": dag_id, "origin_run_id": run_id, "dag_id": dag_id, "run_id": run_id,
                 "upstream_asset": None, "upstream_dag_id": None, "upstream_run_id": None, "triggered_at": None}]
    return list(links.values())


def origins_extra(links: list[dict[str, Any]]) -> dict[str, Any]:
    """Asset event extra for the next hop: the origins this run carries (JSON-serializable)."""
    return {ORIGINS_KEY: [{"dag_id": lk["origin_dag_id"], "run_id": lk["origin_run_id"]} for lk in links]}


def run_summary(states: Mapping[str, str | None], own_task_id: str) -> dict[str, Any]:
    """Status of a DAG run from its task states (`ti.get_task_states`), leaving out the task asking."""
    others = {t: s for t, s in states.items() if t != own_task_id}
    failed = sorted(t for t, s in others.items() if s in FAILED_STATES)
    ok = sum(1 for s in others.values() if s == "success")
    skipped = sum(1 for s in others.values() if s == "skipped")
    status = "FAILED" if failed else ("SKIPPED" if others and skipped == len(others) else "SUCCESS")
    return {"status": status, "tasks_total": len(others), "tasks_ok": ok, "tasks_failed": len(failed),
            "failed_tasks": failed}


# ---------------------------------------------------------------------------------------------- side effects
def pushgateway_base(url: str) -> str:
    """Settings hold `host:port` (what prometheus_client accepts); httpx needs a scheme."""
    url = url.rstrip("/")
    return url if "://" in url else f"http://{url}"


def push_gauges(grouping: dict[str, str], gauges: dict[str, float]) -> bool:
    """POST (= pushadd: replaces only these metric names in the group) text-format gauges to the Pushgateway."""
    s = get_settings()
    if not s.metrics_enabled:
        return False
    try:
        import httpx

        for v in grouping.values():
            if not _LABEL_VALUE.match(v):
                raise ValueError(f"label value not URL-safe: {v!r}")
        path = "/".join(f"{k}/{v}" for k, v in grouping.items())
        body = "".join(f"# TYPE {name} gauge\n{name} {value}\n" for name, value in gauges.items())
        r = httpx.post(f"{pushgateway_base(s.pushgateway_url)}/metrics/job/lambda_pipeline/{path}", content=body,
                       timeout=5)
        r.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001 - metrics must never break the pipeline
        _log.warning("chain metrics push failed: %s", exc)
        return False


def _write_links(links: list[dict[str, Any]], *, started_at: datetime | None = None, emitted_asset: str | None = None,
                 emitted_at: datetime | None = None) -> None:
    sink = get_sink()
    sink.write_many("pipeline_chain_upsert", [
        lk | {"started_at": started_at, "emitted_asset": emitted_asset, "emitted_at": emitted_at} for lk in links])
    sink.flush()


def _ctx(context: Mapping[str, Any]) -> tuple[str, str, Any]:
    dag_run = context["dag_run"]
    return dag_run.dag_id, dag_run.run_id, dag_run


def _links_from_context(context: Mapping[str, Any]) -> list[dict[str, Any]]:
    dag_id, run_id, _ = _ctx(context)
    return chain_links(normalize_events(context.get("triggering_asset_events")), dag_id, run_id)


def chain_start(**context: Any) -> dict[str, Any]:
    """First task of a consumer DAG: record its links, open its audit.pipeline_run, mark the upstream as consumed."""
    dag_id, run_id, dag_run = _ctx(context)
    links = _links_from_context(context)
    started_at = dag_run.start_date or datetime.now(UTC)
    run_type = getattr(dag_run, "run_type", None)
    get_sink().write("pipeline_run_start", {
        "run_id": run_id, "pipeline": dag_id, "dag_id": dag_id, "ref_month": None,
        "trigger": str(getattr(run_type, "value", run_type) or "") or None, "started_at": started_at,
        "host": socket.gethostname(), "git_sha": os.environ.get("LAMBDA_GIT_SHA"),
        "params": {"origins": [f"{lk['origin_dag_id']}/{lk['origin_run_id']}" for lk in links]}})
    _write_links(links, started_at=started_at)
    now = time.time()
    for asset in sorted({lk["upstream_asset"] for lk in links if lk["upstream_asset"]}):
        push_gauges({"pipeline": "chain", "asset": asset, "consumer": dag_id},
                    {"lambda_chain_consumed_timestamp_seconds": now})
    for lk in links:
        _log.info("chain link: origin %s/%s -> %s/%s via %s (event %s)", lk["origin_dag_id"], lk["origin_run_id"],
                  dag_id, run_id, lk["upstream_asset"] or "-", lk["triggered_at"] or "-")
    return origins_extra(links)      # XCom: JSON only


def publish(asset: Any, consumer: str | None = None):
    """python_callable for the task with ``outlets=[asset]``: origins into the event extra, emission recorded."""

    def _publish(**context: Any) -> dict[str, Any]:
        _, _, dag_run = _ctx(context)
        links = _links_from_context(context)
        extra = origins_extra(links)
        context["outlet_events"][asset].extra = extra   # becomes asset_event.extra, read by the next hop
        _write_links(links, started_at=dag_run.start_date, emitted_asset=asset.name, emitted_at=datetime.now(UTC))
        _push_emitted(asset.name, consumer)
        _log.info("published %s carrying origins %s", asset.name, extra[ORIGINS_KEY])
        return extra

    return _publish


def emit_callback(asset: Any, consumer: str | None = None):
    """on_success_callback for a chain START (a task with outlets that consumes nothing, e.g. the bronze load)."""

    def _callback(context: Mapping[str, Any]) -> None:
        try:
            dag_id, run_id, dag_run = _ctx(context)
            links = chain_links([], dag_id, run_id)
            _write_links(links, started_at=dag_run.start_date, emitted_asset=asset.name, emitted_at=datetime.now(UTC))
            _push_emitted(asset.name, consumer)
        except Exception as exc:  # noqa: BLE001 - a callback must never fail the task
            _log.warning("chain emit callback failed for %s: %s", getattr(asset, "name", asset), exc)

    return _callback


def _push_emitted(asset_name: str, consumer: str | None) -> None:
    grouping = {"pipeline": "chain", "asset": asset_name}
    if consumer:            # no consumer label = nothing downstream to wait for (the alert ignores it)
        grouping["consumer"] = consumer
    push_gauges(grouping, {"lambda_chain_emitted_timestamp_seconds": time.time()})


def chain_end(**context: Any) -> dict[str, Any]:
    """Last task (trigger_rule=all_done): close audit.pipeline_run, push run metrics, FAIL if the run failed."""
    dag_id, run_id, dag_run = _ctx(context)
    ti = context["ti"]
    states = ti.get_task_states(dag_id=dag_id, run_ids=[run_id]).get(run_id, {})
    summary = run_summary(states, ti.task_id)
    finished = datetime.now(UTC)
    get_sink().write("pipeline_run_close", {
        "run_id": run_id, "finished_at": finished, "status": summary["status"],
        "tasks_total": summary["tasks_total"], "tasks_ok": summary["tasks_ok"],
        "tasks_failed": summary["tasks_failed"]})
    get_sink().flush()
    started = dag_run.start_date or finished
    gauges = {"lambda_pipeline_duration_seconds": (finished - started).total_seconds(),
              "lambda_pipeline_success": 0.0 if summary["status"] == "FAILED" else 1.0,
              "lambda_pipeline_last_run_timestamp_seconds": time.time(),
              "lambda_pipeline_tasks_failed": float(summary["tasks_failed"])}
    if summary["status"] == "SUCCESS":
        gauges["lambda_pipeline_last_success_timestamp_seconds"] = time.time()
    push_gauges({"pipeline": dag_id, "task": "_pipeline"}, gauges)
    msg = (f"{dag_id} run {run_id}: {summary['status']}, {summary['tasks_ok']}/{summary['tasks_total']} tasks ok"
           + (f", failed: {', '.join(summary['failed_tasks'])}" if summary["failed_tasks"] else ""))
    if summary["status"] == "FAILED":
        _log.error(msg)
        raise RuntimeError(msg)
    _log.info(msg)
    return summary
