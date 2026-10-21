"""Close a pipeline run: audit.pipeline_run status/counts, a `pipeline_end` event and run-level metrics.

    python -m pipelines.commons.finalize_run --pipeline rfb          # run_id from Airflow (AIRFLOW_CTX_DAG_RUN_ID)

Runs as the LAST task of the DAG with trigger_rule=all_done, so it executes even when upstream tasks failed.
It exits 1 when the run failed: otherwise this always-green leaf task would make Airflow mark a broken run as
success (a DAG run's state comes from its leaf tasks).
It also writes the run's root span (pipelines/commons/tracing.py): the task spans already point at it as parent.
Manual runs that share LAMBDA_RUN_ID across several scripts call this at the end to close the run and its trace.
Not a TaskRun on purpose: it must not count itself among the run's tasks.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from pipelines.commons import tracing
from pipelines.commons.audit import get_sink
from pipelines.commons.logger import LOG_CONTEXT, configure_logging, get_logger, log_event
from pipelines.commons.settings import get_settings
from pipelines.commons.task import finalize_pipeline_run

SUMMARY_SQL = """
SELECT p.status, extract(epoch FROM p.finished_at - p.started_at) AS duration_s, p.ref_month,
       extract(epoch FROM p.started_at) AS started_epoch, p.trigger,
       p.tasks_total, p.tasks_ok, p.tasks_failed, p.warn_count, p.block_count,
       (SELECT coalesce(sum(t.rows_out), 0) FROM audit.task_run t WHERE t.run_id = p.run_id
          AND t.layer IN ('bronze', 'silver', 'gold')) AS rows_written,
       (SELECT coalesce(sum(t.rows_rejected), 0) FROM audit.task_run t WHERE t.run_id = p.run_id) AS rows_rejected,
       (SELECT string_agg(t.task_id, ', ') FROM audit.task_run t WHERE t.run_id = p.run_id
          AND t.status = 'FAILED') AS failed_tasks
FROM audit.pipeline_run p WHERE p.run_id = %s
"""


def push_pipeline_metrics(pipeline: str, summary: dict) -> None:
    s = get_settings()
    if not s.metrics_enabled:
        return
    try:
        from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

        reg = CollectorRegistry()

        def g(name: str, doc: str, value: float) -> None:
            Gauge(name, doc, registry=reg).set(value)

        ok = summary["status"] in ("SUCCESS", "SKIPPED")
        g("lambda_pipeline_duration_seconds", "Wall time of the last pipeline run", summary.get("duration_s") or 0)
        g("lambda_pipeline_success", "1 if the last run succeeded (or had nothing to do)", 1.0 if ok else 0.0)
        g("lambda_pipeline_last_run_timestamp_seconds", "End of the last run", time.time())
        if summary["status"] == "SUCCESS":
            g("lambda_pipeline_last_success_timestamp_seconds", "End of the last successful run", time.time())
        g("lambda_pipeline_tasks_failed", "Failed tasks in the last run", summary.get("tasks_failed") or 0)
        g("lambda_pipeline_rows_written", "Rows written to bronze/silver/gold by the last run",
          summary.get("rows_written") or 0)
        g("lambda_pipeline_dq_block", "BLOCK data-quality failures in the last run", summary.get("block_count") or 0)
        g("lambda_pipeline_dq_warn", "WARN data-quality failures in the last run", summary.get("warn_count") or 0)
        if summary.get("ref_month"):
            g("lambda_pipeline_ref_month", "Reference month of the last run", summary["ref_month"])
        pushadd_to_gateway(s.pushgateway_url, job="lambda_pipeline", registry=reg,
                           grouping_key={"pipeline": pipeline, "task": "_pipeline"}, timeout=5)
    except Exception as exc:  # noqa: BLE001 - metrics must never break the pipeline
        logging.getLogger("lambda.metrics").warning("pipeline metrics push failed: %s", exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--run-id", default=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or os.environ.get("LAMBDA_RUN_ID"))
    args = parser.parse_args(argv)
    if not args.run_id:
        parser.error("no run id (pass --run-id or run under Airflow)")

    configure_logging(args.pipeline)
    LOG_CONTEXT.update(run_id=args.run_id, task_id="finalize")
    log = get_logger(f"{args.pipeline}.finalize")
    finalize_pipeline_run(args.run_id)
    get_sink().flush()

    summary = {"status": "UNKNOWN"}
    try:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(get_settings().audit_dsn, row_factory=dict_row) as pg:
            row = pg.execute(SUMMARY_SQL, (args.run_id,)).fetchone()
            summary = {k: (float(v) if hasattr(v, "is_finite") else v) for k, v in (row or {}).items()} or summary
    except Exception as exc:  # noqa: BLE001
        log.warning("could not read the run summary from audit: %s", exc)

    started_epoch = summary.pop("started_epoch", None)
    trigger = summary.pop("trigger", None)
    trace_ids = None
    if started_epoch and tracing.setup(args.pipeline):
        trace_ids = tracing.emit_run_span(run_id=args.run_id, pipeline=args.pipeline, start_ns=int(started_epoch * 1e9),
                              status=summary.get("status") or "UNKNOWN",
                              attrs={k: v for k, v in summary.items() if k != "status"} | {"trigger": trigger})
        tracing.shutdown()

    failed = summary.get("status") == "FAILED"
    log_event(log, "pipeline_end",
              f"{args.pipeline} run {args.run_id}: {summary.get('status')} in {summary.get('duration_s') or 0:.0f}s, "
              f"{summary.get('tasks_ok')}/{summary.get('tasks_total')} tasks ok, "
              f"{int(summary.get('rows_written') or 0):,} rows written",
              level=logging.ERROR if failed else logging.INFO, trace_ids=trace_ids, **summary)
    push_pipeline_metrics(args.pipeline, summary)
    get_sink().close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
