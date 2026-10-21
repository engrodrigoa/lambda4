"""Airflow task callbacks that keep audit truthful when the PROCESS could not report itself.

A pipeline task records its own start/end in audit.task_run (TaskRun). But when Airflow kills it (timeout -> SIGKILL,
OOM, worker crash) the process never reaches its exit code, and the row would stay RUNNING forever. The failure /
retry callback runs in Airflow's own process and closes that attempt as FAILED (only if it is still RUNNING, so a
failure the task already recorded keeps its real error).

Imported by the DAG file (runs inside the Airflow virtualenv: needs psycopg, which it has).
"""
from __future__ import annotations

from typing import Any

from pipelines.commons.audit import get_sink, utcnow_iso


def close_running_attempt(context: dict[str, Any]) -> None:
    try:
        ti = context["ti"]
        exc = context.get("exception")
        sink = get_sink()
        sink.write("task_fail_if_running", {
            "run_id": ti.run_id, "airflow_task_id": ti.task_id, "attempt": ti.try_number,
            "finished_at": utcnow_iso(), "error_class": "AirflowTaskFailure",
            "error_message": (f"{type(exc).__name__}: {exc}" if exc else "failed in Airflow")[:2000],
        })
        sink.close()
    except Exception:  # noqa: BLE001 - a callback must never raise into the scheduler
        pass
