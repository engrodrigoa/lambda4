"""TaskRun: the boundary of every pipeline task (one process = one task).

    def main(task: TaskRun) -> None:
        task.set(files_total=37)
        ...
        task.check("landing.contract_files_present", ok, observed=n, threshold=37)
        raise TaskSkipped("nothing to do")          # -> status SKIPPED, exit 99

    if __name__ == "__main__":
        sys.exit(run_task(main, pipeline="rfb", task_id="rfb01_download", layer="landing", ref_month=ref))

On enter: logging context, audit.pipeline_run (first task creates it), audit.task_run RUNNING, `task_start` event.
On exit:  status + exit code, `task_end` event with every counter, audit.task_run finished, Pushgateway metrics,
          and - for a manual standalone run - the pipeline_run is finalized too (under Airflow the DAG's last task
          does it with `finalize_pipeline_run`).
Exceptions are recorded with their traceback and turned into exit code 1: TaskRun is the outermost layer.
"""
from __future__ import annotations

import logging
import signal
import time
import traceback
from collections.abc import Callable
from typing import Any

from pipelines.commons import dq, metrics
from pipelines.commons.audit import get_sink, utcnow_iso
from pipelines.commons.context import RunContext, airflow_attempt
from pipelines.commons.logger import LOG_CONTEXT, configure_logging, get_logger, log_event
from pipelines.commons.refmonth import RefMonth

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_SKIPPED = 99

COUNTERS = ("rows_in", "rows_out", "rows_rejected", "bytes_in", "bytes_out",
            "files_total", "files_ok", "files_failed", "files_skipped")

_CURRENT: TaskRun | None = None


class TaskSkipped(Exception):
    """Expected 'nothing to do' (e.g. the source has not published the month yet): status SKIPPED, exit 99."""


class TaskTerminated(Exception):
    """SIGTERM received (Airflow timeout, manual kill): recorded as a normal failure before the process exits."""


def current_task() -> TaskRun | None:
    return _CURRENT


class TaskRun:
    def __init__(self, *, pipeline: str, task_id: str, layer: str, ref_month: RefMonth | None = None,
                 entity: str | None = None, params: dict[str, Any] | None = None,
                 finalize_pipeline: bool | None = None):
        self.pipeline, self.task_id, self.layer, self.entity = pipeline, task_id, layer, entity
        self.ref_month = ref_month
        self.params = params or {}
        self.ctx = RunContext.current()
        self.attempt = airflow_attempt()
        self.finalize_pipeline = (not self.ctx.under_airflow) if finalize_pipeline is None else finalize_pipeline
        self.counters: dict[str, int | None] = dict.fromkeys(COUNTERS)
        self.extra_metrics: dict[str, Any] = {}
        self.warn_count = 0
        self.block_count = 0
        self.spark_app_id: str | None = None
        self.status = "RUNNING"
        self.exit_code: int | None = None
        self.log = get_logger(f"{pipeline}.{task_id}")
        self._t0 = 0.0

    # ------------------------------------------------------------------ counters / metrics
    def set(self, **values: int | None) -> None:
        for k, v in values.items():
            if k not in self.counters:
                raise KeyError(f"unknown counter {k}; extra values go to metric()")
            self.counters[k] = v

    def add(self, **values: int) -> None:
        for k, v in values.items():
            if k not in self.counters:
                raise KeyError(f"unknown counter {k}")
            self.counters[k] = (self.counters[k] or 0) + v

    def metric(self, **values: Any) -> None:
        """Free-form values stored in audit.task_run.metrics (jsonb): throughput, partitions, shuffle bytes..."""
        self.extra_metrics.update(values)

    def set_ref_month(self, ref: RefMonth) -> None:
        """For tasks that resolve the month themselves (e.g. `--ref-month next`): stamps logs and the audit rows."""
        self.ref_month = ref
        LOG_CONTEXT["ref_month"] = ref.yyyymm

    # ------------------------------------------------------------------ data quality
    def check(self, name: str, passed: bool, *, observed: float | None = None, threshold: float | None = None,
              message: str = "", **context: Any) -> bool:
        severity = dq.severity_of(name)
        status = "PASS" if passed else "FAIL"
        get_sink().write("dq_event", {
            "created_at": utcnow_iso(), "run_id": self.ctx.run_id, "task_id": self.task_id,
            "pipeline": self.pipeline, "ref_month": self.ref_month.yyyymm if self.ref_month else None,
            "check_name": name, "severity": severity.value, "status": status,
            "observed": observed, "threshold": threshold, "message": message, "context": context,
        })
        level = logging.INFO if passed else (logging.ERROR if severity is dq.Severity.BLOCK else logging.WARNING)
        log_event(self.log, "dq_check", f"{name} {status}" + (f": {message}" if message else ""), level=level,
                  check=name, severity=severity.value, status=status, observed=observed, threshold=threshold,
                  **context)
        if not passed:
            if severity is dq.Severity.BLOCK:
                self.block_count += 1
                raise dq.BlockingCheckFailed(name, message or "failed")
            self.warn_count += 1
        return passed

    # ------------------------------------------------------------------ lifecycle
    def __enter__(self) -> TaskRun:
        global _CURRENT
        configure_logging(self.pipeline)
        LOG_CONTEXT.update(run_id=self.ctx.run_id, task_id=self.task_id,
                           ref_month=self.ref_month.yyyymm if self.ref_month else None)
        sink = get_sink()
        now = utcnow_iso()
        ref = self.ref_month.yyyymm if self.ref_month else None
        sink.write("pipeline_run_start", {
            "run_id": self.ctx.run_id, "pipeline": self.pipeline, "dag_id": self.ctx.dag_id, "ref_month": ref,
            "trigger": self.ctx.trigger, "started_at": now, "host": self.ctx.host, "git_sha": self.ctx.git_sha,
            "params": {"ref_month": str(self.ref_month) if self.ref_month else None, **self.params},
        })
        sink.write("task_start", {
            "run_id": self.ctx.run_id, "task_id": self.task_id, "attempt": self.attempt, "pipeline": self.pipeline,
            "layer": self.layer, "entity": self.entity, "ref_month": ref, "started_at": now,
            "airflow_task_id": self.ctx.airflow_task_id,
        })
        _CURRENT = self
        self._t0 = time.monotonic()
        # params first: the TaskRun's own fields win on a name clash (e.g. params={"entity": ...})
        fields = {**self.params, "layer": self.layer, "entity": self.entity, "attempt": self.attempt,
                  "trigger": self.ctx.trigger, "git_sha": self.ctx.git_sha, "under_airflow": self.ctx.under_airflow}
        log_event(self.log, "task_start", f"{self.pipeline}.{self.task_id} started", **fields)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        global _CURRENT
        duration = time.monotonic() - self._t0
        error_class = error_message = None
        if exc_type is None:
            self.status, self.exit_code = "SUCCESS", EXIT_OK
        elif issubclass(exc_type, TaskSkipped):
            self.status, self.exit_code = "SKIPPED", EXIT_SKIPPED
            error_message = str(exc)
        else:
            self.status, self.exit_code = "FAILED", EXIT_FAILED
            error_class = exc_type.__name__
            error_message = str(exc)[:2000]
            log_event(self.log, "task_error", f"{error_class}: {error_message}", level=logging.ERROR,
                      error_class=error_class, traceback="".join(traceback.format_exception(exc_type, exc, tb))[-4000:])

        counters = {k: v for k, v in self.counters.items() if v is not None}
        level = logging.INFO if self.status != "FAILED" else logging.ERROR
        fields = {**self.extra_metrics, **counters, "status": self.status, "exit_code": self.exit_code,
                  "duration_s": round(duration, 3), "warn_count": self.warn_count, "block_count": self.block_count}
        log_event(self.log, "task_end", f"{self.pipeline}.{self.task_id} {self.status} in {duration:.1f}s",
                  level=level, **fields)

        sink = get_sink()
        sink.write("task_finish", {
            "run_id": self.ctx.run_id, "task_id": self.task_id, "attempt": self.attempt, "status": self.status,
            "ref_month": self.ref_month.yyyymm if self.ref_month else None, "exit_code": self.exit_code,
            "finished_at": utcnow_iso(), **self.counters,
            "warn_count": self.warn_count, "block_count": self.block_count, "spark_app_id": self.spark_app_id,
            "error_class": error_class, "error_message": error_message,
            "metrics": {"duration_s": round(duration, 3), **self.extra_metrics},
        })
        metrics.push_task_metrics(
            pipeline=self.pipeline, task_id=self.task_id, layer=self.layer, status=self.status, duration_s=duration,
            counters=self.counters, ref_month=self.ref_month.yyyymm if self.ref_month else None,
            warn_count=self.warn_count, block_count=self.block_count)
        if self.finalize_pipeline:
            finalize_pipeline_run(self.ctx.run_id, ref_month=self.ref_month.yyyymm if self.ref_month else None)
        sink.flush()
        if sink.spooled:
            self.log.warning("%d audit events spooled (database unavailable); they replay on the next run",
                             sink.spooled)
        _CURRENT = None
        # swallow ordinary exceptions (already recorded); let KeyboardInterrupt/SystemExit propagate
        return exc_type is None or issubclass(exc_type, Exception)


def finalize_pipeline_run(run_id: str, status: str | None = None, ref_month: int | None = None) -> None:
    """Close audit.pipeline_run: counts and status derived from the latest attempt of each task."""
    get_sink().write("pipeline_run_finish", {"run_id": run_id, "finished_at": utcnow_iso(), "status": status,
                                             "ref_month": ref_month})


def _raise_terminated(signum, frame) -> None:
    raise TaskTerminated(f"received signal {signal.Signals(signum).name} (timeout or kill)")


def run_task(fn: Callable[[TaskRun], None], **task_kwargs: Any) -> int:
    """Run `fn` inside a TaskRun and return the process exit code (0 ok, 99 skipped, 1 failed).
    SIGTERM (what Airflow sends on timeout/kill before SIGKILL) becomes an exception, so the audit row is closed."""
    previous = signal.signal(signal.SIGTERM, _raise_terminated)
    try:
        task = TaskRun(**task_kwargs)
        with task:
            fn(task)
        get_sink().close()
        return task.exit_code if task.exit_code is not None else EXIT_FAILED
    finally:
        signal.signal(signal.SIGTERM, previous)
