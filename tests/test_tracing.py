"""Tracing: the no-op paths never get in the way, and the spans of a task have the ids/attributes Tempo expects."""
import hashlib
import json
import socket

import pytest

from pipelines.commons import dq, settings, tracing
from pipelines.commons.logger import log_event
from pipelines.commons.refmonth import RefMonth
from pipelines.commons.task import EXIT_FAILED, EXIT_OK, run_task


def _run(body, **kw):
    kw.setdefault("ref_month", RefMonth(2026, 9))
    return run_task(body, pipeline="test", task_id="t1", layer="bronze", **kw)


def _log_lines(tmp_path):
    return [json.loads(x) for f in (tmp_path / "logs" / "test").glob("*.jsonl") for x in f.read_text().splitlines()]


# ---------------------------------------------------------------- ids (must match audit.v_trace_spans in SQL)
def test_ids_are_derived_from_the_run_id_like_the_sql_view():
    run_id = "manual__2026-10-21T10:00:00+00:00"
    def md5(key: str) -> str:
        return hashlib.md5(key.encode()).hexdigest()

    assert tracing.format_trace_id(tracing.trace_id_for_run(run_id)) == md5(run_id)
    assert tracing.format_span_id(tracing.root_span_id(run_id)) == md5(f"{run_id}/root")[:16]
    task_span = tracing.task_span_id(run_id, "rfb03_bronze", 2)
    assert tracing.format_span_id(task_span) == md5(f"{run_id}/rfb03_bronze/2")[:16]


def test_attributes_are_otel_compatible():
    out = tracing.attributes({"a": 1, "b": None, "c": True, "d": ["x", "y"], "e": {"k": 1}, "f": [1, "x"]})
    assert out == {"a": 1, "c": True, "d": ["x", "y"], "e": "{'k': 1}", "f": "[1, 'x']"}


# ---------------------------------------------------------------- no-op paths
def test_disabled_by_flag_is_a_noop(isolated_env):
    seen = {}

    def body(t):
        seen["enabled"] = tracing.enabled()
        seen["ids"] = tracing.current_ids()
        log_event(t.log, "step_done", "a timed step", seconds=1.5)
        with tracing.span("block") as sp:
            seen["span"] = sp

    assert _run(body) == EXIT_OK
    assert seen == {"enabled": False, "ids": (None, None), "span": None}
    assert all("trace_id" not in x for x in _log_lines(isolated_env))


def test_missing_sdk_is_a_noop(monkeypatch):
    monkeypatch.setenv("LAMBDA_OTEL_ENABLED", "1")
    settings.get_settings.cache_clear()
    monkeypatch.setattr(tracing, "SDK_AVAILABLE", False)
    assert tracing.setup("test") is False
    assert _run(lambda t: None) == EXIT_OK


def test_unreachable_collector_is_a_noop(monkeypatch):
    with socket.socket() as s:          # a free port nobody listens on
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    monkeypatch.setenv("LAMBDA_OTEL_ENABLED", "1")
    monkeypatch.setenv("LAMBDA_OTEL_ENDPOINT", f"http://127.0.0.1:{port}")
    settings.get_settings.cache_clear()
    assert tracing.setup("test") is False
    assert not tracing.enabled()


# ---------------------------------------------------------------- spans (in-memory exporter)
@pytest.fixture
def spans(monkeypatch):
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    monkeypatch.setenv("LAMBDA_OTEL_ENABLED", "1")
    settings.get_settings.cache_clear()
    exporter = InMemorySpanExporter()
    assert tracing.setup("test", exporter=exporter)
    return exporter


def test_task_span_ids_attributes_and_children(spans, isolated_env, monkeypatch):
    monkeypatch.setitem(dq.SEVERITY, "test.warn", dq.Severity.WARN)
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "estabelecimentos.bronze")

    def body(t):
        t.set(rows_out=10)
        t.check("test.warn", False, observed=1, threshold=0)
        log_event(t.log, "file_written", "wrote a file", seconds=2.0, file="a.csv", api_token="secret")
        log_event(t.log, "health_check", "s3: OK", latency_ms=150.0, component="s3")

    assert _run(body, entity="estabelecimentos") == EXIT_OK
    finished = {s.name: s for s in spans.get_finished_spans()}
    task, step = finished["t1[estabelecimentos]"], finished["file_written"]
    run_id = "manual__test"

    assert task.context.trace_id == tracing.trace_id_for_run(run_id)
    assert task.context.span_id == tracing.task_span_id(run_id, "t1", 1)
    assert task.parent.span_id == tracing.root_span_id(run_id)
    assert {k: task.attributes[k] for k in ("pipeline", "task_id", "layer", "entity", "ref_month", "run_id",
                                            "attempt", "airflow_task_id")} == {
        "pipeline": "test", "task_id": "t1", "layer": "bronze", "entity": "estabelecimentos", "ref_month": 202609,
        "run_id": run_id, "attempt": 1, "airflow_task_id": "estabelecimentos.bronze"}
    assert task.attributes["status"] == "SUCCESS" and task.attributes["rows_out"] == 10
    assert task.attributes["warn_count"] == 1
    assert [e.name for e in task.events] == ["dq_check"] and task.events[0].attributes["status"] == "FAIL"
    assert task.resource.attributes["service.name"] == "lambda-test"

    # the timed step: child of the task span, backdated by its duration, secrets redacted
    assert step.parent.span_id == task.context.span_id and step.context.trace_id == task.context.trace_id
    assert abs((step.end_time - step.start_time) / 1e9 - 2.0) < 0.01
    assert step.attributes["file"] == "a.csv" and step.attributes["api_token"] == "***"
    probe = finished["health_check:s3"]          # latency_ms counts as a duration too; the name says what was probed
    assert abs((probe.end_time - probe.start_time) / 1e9 - 0.15) < 0.01

    # LAMBDA_RUN_ID came from outside (a shared manual run): this task must not close the run's trace
    assert "test.run" not in finished

    # log lines point at their span: the step line at the step span, the rest at the task span
    lines = _log_lines(isolated_env)
    trace_hex = tracing.format_trace_id(task.context.trace_id)
    assert all(x["trace_id"] == trace_hex for x in lines if x["event"] != "log")
    by_event = {x["event"]: x for x in lines}
    assert by_event["file_written"]["span_id"] == tracing.format_span_id(step.context.span_id)
    assert by_event["task_end"]["span_id"] == tracing.format_span_id(task.context.span_id)


def test_failed_task_span_is_an_error_with_the_exception(spans):
    def boom(t):
        raise RuntimeError("boom")

    assert _run(boom) == EXIT_FAILED
    (task,) = spans.get_finished_spans()
    from opentelemetry.trace import StatusCode

    assert task.status.status_code is StatusCode.ERROR and task.attributes["status"] == "FAILED"
    assert any(e.name == "exception" and e.attributes["exception.message"] == "boom" for e in task.events)


def test_standalone_manual_run_emits_the_root_span(spans, monkeypatch):
    monkeypatch.delenv("LAMBDA_RUN_ID")          # this process creates the run id -> it owns the run
    assert _run(lambda t: None) == EXIT_OK
    finished = {s.name: s for s in spans.get_finished_spans()}
    root, task = finished["test.run"], finished["t1"]
    assert root.parent is None and root.context.span_id == task.parent.span_id
    assert root.context.trace_id == task.context.trace_id
    assert root.start_time <= task.start_time and root.end_time >= task.end_time


def test_airflow_traceparent_becomes_a_link(spans, monkeypatch):
    airflow_trace, airflow_span = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
    monkeypatch.setenv("TRACEPARENT", f"00-{airflow_trace}-{airflow_span}-01")
    assert _run(lambda t: None) == EXIT_OK
    (task,) = spans.get_finished_spans()
    (link,) = task.links
    assert tracing.format_trace_id(link.context.trace_id) == airflow_trace
    assert tracing.format_span_id(link.context.span_id) == airflow_span
    assert link.attributes["link.kind"] == "airflow.task"
