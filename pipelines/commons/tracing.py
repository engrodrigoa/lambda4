"""OpenTelemetry traces for pipeline runs: one trace per run, one span per task attempt, child spans per timed step.

    <pipeline>.run            root span, emitted when the run is finalized     trace_id = md5(run_id)
      └─ <task_id>[<entity>]  one per TaskRun attempt (one process)           span_id  = md5(run_id/task_id/attempt)
           └─ <event>         log events that carry `seconds` / `latency_ms`, and `tracing.span(...)` blocks

Ids are derived from the run id exactly like the SQL view `audit.v_trace_spans`, so the audit rows, the SQL-built
trace and the Tempo trace of a run share one trace id, and every task of a run (separate processes, separate Airflow
workers, retries) lands in the same trace without passing any context around.

Airflow's own spans (AIRFLOW__TRACES__OTEL_ON) form a separate trace per DAG run with a random id. When the DAG passes
the W3C `TRACEPARENT` of Airflow's `worker.<task>` span to the process, our task/run span carries a span *link* to it.

Export: OTLP/HTTP to Alloy (LAMBDA_OTEL_ENDPOINT, default http://127.0.0.1:4318), which forwards to Tempo.
Everything here is a quiet no-op when LAMBDA_OTEL_ENABLED=0, the SDK is not installed, or the collector does not
answer at start-up: tracing must never slow down or break a pipeline.
"""
from __future__ import annotations

import contextvars
import hashlib
import logging
import os
import socket
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse

from pipelines.commons.context import git_sha
from pipelines.commons.settings import get_settings

try:
    from opentelemetry import context as otel_context
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
    from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
    from opentelemetry.trace import Link, NonRecordingSpan, SpanContext, Status, StatusCode, TraceFlags
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    SDK_AVAILABLE = True
except ImportError:  # the data env without opentelemetry-sdk: tracing is a no-op
    SDK_AVAILABLE = False
    RandomIdGenerator = object  # type: ignore[assignment,misc]

_log = logging.getLogger("lambda.tracing")
SCOPE = "lambda4.pipelines"
FLUSH_TIMEOUT_MS = 5000

# one-shot (trace_id, span_id) for the next span this process starts: how a span gets a deterministic id
_FORCED_IDS: contextvars.ContextVar[tuple[int, int] | None] = contextvars.ContextVar("lambda_forced_ids",
                                                                                      default=None)


class _DeterministicIds(RandomIdGenerator):
    """Random ids, except when `_FORCED_IDS` holds the ids of a run/task span (derived from the run id)."""

    def generate_span_id(self) -> int:
        forced = _FORCED_IDS.get()
        return forced[1] if forced else super().generate_span_id()

    def generate_trace_id(self) -> int:
        forced = _FORCED_IDS.get()
        return forced[0] if forced else super().generate_trace_id()


class _Runtime:
    initialized = False
    provider: Any = None
    tracer: Any = None
    task_span: Any = None
    task_token: Any = None


_rt = _Runtime()


# ---------------------------------------------------------------------------------------------- ids
def _md5_int(key: str, hex_chars: int) -> int:
    return int(hashlib.md5(key.encode(), usedforsecurity=False).hexdigest()[:hex_chars], 16) or 1


def trace_id_for_run(run_id: str) -> int:
    """128-bit trace id = md5(run_id), the same `traceID` audit.v_trace_spans computes in SQL."""
    return _md5_int(run_id, 32)


def root_span_id(run_id: str) -> int:
    return _md5_int(f"{run_id}/root", 16)


def task_span_id(run_id: str, task_id: str, attempt: int) -> int:
    return _md5_int(f"{run_id}/{task_id}/{attempt}", 16)


def format_trace_id(trace_id: int) -> str:
    return f"{trace_id:032x}"


def format_span_id(span_id: int) -> str:
    return f"{span_id:016x}"


# ---------------------------------------------------------------------------------------------- setup
def _collector_reachable(endpoint: str, timeout_s: float = 0.3) -> bool:
    url = urlparse(endpoint)
    try:
        with socket.create_connection((url.hostname or "127.0.0.1", url.port or 4318), timeout=timeout_s):
            return True
    except OSError:
        return False


def setup(pipeline: str, exporter: SpanExporter | None = None) -> bool:
    """Idempotent. Builds the tracer provider for this process; returns whether tracing is active.
    `exporter` (tests) replaces the OTLP exporter and is called synchronously."""
    if _rt.initialized:
        return _rt.tracer is not None
    _rt.initialized = True
    s = get_settings()
    if not (s.otel_enabled and SDK_AVAILABLE):
        if s.otel_enabled:
            _log.info("tracing disabled: opentelemetry-sdk is not installed in this environment")
        return False
    if exporter is None:
        if not _collector_reachable(s.otel_endpoint):
            _log.warning("tracing disabled for this run: no OTLP collector at %s (start alloy and tempo)",
                         s.otel_endpoint)
            return False
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=s.otel_endpoint.rstrip("/") + "/v1/traces",
                                                        timeout=5),
                                       schedule_delay_millis=2000)
    else:
        processor = SimpleSpanProcessor(exporter)
    resource = Resource.create({
        "service.name": f"lambda-{pipeline}", "service.namespace": "lambda4", "service.version": git_sha() or "",
        "host.name": socket.gethostname(), "deployment.environment": "lab",
    })
    _rt.provider = TracerProvider(resource=resource, id_generator=_DeterministicIds())
    _rt.provider.add_span_processor(processor)
    _rt.tracer = _rt.provider.get_tracer(SCOPE)
    return True


def enabled() -> bool:
    return _rt.tracer is not None


def flush() -> None:
    if _rt.provider is not None:
        try:
            _rt.provider.force_flush(FLUSH_TIMEOUT_MS)
        except Exception as exc:  # noqa: BLE001 - tracing must never break the pipeline
            _log.warning("trace flush failed: %s", exc)


def shutdown() -> None:
    """Flush and forget the provider (end of process; tests)."""
    if _rt.task_token is not None:
        otel_context.detach(_rt.task_token)
    if _rt.provider is not None:
        try:
            _rt.provider.shutdown()
        except Exception as exc:  # noqa: BLE001
            _log.warning("trace shutdown failed: %s", exc)
    _rt.initialized, _rt.provider, _rt.tracer, _rt.task_span, _rt.task_token = False, None, None, None, None


# ---------------------------------------------------------------------------------------------- helpers
def attributes(values: Mapping[str, Any]) -> dict[str, Any]:
    """OTel attribute values are str/bool/int/float (or homogeneous lists of them); None is dropped."""
    out: dict[str, Any] = {}
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, bool | int | float | str):
            out[key] = value
        elif isinstance(value, list | tuple) and value and all(isinstance(v, str) for v in value):
            out[key] = list(value)
        else:
            out[key] = str(value)[:1000]
    return out


def _airflow_links() -> list[Link]:
    """A link to Airflow's `worker.<task>` span when the DAG exported its W3C context as TRACEPARENT."""
    traceparent = os.environ.get("TRACEPARENT")
    if not traceparent:
        return []
    ctx = trace.get_current_span(TraceContextTextMapPropagator().extract({"traceparent": traceparent}))
    span_context = ctx.get_span_context()
    return [Link(span_context, {"link.kind": "airflow.task"})] if span_context.is_valid else []


def _set_status(span: Any, status: str, error: BaseException | None = None) -> None:
    span.set_attribute("status", status)
    if status == "FAILED":
        if error is not None:
            span.record_exception(error)
        span.set_status(Status(StatusCode.ERROR, f"{type(error).__name__}: {error}"[:500] if error else None))
    else:
        span.set_status(Status(StatusCode.OK))


def _parent_context() -> Any:
    """The current span of this thread, else the task span (worker threads do not inherit the context)."""
    if trace.get_current_span().get_span_context().is_valid:
        return None
    return trace.set_span_in_context(_rt.task_span) if _rt.task_span is not None else None


def current_ids() -> tuple[str | None, str | None]:
    """(trace_id, span_id) to stamp on log lines; (None, None) when tracing is off."""
    if _rt.tracer is None:
        return None, None
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid and _rt.task_span is not None:
        span_context = _rt.task_span.get_span_context()
    if not span_context.is_valid:
        return None, None
    return format_trace_id(span_context.trace_id), format_span_id(span_context.span_id)


# ---------------------------------------------------------------------------------------------- spans
def start_task_span(*, run_id: str, task_id: str, attempt: int, name: str, attrs: Mapping[str, Any],
                    start_ns: int | None = None) -> Any:
    """Open the span of a task attempt (child of the run's root span) and make it current."""
    if _rt.tracer is None:
        return None
    trace_id = trace_id_for_run(run_id)
    parent = NonRecordingSpan(SpanContext(trace_id=trace_id, span_id=root_span_id(run_id), is_remote=True,
                                          trace_flags=TraceFlags(TraceFlags.SAMPLED)))
    token = _FORCED_IDS.set((trace_id, task_span_id(run_id, task_id, attempt)))
    try:
        span = _rt.tracer.start_span(name, context=trace.set_span_in_context(parent), attributes=attributes(attrs),
                                     links=_airflow_links(), start_time=start_ns)
    finally:
        _FORCED_IDS.reset(token)
    _rt.task_span = span
    _rt.task_token = otel_context.attach(trace.set_span_in_context(span))
    return span


def end_task_span(span: Any, *, status: str, attrs: Mapping[str, Any], error: BaseException | None = None) -> None:
    if span is None:
        return
    span.set_attributes(attributes(attrs))
    _set_status(span, status, error)
    span.end()
    if _rt.task_token is not None:
        otel_context.detach(_rt.task_token)
    _rt.task_span, _rt.task_token = None, None


def emit_run_span(*, run_id: str, pipeline: str, start_ns: int, status: str, attrs: Mapping[str, Any],
                  end_ns: int | None = None) -> tuple[str, str] | None:
    """The run's root span (deterministic ids), written once when the run is finalized. Returns its ids."""
    if _rt.tracer is None:
        return None
    token = _FORCED_IDS.set((trace_id_for_run(run_id), root_span_id(run_id)))
    try:
        span = _rt.tracer.start_span(f"{pipeline}.run", context=otel_context.Context(), start_time=start_ns,
                                     attributes=attributes({"pipeline": pipeline, "run_id": run_id, **attrs}),
                                     links=_airflow_links())
    finally:
        _FORCED_IDS.reset(token)
    _set_status(span, status)
    span.end(end_time=end_ns)
    return format_trace_id(trace_id_for_run(run_id)), format_span_id(root_span_id(run_id))


def record_step(name: str, seconds: float, attrs: Mapping[str, Any],
                error: bool = False) -> tuple[str, str] | None:
    """A step that already happened (a log event with its duration) as a child span ending now.
    Returns its (trace_id, span_id), so the log line can point at the step span."""
    if _rt.tracer is None or not seconds or seconds < 0:
        return None
    end_ns = time.time_ns()
    span = _rt.tracer.start_span(name, context=_parent_context(), start_time=end_ns - int(seconds * 1e9),
                                 attributes=attributes(attrs))
    if error:
        span.set_status(Status(StatusCode.ERROR))
    span.end(end_time=end_ns)
    span_context = span.get_span_context()
    return format_trace_id(span_context.trace_id), format_span_id(span_context.span_id)


def add_event(name: str, attrs: Mapping[str, Any]) -> None:
    """A point-in-time event (e.g. a data-quality check) on the current span."""
    if _rt.tracer is None:
        return
    span = trace.get_current_span()
    if not span.get_span_context().is_valid:
        span = _rt.task_span
    if span is not None:
        span.add_event(name, attributes(attrs))


@contextmanager
def span(name: str, **attrs: Any) -> Iterator[Any]:
    """A live child span around a block: `with tracing.span("merge", entity=e): ...` (yields None when off)."""
    if _rt.tracer is None:
        yield None
        return
    with _rt.tracer.start_as_current_span(name, context=_parent_context(), attributes=attributes(attrs)) as sp:
        yield sp
