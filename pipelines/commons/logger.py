"""Structured logging: one JSON event per line, with run context on every line.

Handlers (configured once per process by `configure_logging`):
* stdout   - short human-readable line (what you read in the Airflow task log / terminal)
* file     - JSON lines in LAMBDA_LOG_DIR/<pipeline>/<YYYY-MM-DD>.jsonl, shipped to Loki by Alloy
* audit    - level >= AUDIT_LOG_MIN_LEVEL mirrored into audit.log_event (full raw line + parsed fields), for Grafana

Use `log_event(logger, "event_code", "message", **fields)` for anything a dashboard or alert may key on: the event
code is stable, the message is for humans.

Traces: while tracing is on, every line carries `trace_id`/`span_id` (Grafana jumps between Loki and Tempo with them),
and an event with a duration (`seconds`, or `latency_ms`) also becomes a child span of the task's span.
"""
from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from logging.handlers import WatchedFileHandler
from typing import Any

from pipelines.commons import tracing
from pipelines.commons.audit import get_sink
from pipelines.commons.settings import get_settings

ROOT = "lambda"

# mutable context stamped on every record (run/task are set by TaskRun)
LOG_CONTEXT: dict[str, Any] = {"pipeline": None, "run_id": None, "task_id": None, "ref_month": None}

_RESERVED = {"ts", "level", "logger", "pipeline", "run_id", "task_id", "ref_month", "event", "message", "exc",
             "trace_id", "span_id"}
_SECRET_KEY = re.compile(r"(password|passwd|secret|token|access_key|api_key|credential)", re.IGNORECASE)
_configured = False


def _redact(fields: dict[str, Any]) -> dict[str, Any]:
    return {k: ("***" if _SECRET_KEY.search(k) else v) for k, v in fields.items()}


def _event_dict(record: logging.LogRecord) -> dict[str, Any]:
    out: dict[str, Any] = {
        "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
        "level": record.levelname,
        "logger": record.name,
        **{k: LOG_CONTEXT.get(k) for k in ("pipeline", "run_id", "task_id", "ref_month")},
        "event": getattr(record, "event", None) or "log",
        "message": record.getMessage(),
    }
    trace_id, span_id = getattr(record, "trace_ids", None) or tracing.current_ids()
    if trace_id:
        out["trace_id"], out["span_id"] = trace_id, span_id
    for key, value in _redact(getattr(record, "fields", None) or {}).items():
        out[f"field_{key}" if key in _RESERVED else key] = value
    if record.exc_info:
        out["exc"] = logging.Formatter().formatException(record.exc_info)
    return out


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(_event_dict(record), default=str, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        d = _event_dict(record)
        extras = " ".join(f"{k}={v}" for k, v in d.items()
                          if k not in _RESERVED and v is not None)
        event = "" if d["event"] == "log" else f"[{d['event']}] "
        line = f"{d['ts']} {d['level']:<7} {event}{d['message']}" + (f" | {extras}" if extras else "")
        return line + (f"\n{d['exc']}" if "exc" in d else "")


class AuditHandler(logging.Handler):
    """Mirrors records into audit.log_event via the best-effort sink. Ignores the sink's own logger (no loops)."""

    def emit(self, record: logging.LogRecord) -> None:
        if record.name.startswith("lambda.audit"):
            return
        try:
            d = _event_dict(record)
            fields = {k: v for k, v in d.items() if k not in _RESERVED}
            if "exc" in d:
                fields["exc"] = d["exc"]
            if "trace_id" in d:
                fields["trace_id"], fields["span_id"] = d["trace_id"], d["span_id"]
            get_sink().log({
                "ts": d["ts"], "run_id": d["run_id"], "task_id": d["task_id"], "pipeline": d["pipeline"],
                "level": d["level"], "logger": d["logger"], "event": d["event"], "message": d["message"],
                "fields": json.loads(json.dumps(fields, default=str)),
                "raw_line": json.dumps(d, default=str, ensure_ascii=False),
            })
        except Exception:  # noqa: BLE001 - logging must never break the pipeline
            self.handleError(record)


def configure_logging(pipeline: str, level: int = logging.INFO) -> logging.Logger:
    """Idempotent. Sets the pipeline name on the context and installs the three handlers on the `lambda` logger."""
    global _configured
    LOG_CONTEXT["pipeline"] = pipeline
    root = logging.getLogger(ROOT)
    if _configured:
        return root
    s = get_settings()
    root.setLevel(level)
    root.propagate = False

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(ConsoleFormatter())
    root.addHandler(console)

    log_file = s.log_dir / pipeline / f"{datetime.now(UTC):%Y-%m-%d}.jsonl"
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = WatchedFileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(JsonFormatter())
        root.addHandler(file_handler)
    except OSError as exc:
        root.warning("file logging disabled (%s): %s", log_file, exc)

    audit_handler = AuditHandler(level=getattr(logging, s.audit_log_min_level, logging.INFO))
    root.addHandler(audit_handler)
    _configured = True
    return root


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"{ROOT}.{name}")


# fields that name WHAT a step worked on, low-cardinality: they qualify the span name ("health_check:s3")
_STEP_QUALIFIERS = ("component", "target", "table", "entity")


def _step_name(event: str, fields: dict[str, Any]) -> str:
    qualifier = next((fields[k] for k in _STEP_QUALIFIERS if fields.get(k)), None)
    return f"{event}:{qualifier}" if qualifier else event


def _step_seconds(fields: dict[str, Any]) -> float | None:
    for key, scale in (("seconds", 1.0), ("latency_ms", 0.001)):
        value = fields.get(key)
        if isinstance(value, int | float) and not isinstance(value, bool) and value > 0:
            return value * scale
    return None


def log_event(logger: logging.Logger, event: str, message: str, level: int = logging.INFO,
              exc_info: bool = False, trace_ids: tuple[str, str] | None = None, **fields: Any) -> None:
    """`trace_ids` (trace_id, span_id) points the line at a span other than the current one."""
    extra: dict[str, Any] = {"event": event, "fields": fields, "trace_ids": trace_ids}
    seconds = _step_seconds(fields) if tracing.enabled() else None
    if seconds is not None:
        # a timed step becomes a span that ends now; the log line points at that span
        extra["trace_ids"] = tracing.record_step(_step_name(event, fields), seconds,
                                                 {**_redact(fields), "message": message}, error=level >= logging.ERROR)
    logger.log(level, message, extra=extra, exc_info=exc_info)


def reset_logging() -> None:
    """Tests: remove handlers so the next configure_logging starts clean."""
    global _configured
    root = logging.getLogger(ROOT)
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()
    _configured = False
    LOG_CONTEXT.update({"pipeline": None, "run_id": None, "task_id": None, "ref_month": None})
