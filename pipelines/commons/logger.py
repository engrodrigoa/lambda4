"""Structured logging: one JSON event per line, with run context on every line.

Handlers (configured once per process by `configure_logging`):
* stdout   - short human-readable line (what you read in the Airflow task log / terminal)
* file     - JSON lines in LAMBDA_LOG_DIR/<pipeline>/<YYYY-MM-DD>.jsonl, shipped to Loki by Alloy
* audit    - level >= AUDIT_LOG_MIN_LEVEL mirrored into audit.log_event (full raw line + parsed fields), for Grafana

Use `log_event(logger, "event_code", "message", **fields)` for anything a dashboard or alert may key on: the event
code is stable, the message is for humans.
"""
from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from logging.handlers import WatchedFileHandler
from typing import Any

from pipelines.commons.audit import get_sink
from pipelines.commons.settings import get_settings

ROOT = "lambda"

# mutable context stamped on every record (run/task are set by TaskRun)
LOG_CONTEXT: dict[str, Any] = {"pipeline": None, "run_id": None, "task_id": None, "ref_month": None}

_RESERVED = {"ts", "level", "logger", "pipeline", "run_id", "task_id", "ref_month", "event", "message", "exc"}
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


def log_event(logger: logging.Logger, event: str, message: str, level: int = logging.INFO,
              exc_info: bool = False, **fields: Any) -> None:
    logger.log(level, message, extra={"event": event, "fields": fields}, exc_info=exc_info)


def reset_logging() -> None:
    """Tests: remove handlers so the next configure_logging starts clean."""
    global _configured
    root = logging.getLogger(ROOT)
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()
    _configured = False
    LOG_CONTEXT.update({"pipeline": None, "run_id": None, "task_id": None, "ref_month": None})
