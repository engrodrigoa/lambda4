"""Airflow operators shared by the DAG files (imported inside the Airflow virtualenv, never by pipeline code)."""
from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator


class TracedBashOperator(BashOperator):
    """BashOperator that hands Airflow's current span (`worker.<task>`, when AIRFLOW__TRACES__OTEL_ON) to the process
    as W3C TRACEPARENT; pipelines/commons/tracing.py turns it into a span link."""

    def get_env(self, context) -> dict:
        env = super().get_env(context)
        try:
            from opentelemetry.propagate import inject

            carrier: dict[str, str] = {}
            inject(carrier)
            if carrier.get("traceparent"):
                env["TRACEPARENT"] = carrier["traceparent"]
        except Exception:  # noqa: BLE001 - tracing must never fail a task
            pass
        return env
