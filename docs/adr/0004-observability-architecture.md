# ADR 0004 — Observability architecture for the pipelines

**Status:** accepted (2026-10-21)

## Context

Observability and reliability are the premise of every lab project. Batch pipelines need four kinds of signal that
are easy to conflate: logs (what happened), metrics (how much / how long over time), audit (who ran what, with which
result, queryable as data) and control state (where to resume). Grafana, Prometheus, Loki and Alloy already run here;
Airflow 3 ships OpenTelemetry support; no trace backend (Tempo/Jaeger) is installed.

## Decision

* **Control ≠ audit.** `ctrl` (Delta) only holds resume state. Everything about *how runs went* goes to the Postgres
  schema `audit`, written best-effort (spool + replay: observability never breaks the pipeline).
* **Logs:** one JSON event per line with a stable `event` code; shipped by Alloy to Loki (labels: job, pipeline, task,
  level — low cardinality) and mirrored (INFO+) to `audit.log_event` for SQL tables in Grafana. Airflow's raw task
  logs go to Loki too (labels dag_id, task_id, attempt).
* **Metrics:** batch tasks push to the Pushgateway (pushadd per pipeline/task, so "last success" survives a failure);
  Airflow emits OpenTelemetry metrics → Alloy (OTLP/HTTP :4318) → Prometheus remote write.
* **Tracing (phase 1):** a run is rendered as a trace from the audit tables (`audit.v_trace_spans`: run → task
  attempts → timed steps) in Grafana's Traces panel. No new infrastructure.
* **Tracing (phase 2, proposed):** Grafana Tempo + OpenTelemetry SDK spans in `TaskRun` (and Airflow 3 OTel traces),
  linking logs and metrics by `trace_id`. Requires installing Tempo and the OTel SDK in the data env.
* **Data-level signals** (the "five pillars": freshness, volume, schema, distribution, lineage): freshness via
  watermarks/last success; volume via rows per task and per Delta table; schema via the contract + PERMISSIVE parse;
  distribution via DQ checks (null/invalid/dup/change rate); lineage via run_id ↔ task ↔ Delta version.
* **Dashboards as code** (`infra/grafana/build_dashboards.py`) with a validator that runs every query; alerts as
  Prometheus rules in git.

## Consequences

* Each failure mode has an owner signal: infra (preflight health), orchestration (Airflow metrics/DB), execution
  (task audit, Spark UI), data (dq_event, quarantine), storage (delta_table_stats).
* Two log stores (Loki for search/streaming, Postgres for SQL/joins) cost a little duplication but let audit tables
  be joined with runs and checks.
* The trace panel is SQL-built until Tempo exists; spans have no OTel context propagation yet.
