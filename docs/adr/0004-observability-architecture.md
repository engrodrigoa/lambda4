# ADR 0004 — Observability architecture for the pipelines

**Status:** accepted (2026-10-21); phase 2 tracing implemented 2026-10-21

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
* **Tracing (phase 2, implemented):** Grafana Tempo 3.1 (on demand, SSD, 127.0.0.1) + OpenTelemetry SDK spans
  (`pipelines/commons/tracing.py`): OTLP/HTTP → Alloy :4318 → Tempo :4417. One trace per run with ids derived from
  the run id (`trace_id = md5(run_id)`, as in `audit.v_trace_spans`); one span per task attempt (`TaskRun`); timed
  log events (`seconds` / `latency_ms`) become child spans; DQ checks are span events; `finalize_run` writes the root
  span. Airflow 3 traces (`AIRFLOW__TRACES__OTEL_ON`) go to the same Tempo; our spans *link* to Airflow's
  `worker.<task>` span (W3C `TRACEPARENT` passed by the DAG). Logs carry `trace_id`/`span_id` (JSON + Loki
  structured metadata) and Grafana links both ways. Tempo's metrics-generator adds span RED metrics to Prometheus.
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
* Deterministic ids instead of context propagation: every task process computes the same trace id from the run id,
  so no carrier has to travel between Airflow workers, retries or manual shells, and audit rows join to traces by
  `md5(run_id)`. The price: our run trace and Airflow's trace are two traces joined by span links, not one tree
  (Airflow 3.3 creates its DAG-run trace id at random and does not let a DAG choose it).
* The SQL-built trace panel stays as a fallback (Tempo stopped, runs that predate tracing); it now also reads
  `latency_ms` steps, so both show the same tree.
* Tracing is best-effort like audit and metrics: disabled by `LAMBDA_OTEL_ENABLED=0`, by a missing SDK, or when no
  collector answers at start-up (one warning, no delay); spans are exported asynchronously and flushed with a 5 s cap.
* Tempo and Alloy are on-demand services: start them with the rest of the stack before a run, or the run simply has
  may have no trace (preflight reports both as WARN; with Alloy up, Alloy holds spans up to 10 min for Tempo).
