# Design 0001 — `ctrl` (Delta) and `audit` (Postgres) models

**Status:** ACCEPTED and implemented (2026-10-18). Decisions on the open questions: Q1 layer watermark in `ctrl`;
Q2 raw quarantined rows in a Delta table in bucket `audit`; Q3 90 days of mirrored logs; Q4 Pushgateway installed.

**Implementation deviation (reviewed with the owner after the fact):** the `ctrl` tables are **generic**, with a
`pipeline` column, instead of one set per project: `ctrl.download_ref`, `ctrl.download_file`, `ctrl.layer_watermark`
(the `rfb_*` names below read as `pipeline = 'rfb'`). Reason: DRY across future projects (ANP, NF-e...).
Code: `pipelines/commons/ctrl.py`, `pipelines/commons/audit.py`, DDL `infra/postgres/audit_schema.sql`.

Owner's rule: **`ctrl` = reference/resume control for downloads only. `audit` = pipeline logs and audit. Never mixed.**

---

## A. `ctrl` — Delta tables in `s3a://ctrl/` (database `ctrl` in the Hive metastore)

Purpose: remember which RFB reference months were downloaded, when, with which status, and where to resume
("202608 downloaded -> proceed to 202609"). History is kept (Delta time travel gives it for free on the state table).
Single writer: only the download task writes `ctrl` (Delta on S3 is single-writer safe).

### A1. `ctrl.rfb_download_ref` — current state, 1 row per `ref_month` (MERGE/upsert)

| Column | Type | Notes |
|---|---|---|
| `ref_month` | int | **natural key**, `YYYYMM` (202609) |
| `status` | string | `PENDING`, `DOWNLOADING`, `DOWNLOADED`, `PARTIAL`, `NOT_PUBLISHED`, `FAILED` |
| `files_expected` | int | 37 (the source contract) |
| `files_ok` | int | readable ZIPs of the announced size |
| `bytes_total` | bigint | sum of `local_bytes` |
| `landing_uri` | string | where the ZIPs are (`LANDING_URI`) |
| `attempts` | int | how many download runs touched this month |
| `first_attempt_at`, `last_attempt_at`, `downloaded_at` | timestamp (UTC) | `downloaded_at` null until complete |
| `last_run_id` | string | joins with `audit.pipeline_run.run_id` (reference only, no FK across systems) |
| `last_error` | string | short cause code, e.g. `source_incomplete` |
| `updated_at` | timestamp (UTC) | |

### A2. `ctrl.rfb_download_file` — history, 1 row per file per download run (append-only)

`ref_month`, `file_name`, `expected_bytes`, `local_bytes`, `sha256`, `http_status`, `attempts`,
`status` (`OK`, `SKIPPED_EXISTING`, `MISSING`, `CORRUPT`), `started_at`, `finished_at`, `run_id`.
~37 rows per run: tiny, no partitioning. This is the "download audit table" the owner asked for.

### Resume logic (what the code will do)
1. `next_ref` = the oldest `ref_month` in the requested range whose `status <> 'DOWNLOADED'`; if the range is
   open-ended, `max(DOWNLOADED) + 1 month`.
2. A `DOWNLOADED` month whose 37 ZIPs still validate on disk is **skipped without contacting the source**
   (datalabs ADR 0009 behaviour). `NOT_PUBLISHED` is retried on the next run (exit code 99 = skipped, not failed).
3. A crash in the middle leaves the month `DOWNLOADING`/`PARTIAL`; the next run resumes file by file.

### Open question Q1
Layer progress ("bronze loaded for 202609", "silver SCD2 applied up to 202609") is also *resume state*, and SCD2 needs
the **last applied month** to enforce ordering. Options: (a) add `ctrl.rfb_layer_watermark` (same bucket, same rules), or
(b) keep it inside each silver table's own metadata. **Recommendation: (a)**, still pure control, no logs in it.

---

## B. `audit` — Postgres schema `audit` in database `lambda` (read by Grafana as `grafana_ro`)

Purpose: everything about *how the pipelines ran*. Small, relational, Grafana-native.
Written by one helper in `pipelines/commons` (DRY). `run_id` = Airflow `run_id`, or a generated UUID for manual runs.

### B1. `audit.pipeline_run` — 1 row per DAG run
`run_id` (PK, text), `pipeline` (`rfb`), `dag_id`, `ref_month` int, `trigger` (`manual|scheduled|backfill`),
`status` (`RUNNING|SUCCESS|FAILED|PARTIAL|SKIPPED`), `started_at`, `finished_at`, `duration_s`,
`tasks_total/ok/failed`, `warn_count`, `block_count`, `host`, `git_sha`, `params` jsonb.

### B2. `audit.task_run` — 1 row per task attempt
`id` (bigserial PK), `run_id` (FK), `task_id`, `layer` (`landing|extract|bronze|silver|gold|serve`), `entity`
(`empresas`, `estabelecimentos`, ...), `attempt`, `status`, `started_at`, `finished_at`, `duration_s`,
`rows_in`, `rows_out`, `rows_rejected`, `bytes_in`, `bytes_out`, `spark_app_id`, `error_class`, `error_message`,
`metrics` jsonb (anything extra: shuffle bytes, partitions, executor time).

### B3. `audit.dq_event` — 1 row per check evaluation
`id`, `run_id`, `task_id`, `check_name`, `severity` (`WARN|BLOCK`), `status` (`PASS|FAIL`), `observed` numeric,
`threshold` numeric, `message`, `context` jsonb, `created_at`.
Severity is defined in one place in `commons` (BLOCK only for model-breaking failures).

### B4. `audit.log_event` — structured log lines mirrored (partitioned by month on `ts`)
`id` (bigserial), `ts` timestamptz, `run_id`, `task_id`, `level`, `logger`,
`event` (stable code, e.g. `download_ok`, `csv_row_repaired`),
`message` (human-readable full string),
`fields` jsonb (**parsed** key/values of the event),
`raw_line` text (the **complete original** JSON line).
Indexes: `(ts)`, `(run_id, ts)`, partial `(level, ts) WHERE level IN ('WARNING','ERROR','CRITICAL')`.
Retention: monthly partitions, drop after 90 days (Loki keeps the full stream). Mirror level `>= INFO`
(`AUDIT_LOG_MIN_LEVEL`), never per-row debug.

### B5. `audit.quarantine_summary` — 1 row per (run, file, reason)
`run_id`, `file_name`, `entity`, `reason` (`bad_quote`, `extra_columns`, `bad_encoding`, ...), `rows`,
`repaired` (bool), `sample` text (a few raw lines). Counts only; **the raw rows themselves go to a Delta table**
(see Q2) because they can be large.

### B6. Views for Grafana tables
`v_last_runs` (latest 50 runs with status/duration), `v_task_latest` (last attempt per task), `v_dq_open`
(latest failing check per name/severity), `v_logs_recent` (last 24h WARNING+), `v_download_status`
(joins `ctrl` via a periodic sync only if wanted; default: not joined, to keep ctrl and audit separate).

### Reliability rule (observability must not break the pipeline)
The audit writer **never raises into the pipeline**: events are buffered and batch-inserted; if Postgres is down they
are spooled to `/srv/scratch/audit-spool/*.jsonl` and replayed on the next run. (Metrics go to Pushgateway the same
best-effort way.)

---

## C. Metrics and alerts (design only)
Pushgateway metrics (labels `pipeline`, `task`, `layer`, `ref_month`): `lambda_task_duration_seconds`,
`lambda_task_rows_out`, `lambda_task_rows_rejected`, `lambda_dq_fail_total{severity}`,
`lambda_pipeline_last_success_timestamp`. Alerts: task failed, any BLOCK dq, no successful run in 40 days (monthly
cadence), download `NOT_PUBLISHED` for more than N days, rows_out deviating from the previous month beyond a threshold.

## D. Grafana (design only)
Dashboards: *Pipeline overview* (last runs table, duration per task, rows per month), *Data quality* (open dq events,
quarantine by reason), *Logs* (table over `v_logs_recent`, plus Loki panel for the full stream), *Downloads*
(Delta `ctrl` state mirrored on demand, or queried via DuckDB later).

## Open questions
- **Q1** — layer watermark in `ctrl` (recommended) or elsewhere?
- **Q2** — raw quarantined rows: new Delta table in a bucket `audit` (recommended: bulk data stays out of Postgres), or Postgres?
- **Q3** — keep 90 days of mirrored logs, or longer?
- **Q4** — install Pushgateway now (apt package) or defer metrics until the first task runs?
