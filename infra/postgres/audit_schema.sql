-- audit schema: pipeline logs and audit, mirrored for Grafana (design docs/design/0001-ctrl-and-audit-model.md).
-- Control state (download references, layer watermarks) is NOT here: it lives in the Delta tables of s3a://ctrl/.
-- Idempotent: safe to re-run. Apply as the schema owner:
--   psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/audit_schema.sql
-- Grafana reads through role grafana_ro (USAGE + default SELECT privileges granted on schema creation).

SET search_path = audit, public;

-- ---------------------------------------------------------------- pipeline_run: one row per DAG run
CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    run_id        text PRIMARY KEY,                 -- Airflow run_id, or manual__<utc> for manual runs
    pipeline      text        NOT NULL,             -- e.g. rfb
    dag_id        text,
    ref_month     int,                              -- YYYYMM
    trigger       text,                             -- manual | scheduled | backfill
    status        text        NOT NULL DEFAULT 'RUNNING'
                  CHECK (status IN ('RUNNING','SUCCESS','FAILED','PARTIAL','SKIPPED')),
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    duration_s    numeric GENERATED ALWAYS AS (extract(epoch FROM finished_at - started_at)) STORED,
    tasks_total   int NOT NULL DEFAULT 0,
    tasks_ok      int NOT NULL DEFAULT 0,
    tasks_failed  int NOT NULL DEFAULT 0,
    warn_count    int NOT NULL DEFAULT 0,
    block_count   int NOT NULL DEFAULT 0,
    host          text,
    git_sha       text,
    params        jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS pipeline_run_started_idx ON audit.pipeline_run (pipeline, started_at DESC);

-- ---------------------------------------------------------------- task_run: one row per task attempt
CREATE TABLE IF NOT EXISTS audit.task_run (
    id             bigserial PRIMARY KEY,
    run_id         text NOT NULL REFERENCES audit.pipeline_run (run_id) ON DELETE CASCADE,
    task_id        text NOT NULL,
    attempt        int  NOT NULL DEFAULT 1,
    pipeline       text NOT NULL,
    layer          text CHECK (layer IN ('landing','extract','bronze','silver','gold','serve','ctrl','other')),
    entity         text,
    ref_month      int,
    status         text NOT NULL DEFAULT 'RUNNING'
                   CHECK (status IN ('RUNNING','SUCCESS','FAILED','SKIPPED')),
    exit_code      int,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    duration_s     numeric GENERATED ALWAYS AS (extract(epoch FROM finished_at - started_at)) STORED,
    rows_in        bigint,
    rows_out       bigint,
    rows_rejected  bigint,
    bytes_in       bigint,
    bytes_out      bigint,
    files_total    int,
    files_ok       int,
    files_failed   int,
    files_skipped  int,
    warn_count     int NOT NULL DEFAULT 0,
    block_count    int NOT NULL DEFAULT 0,
    spark_app_id   text,
    error_class    text,
    error_message  text,
    metrics        jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (run_id, task_id, attempt)
);
CREATE INDEX IF NOT EXISTS task_run_task_idx ON audit.task_run (pipeline, task_id, started_at DESC);

-- ---------------------------------------------------------------- dq_event: one row per check evaluation
CREATE TABLE IF NOT EXISTS audit.dq_event (
    id          bigserial PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now(),
    run_id      text NOT NULL,
    task_id     text,
    pipeline    text NOT NULL,
    ref_month   int,
    check_name  text NOT NULL,
    severity    text NOT NULL CHECK (severity IN ('WARN','BLOCK')),
    status      text NOT NULL CHECK (status IN ('PASS','FAIL')),
    observed    numeric,
    threshold   numeric,
    message     text,
    context     jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS dq_event_check_idx ON audit.dq_event (pipeline, check_name, created_at DESC);
CREATE INDEX IF NOT EXISTS dq_event_fail_idx  ON audit.dq_event (created_at DESC) WHERE status = 'FAIL';

-- ---------------------------------------------------------------- log_event: structured log lines (monthly partitions)
CREATE TABLE IF NOT EXISTS audit.log_event (
    id        bigserial,
    ts        timestamptz NOT NULL,
    run_id    text,
    task_id   text,
    pipeline  text,
    level     text NOT NULL,
    logger    text,
    event     text,          -- stable event code, e.g. download_ok, source_incomplete
    message   text,          -- human-readable full string
    fields    jsonb NOT NULL DEFAULT '{}'::jsonb,   -- parsed key/values of the event
    raw_line  text,          -- the complete original JSON line
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);
-- catch-all so an insert never fails for lack of a partition (observability must not break the pipeline)
CREATE TABLE IF NOT EXISTS audit.log_event_default PARTITION OF audit.log_event DEFAULT;
CREATE INDEX IF NOT EXISTS log_event_ts_idx   ON audit.log_event (ts DESC);
CREATE INDEX IF NOT EXISTS log_event_run_idx  ON audit.log_event (run_id, ts);
CREATE INDEX IF NOT EXISTS log_event_warn_idx ON audit.log_event (level, ts DESC)
    WHERE level IN ('WARNING','ERROR','CRITICAL');

-- Create the monthly partitions for [this month - 1, this month + months_ahead].
CREATE OR REPLACE FUNCTION audit.ensure_log_partitions(months_ahead int DEFAULT 2) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    m    date;
    name text;
BEGIN
    FOR i IN -1..months_ahead LOOP
        m := (date_trunc('month', now()) + make_interval(months => i))::date;
        name := format('log_event_%s', to_char(m, 'YYYYMM'));
        IF to_regclass('audit.' || name) IS NULL THEN
            EXECUTE format('CREATE TABLE audit.%I PARTITION OF audit.log_event FOR VALUES FROM (%L) TO (%L)',
                           name, m, (m + interval '1 month')::date);
        END IF;
    END LOOP;
END $$;

-- Drop monthly partitions that ended more than keep_days ago (Loki keeps the full stream).
CREATE OR REPLACE FUNCTION audit.drop_old_log_partitions(keep_days int DEFAULT 90) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    r       record;
    dropped int := 0;
BEGIN
    FOR r IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        JOIN pg_namespace n ON n.oid = p.relnamespace
        WHERE n.nspname = 'audit' AND p.relname = 'log_event' AND c.relname ~ '^log_event_[0-9]{6}$'
    LOOP
        IF (to_date(right(r.relname, 6), 'YYYYMM') + interval '1 month') < now() - make_interval(days => keep_days) THEN
            EXECUTE format('DROP TABLE audit.%I', r.relname);
            dropped := dropped + 1;
        END IF;
    END LOOP;
    RETURN dropped;
END $$;

SELECT audit.ensure_log_partitions(2);

-- ---------------------------------------------------------------- quarantine_summary: counts of rejected/repaired rows
-- The raw rows themselves go to the Delta table in s3a://audit/ (bulk data stays out of Postgres).
CREATE TABLE IF NOT EXISTS audit.quarantine_summary (
    id          bigserial PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now(),
    run_id      text NOT NULL,
    task_id     text,
    pipeline    text NOT NULL,
    ref_month   int,
    entity      text,
    file_name   text,
    reason      text NOT NULL,      -- bad_quote | extra_columns | missing_columns | bad_encoding | ...
    rows        bigint NOT NULL,
    repaired    boolean NOT NULL,
    sample      text                -- a few raw lines, for eyeballing in Grafana
);
CREATE INDEX IF NOT EXISTS quarantine_run_idx ON audit.quarantine_summary (run_id);

-- ---------------------------------------------------------------- views for Grafana tables
CREATE OR REPLACE VIEW audit.v_last_runs AS
SELECT run_id, pipeline, ref_month, trigger, status, started_at, finished_at, round(duration_s) AS duration_s,
       tasks_total, tasks_ok, tasks_failed, warn_count, block_count
FROM audit.pipeline_run
ORDER BY started_at DESC
LIMIT 50;

CREATE OR REPLACE VIEW audit.v_task_latest AS
SELECT DISTINCT ON (pipeline, task_id, entity)
       pipeline, task_id, layer, entity, ref_month, run_id, attempt, status, exit_code, started_at,
       round(duration_s) AS duration_s, rows_in, rows_out, rows_rejected, bytes_out,
       files_ok, files_failed, files_skipped, warn_count, block_count, error_class, error_message
FROM audit.task_run
ORDER BY pipeline, task_id, entity, started_at DESC;

CREATE OR REPLACE VIEW audit.v_dq_open AS
SELECT DISTINCT ON (pipeline, check_name)
       pipeline, check_name, severity, status, observed, threshold, message, ref_month, run_id, created_at
FROM audit.dq_event
ORDER BY pipeline, check_name, created_at DESC;

CREATE OR REPLACE VIEW audit.v_logs_recent AS
SELECT ts, level, pipeline, task_id, run_id, event, message, fields
FROM audit.log_event
WHERE ts > now() - interval '24 hours' AND level IN ('WARNING','ERROR','CRITICAL')
ORDER BY ts DESC;

GRANT USAGE ON SCHEMA audit TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO grafana_ro;
