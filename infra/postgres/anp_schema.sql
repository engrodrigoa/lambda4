-- ANP (fuel prices) in Postgres: control state (schema ctrl) + raw landing (schema anp_bronze). ADR 0006.
-- Idempotent: safe to re-run. Apply as the schema owner (lambda, peer auth):
--   psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/anp_schema.sql
--
-- ctrl       = ONLY reference/resume control for downloads (which source file, which version, when, status).
--              Generic across scraped sources (column `pipeline`). Logs and audit stay in schema `audit`.
--              Exception to "ctrl = Delta in s3a://ctrl/" (ADR 0006): here the control row is updated in the SAME
--              transaction as the bronze load, so a file is either loaded and marked, or neither.
-- anp_bronze = rows exactly as they arrive (every column text), plus lineage columns (_file_key, _record_no, ...).

CREATE SCHEMA IF NOT EXISTS ctrl;
CREATE SCHEMA IF NOT EXISTS anp_bronze;
COMMENT ON SCHEMA ctrl IS 'download/load control of file-based sources (Postgres side; Spark pipelines use s3a://ctrl)';
COMMENT ON SCHEMA anp_bronze IS 'ANP fuel prices, raw rows as published (all text) + lineage columns';

-- ---------------------------------------------------------------- ctrl.source_file: current state per source file
-- One row per (pipeline, file_key). file_key is stable across runs and independent of the publisher's file name,
-- e.g. semestral/ca/2014-S1, mensal/glp/2026-09, semanal/glp/2026-10-20 (weekly: keyed by the page update date).
CREATE TABLE IF NOT EXISTS ctrl.source_file (
    pipeline            text        NOT NULL,
    file_key            text        NOT NULL,
    source_kind         text        NOT NULL,             -- semestral | mensal | semanal
    product_group       text        NOT NULL,             -- ca | glp | diesel-gnv | gasolina-etanol
    period_start        date        NOT NULL,
    period_end          date        NOT NULL,
    url                 text        NOT NULL,
    link_text           text,
    -- discovery
    listed              boolean     NOT NULL DEFAULT true, -- still linked from the source page
    first_seen_at       timestamptz NOT NULL DEFAULT now(),
    last_seen_at        timestamptz NOT NULL DEFAULT now(),
    remote_bytes        bigint,                            -- size announced by the source at the last check
    remote_checked_at   timestamptz,
    -- download (landing)
    download_status     text        NOT NULL DEFAULT 'PENDING'
                        CHECK (download_status IN ('PENDING', 'DOWNLOADED', 'FAILED', 'MISSING_AT_SOURCE')),
    download_attempts   int         NOT NULL DEFAULT 0,
    version             int         NOT NULL DEFAULT 0,    -- +1 for every new content downloaded
    local_path          text,
    local_bytes         bigint,
    sha256              text,
    file_format         text,                              -- csv | zip (by magic bytes, not by extension)
    downloaded_at       timestamptz,
    duplicate_of        text,                              -- file_key with the same sha256 (not loaded twice)
    -- bronze
    bronze_status       text        NOT NULL DEFAULT 'PENDING'
                        CHECK (bronze_status IN ('PENDING', 'LOADED', 'FAILED', 'SKIPPED_DUPLICATE')),
    bronze_sha256       text,                              -- content currently in bronze
    bronze_rows         bigint,
    bronze_quarantined  bigint,
    loaded_at           timestamptz,
    -- bookkeeping
    last_run_id         text,
    last_error          text,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, file_key)
);
CREATE INDEX IF NOT EXISTS source_file_pending_idx ON ctrl.source_file (pipeline, download_status, bronze_status);
COMMENT ON TABLE ctrl.source_file IS 'current control state per source file: discovered, downloaded, loaded';

-- ---------------------------------------------------------------- ctrl.source_file_event: append-only history
-- Every state change of a file (the "when was it downloaded / loaded" trail). Never updated, never deleted.
CREATE TABLE IF NOT EXISTS ctrl.source_file_event (
    id          bigserial   PRIMARY KEY,
    pipeline    text        NOT NULL,
    file_key    text        NOT NULL,
    event       text        NOT NULL
                CHECK (event IN ('DISCOVERED', 'CHANGED_AT_SOURCE', 'UNLISTED', 'RELISTED', 'DOWNLOADED',
                                 'DOWNLOAD_FAILED', 'MISSING_AT_SOURCE', 'DUPLICATE_CONTENT', 'LOADED',
                                 'LOAD_FAILED')),
    run_id      text,
    task_id     text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    version     int,
    bytes       bigint,
    sha256      text,
    rows        bigint,
    detail      jsonb
);
CREATE INDEX IF NOT EXISTS source_file_event_key_idx ON ctrl.source_file_event (pipeline, file_key, created_at);
CREATE INDEX IF NOT EXISTS source_file_event_run_idx ON ctrl.source_file_event (run_id);
COMMENT ON TABLE ctrl.source_file_event IS 'append-only history of ctrl.source_file state changes';

-- ---------------------------------------------------------------- anp_bronze.precos_raw: rows as published
-- Partitioned by source kind: the weekly churn touches only the small partition, and a reload of one file is a
-- DELETE by _file_key inside its partition (index below).
CREATE TABLE IF NOT EXISTS anp_bronze.precos_raw (
    regiao_sigla        text,
    estado_sigla        text,
    municipio           text,
    revenda             text,
    cnpj_da_revenda     text,
    nome_da_rua         text,
    numero_rua          text,
    complemento         text,
    bairro              text,
    cep                 text,
    produto             text,
    data_da_coleta      text,
    valor_de_venda      text,
    valor_de_compra     text,
    unidade_de_medida   text,
    bandeira            text,
    _source_kind        text        NOT NULL,              -- semestral | mensal | semanal
    _file_key           text        NOT NULL,              -- -> ctrl.source_file (pipeline 'anp')
    _file_sha256        text        NOT NULL,              -- exact content version the row came from
    _record_no          bigint      NOT NULL,              -- 1-based record number inside the file (header excluded)
    _run_id             text        NOT NULL,
    _ingested_at        timestamptz NOT NULL
) PARTITION BY LIST (_source_kind);
CREATE TABLE IF NOT EXISTS anp_bronze.precos_raw_semestral PARTITION OF anp_bronze.precos_raw FOR VALUES IN ('semestral');
CREATE TABLE IF NOT EXISTS anp_bronze.precos_raw_mensal    PARTITION OF anp_bronze.precos_raw FOR VALUES IN ('mensal');
CREATE TABLE IF NOT EXISTS anp_bronze.precos_raw_semanal   PARTITION OF anp_bronze.precos_raw FOR VALUES IN ('semanal');
CREATE INDEX IF NOT EXISTS precos_raw_file_idx ON anp_bronze.precos_raw (_file_key);
CREATE INDEX IF NOT EXISTS precos_raw_ingested_idx ON anp_bronze.precos_raw (_ingested_at);
COMMENT ON TABLE anp_bronze.precos_raw IS 'ANP fuel price survey, every published file, raw text; overlaps between '
    'semestral, monthly and weekly files are kept here and resolved in silver';

-- ---------------------------------------------------------------- anp_bronze.precos_quarantine: refused records
CREATE TABLE IF NOT EXISTS anp_bronze.precos_quarantine (
    _source_kind    text        NOT NULL,
    _file_key       text        NOT NULL,
    _file_sha256    text        NOT NULL,
    _record_no      bigint,
    reason          text        NOT NULL,                  -- field_count_<n> | ...
    raw_record      text,
    _run_id         text        NOT NULL,
    _quarantined_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS precos_quarantine_file_idx ON anp_bronze.precos_quarantine (_file_key);
COMMENT ON TABLE anp_bronze.precos_quarantine IS 'records the bronze loader refused, kept raw with the reason';

-- ---------------------------------------------------------------- anp_bronze.batch_profile: survey batch statistics
-- A survey batch ("lote de pesquisa") = one source file x one collection month (YYYYMM of data_da_coleta): a monthly
-- file is one batch, a semester file six. Computed by anp03_bronze from the rows it just loaded, in the SAME
-- transaction (replaced on reload), so Grafana reads a few rows per file instead of scanning precos_raw.
-- produto = '*' is the whole batch (all products). Prices parsed from the raw text ('2,899'); unparseable -> missing.
CREATE TABLE IF NOT EXISTS anp_bronze.batch_profile (
    file_key        text        NOT NULL,              -- -> ctrl.source_file
    file_sha256     text        NOT NULL,
    source_kind     text        NOT NULL,
    product_group   text        NOT NULL,
    batch_month     int         NOT NULL,              -- YYYYMM of the collection date; 0 = date not parseable
    produto         text        NOT NULL,              -- upper(trim(produto)), '*' = all products
    rows            bigint      NOT NULL,
    stations        bigint      NOT NULL,              -- distinct CNPJ (digits only)
    brands          bigint      NOT NULL,              -- distinct bandeira
    cities          bigint      NOT NULL,              -- distinct municipio/UF
    ufs             bigint      NOT NULL,
    price_avg       numeric,                           -- valor_de_venda (GLP: per 13 kg bottle)
    price_min       numeric,
    price_p50       numeric,
    price_max       numeric,
    price_stddev    numeric,
    price_missing   bigint      NOT NULL,              -- empty or not a number
    buy_price_avg   numeric,                           -- valor_de_compra (mostly empty)
    run_id          text        NOT NULL,
    profiled_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (file_key, batch_month, produto)
);
COMMENT ON TABLE anp_bronze.batch_profile IS 'statistics per survey batch (file x collection month) and product, '
    'computed at bronze load time; produto = ''*'' is the whole batch';

-- one row per survey batch, average sale price of each product in columns (the Grafana matrix)
CREATE OR REPLACE VIEW anp_bronze.v_batch_matrix AS
SELECT CASE WHEN batch_month = 0 THEN 'no date'
            ELSE left(batch_month::text, 4) || '-' || right(batch_month::text, 2) END       AS batch,
       batch_month,
       file_key,
       source_kind,
       product_group,
       max(rows)          FILTER (WHERE produto = '*')                   AS rows,
       max(stations)      FILTER (WHERE produto = '*')                   AS stations,
       max(brands)        FILTER (WHERE produto = '*')                   AS brands,
       max(cities)        FILTER (WHERE produto = '*')                   AS cities,
       max(ufs)           FILTER (WHERE produto = '*')                   AS ufs,
       max(price_avg)     FILTER (WHERE produto = 'GASOLINA')            AS gasolina,
       max(price_avg)     FILTER (WHERE produto = 'GASOLINA ADITIVADA')  AS gasolina_aditivada,
       max(price_avg)     FILTER (WHERE produto = 'ETANOL')              AS etanol,
       max(price_avg)     FILTER (WHERE produto = 'DIESEL')              AS diesel,
       max(price_avg)     FILTER (WHERE produto = 'DIESEL S10')          AS diesel_s10,
       max(price_avg)     FILTER (WHERE produto = 'DIESEL S50')          AS diesel_s50,
       max(price_avg)     FILTER (WHERE produto = 'GNV')                 AS gnv,
       max(price_avg)     FILTER (WHERE produto = 'GLP')                 AS glp,
       coalesce(sum(rows) FILTER (WHERE produto NOT IN ('*', 'GASOLINA', 'GASOLINA ADITIVADA', 'ETANOL', 'DIESEL',
                                                        'DIESEL S10', 'DIESEL S50', 'GNV', 'GLP')), 0) AS other_product_rows,
       max(price_missing) FILTER (WHERE produto = '*')                   AS price_missing,
       max(profiled_at)                                                  AS profiled_at
FROM anp_bronze.batch_profile
GROUP BY batch_month, file_key, source_kind, product_group;
COMMENT ON VIEW anp_bronze.v_batch_matrix IS 'one row per survey batch, average sale price per product in columns';

-- ---------------------------------------------------------------- read-only access (Grafana, owner's DBeaver)
GRANT USAGE ON SCHEMA ctrl, anp_bronze TO grafana_ro, dbeaver_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA ctrl, anp_bronze TO grafana_ro, dbeaver_ro;
ALTER DEFAULT PRIVILEGES FOR ROLE lambda IN SCHEMA ctrl, anp_bronze GRANT SELECT ON TABLES TO grafana_ro, dbeaver_ro;
