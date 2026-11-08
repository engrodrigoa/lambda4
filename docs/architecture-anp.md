# lambda4 — ANP fuel prices architecture (up to bronze)

Reference for the ANP pipeline (fuel price survey, full history since 2004): what runs, where data lives, which script
writes which table, how it is observed. Decisions: ADR 0006. Study material in Portuguese: `docs/learning/08`.
Silver, gold and the Streamlit app are built by the owner on top of `anp_bronze.precos_raw` (dbt, section 5).

## 1. End-to-end flow

```mermaid
flowchart TB
    P["ANP portal page<br/>224 files: semestral / mensal / semanal"] --> D["anp01 discover<br/>classify links, size probes"]
    D --> C[("ctrl.source_file<br/>+ source_file_event")]
    C --> W["anp02 download<br/>sequential, sha256, validated"]
    W --> L["landing (HDD 1 TB)<br/>anp/kind/product/label__sha12.csv|zip"]
    L --> B["anp03 bronze<br/>csv scan + Polars, COPY"]
    B --> R[("anp_bronze.precos_raw<br/>+ precos_quarantine")]
    B --> C
    R -.-> DBT["dbt (owner): silver → gold"] -.-> ST["Streamlit (later)"]
```

## 2. The DAG `anp_pipeline` (`airflow/dags/dag_anp.py`)

`preflight → discover → download → bronze → finalize`, daily 08:00 America/Sao_Paulo, **created paused**.

| Task | Exit 99 (skipped) when | Notes |
|---|---|---|
| preflight | — | BLOCK: Postgres + ANP tables, landing mounted. WARN: disks, portal, observability |
| discover | nothing to download and nothing waiting for bronze | downstream is skipped too |
| download | nothing pending | retries 2; failed files stay FAILED and are retried next run |
| bronze | nothing pending | `trigger_rule=none_failed`: also runs when download had nothing new; outlet asset `anp_bronze.precos_raw` |
| finalize | — | `all_done`, closes `audit.pipeline_run`, fails if the run failed |

Params: `full_recheck` (probe every downloaded file), `kinds` and `limit` (a small manual run, e.g. `semanal`, 3).

## 3. Lineage

| Script | Reads | Writes |
|---|---|---|
| `anp00_preflight` | Postgres catalog, disks, portal | `audit.health_check` |
| `anp01_discover` | portal page, ranged GETs, `ctrl.source_file` | `ctrl.source_file` (new / changed / unlisted), events DISCOVERED, CHANGED_AT_SOURCE, UNLISTED, RELISTED |
| `anp02_download` | `ctrl.source_file` (pending) | landing files; ctrl DOWNLOADED / FAILED / MISSING_AT_SOURCE / duplicate; events |
| `anp03_bronze` | landing files (ZIPs extracted to `/srv/scratch/anp-extract`, deleted after) | `anp_bronze.precos_raw`, `precos_quarantine`, ctrl LOADED + event, all in ONE transaction per file; `audit.quarantine_summary` |

## 4. Data model

**`ctrl.source_file`** (one row per `(pipeline, file_key)`): source kind, product group, period, URL, link text;
discovery (`listed`, first/last seen, `remote_bytes`); download (`download_status`, `version`, `local_path`,
`sha256`, `file_format`, `duplicate_of`); bronze (`bronze_status`, `bronze_sha256`, `bronze_rows`,
`bronze_quarantined`, `loaded_at`). **`ctrl.source_file_event`**: append-only history of every change.

**`anp_bronze.precos_raw`** — partitions `precos_raw_semestral | _mensal | _semanal`:

| Column | Content |
|---|---|
| `regiao_sigla` … `bandeira` (16) | text exactly as published (e.g. `" 49.051.667/0001-02"`, `"2,899"`, `"02/01/2014"`); empty → NULL |
| `_source_kind`, `_file_key` | which file (→ `ctrl.source_file`) |
| `_file_sha256` | which content version |
| `_record_no` | record number inside the file (header excluded) |
| `_run_id`, `_ingested_at` | which run loaded it (→ `audit.pipeline_run`) |

Overlap: since 2023 the same collection can be in a semester, a month and a weekly file. Bronze keeps all of them.

**`anp_bronze.batch_profile`** — statistics per **survey batch** (*lote de pesquisa* = one file × one collection
month, `batch_month` = YYYYMM of `data_da_coleta`, 0 = unparseable date) and product (`produto = '*'` = the whole
batch): `rows`, `stations` (distinct CNPJ digits), `brands`, `cities`, `ufs`, `price_avg/min/p50/max/stddev`,
`price_missing`, `buy_price_avg`. Written by anp03 from the rows it just loaded, in the same transaction (replaced on
reload); `python -m pipelines.anp.anp03_bronze --reprofile` recomputes it for loaded files. Prices and dates are
parsed only here (profiling); bronze stays raw. **`anp_bronze.v_batch_matrix`**: one row per batch with the average
price of each product in columns (gasolina … glp), stations, brands, `other_product_rows`, `price_missing`.

## 5. dbt layers (`dbt_lambda/`): silver, data quality, gold

Three DAGs chained by Airflow assets: `anp_pipeline` → `ANP_BRONZE` → `anp_dbt` (silver + anp_dq) → `ANP_SILVER` →
`anp_dbt_gold` → `ANP_GOLD` (assets in `pipelines/commons/assets.py`, Cosmos config in
`pipelines/commons/airflow_dbt.py`). dbt runs from `dbt_lambda/.venv`, profile in the project (socket + peer auth).
Handoff and decisions: `docs/handoff/2026-11-08_dbt_anp_silver.md`.

**Silver (`anp_silver`).** `stg_anp__precos` (view: typing identical to the bronze `PROFILE_SQL`) →
`anp__precos_coleta` (incremental, delete+insert per month × product_group). One source per month × group:
**mensal > semanal > semestral** (a month is rebuilt whole when a new file touches it); exact duplicates (`row_hash`
over the raw columns) collapsed (`qt_publicacoes`); several prices for the same posto/produto/dia kept and flagged
(`fl_conflito_preco`); `fl_preco_suspeito` = price outside [0.5, 2.0] × median of month × produto × UF (3 rows of
31.6 M, all typos). Contract: NOT NULL on every key/measure/location column; nullable only where the source does not
publish (`valor_compra`, `complemento`, `bairro`, `numero`, `logradouro`).

**Data quality (`anp_dq`).** `dq_anp__carga_mes` (bronze → silver funnel per month × group), `dq_anp__qualidade_lote`
(price gate per batch = month × produto: avg/median/min/max/p01/p99/CV, suspicious, MoM change of the median and of
the stations; warn tests judge only the last 3 months), `dq_anp__qualidade_lote_uf`, `dq_anp__precos_suspeitos`.
Elementary (`anp_elementary`) records every model/test result. Grafana board `lambda4-anp-dbt`.

**Gold (`anp_gold`), star schema:**

| Object | Grain / key | Notes |
|---|---|---|
| `dim_calendario` | day (`data`) | 2004 → end of the current year; ISO week, month, quarter, weekday |
| `dim_produto` | `produto` (natural key) | group, unit (`R$ / litro`, `R$ / 13 kg`, `R$ / m³`), display order |
| `dim_municipio` | `md5(uf\|municipio)` | 901 municipalities (no IBGE code in the source) |
| `dim_posto` | `md5(cnpj\|first collection of the version)` | **SCD2 derived from the collection timeline** (147,349 versions, 87,058 CNPJs) |
| `fct_preco_diario` | posto × produto × dia | 31.6 M rows, incremental by month; avg/min/max/`qt_precos`, `fl_preco_suspeito`, `bandeira_publicada` |
| `ag_preco_posto_atual` | posto × produto | last + previous price (18 months), current posto version: Streamlit cards/matrix |
| `ag_preco_municipio_diario` | dia × município × produto | `soma_preco_venda` + `qtd_postos` for exact UF/Brasil/week roll-ups: Streamlit history |
| `ag_preco_semanal_uf` | semana × UF × produto | Grafana / BI |

Why the SCD2 is derived from the data (not a `dbt snapshot`): the 20 years arrive at once, a snapshot would only
record changes from now on. Rules measured on 2026-11-08:
* attributes come from the **fuel** rows: in the GLP files "bandeira" is the gas distributor (Liquigás, Supergasbras),
  not the station brand; 50,407 CNPJs appear only in GLP (gas resellers, `tipo_revenda = revenda_glp`);
* a **BRANCA stretch between two stretches of the same brand** is a source omission and inherits the brand: the ANP
  published BRANCA instead of the brand in several months (Aug–Nov 2005 the BRANCA share jumped from 28% to 60%);
  without the rule 22,829 false stretches became versions;
* products of the same station on the same day always agree on the attributes (0 divergences in 8.25 M station-days).

Serving marts exclude suspicious prices; the fact keeps them flagged. Tests: grain of every table, relationships
fact → dims, SCD2 without overlap/gap and exactly one current version per CNPJ, silver → fact and fact → mart
reconciliation (rows and summed price). Full build 11.5 min (fact 5.7 min); incremental with no new data: 15 s.

## 6. Observability

**Grafana dashboard `lambda4-anp` — "ANP · Pipeline health & survey batches"** (folder *Lambda4*, built by
`infra/grafana/build_dashboards.py`, variables: run, file kind, product):

* **Section 1 · Pipeline health:** run status/duration/failed tasks/files and bytes downloaded/rows loaded/DQ
  fails/quarantine; tasks of the run, task durations, checks, preflight probes, the run's trace (audit SQL); source
  files from `ctrl` (known/downloaded/loaded/pending/failed, by kind and state, files needing attention, control
  events per day and the latest ones); Airflow `anp_pipeline` runs and task instances, Airflow task logs (Loki
  `job=airflow-tasks`), pipeline events by level and warnings/errors (`audit.log_event`).
* **Section 2 · Survey batches:** the batch matrix (`v_batch_matrix`: average price per product in columns,
  stations = distinct CNPJ, brands); months covered; monthly average price per product (fuels and GLP separately,
  one source per month: semestral > mensal > semanal); stations, brands, rows, cities/UFs per batch and file family;
  price dispersion per batch and product (min/p50/avg/max, stddev, cv %); data quality per file (quarantine, rows
  without a date or price, products outside the matrix). Survey panels show the whole history (25 y) whatever the
  dashboard time range.

Load cost (measured 2026-10-23 on a 915 k-row semester, rolled back): read 1.8 s; COPY ~45 k rows/s (psycopg3; the
same CSV with `psql \copy` or psycopg2 ~100 k rows/s, so the gap is driver overhead, not the indexes; UNLOGGED would
be ~285 k rows/s, the price of the WAL); batch profile ~12 s. First full load ≈ 15 min COPY + 8 min profile + reads.

Same contract as RFB (ADR 0004): `audit.task_run` / `pipeline_run` / `dq_event` / `log_event` / `health_check` /
`quarantine_summary`, Pushgateway metrics `job=lambda_pipeline, pipeline=anp`, Loki `job=lambda-pipelines`, one trace
per run in Tempo (`service.name=lambda-anp`). Useful events: `catalog_parsed`, `discover_done`, `file_downloaded`
(MiB/s, sha256), `file_loaded` (rows, rows/s, rows by product, stations, date range), `bronze_table_stats`.

```sql
-- what is pending / failed
SELECT download_status, bronze_status, count(*) FROM ctrl.source_file WHERE pipeline = 'anp' GROUP BY 1, 2;
-- history of one file
SELECT created_at, event, version, bytes, rows, detail FROM ctrl.source_file_event
WHERE file_key = 'semestral/ca/2014-S1' ORDER BY id;
```

## 7. Runbook

* **Once:** `psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/anp_schema.sql` (idempotent).
* **Small first run, by hand:** `cd ~/lambda4 && export LAMBDA_RUN_ID=manual__$(date -u +%Y%m%dT%H%M%SZ)`, then
  `~/.envs/env-data/bin/python -m pipelines.anp.anp00_preflight`, `... anp01_discover`,
  `... anp02_download --kinds semanal --limit 3`, `... anp03_bronze --kinds semanal --limit 3`,
  `... -m pipelines.commons.finalize_run --pipeline anp`.
* **Full history:** unpause `anp_pipeline` and trigger with the defaults (~5.5 GB download, then ~40 M rows).
* **Republication check of everything:** trigger with `{"full_recheck": true}`.
* **Recompute the survey batch statistics** (after changing `PROFILE_SQL`):
  `~/.envs/env-data/bin/python -m pipelines.anp.anp03_bronze --reprofile` (data untouched).
* **Dashboards:** `infra/grafana/install.sh` (build + copy, needs sudo); `python3 infra/grafana/validate_dashboards.py`
  runs every query as `grafana_ro`.
* **Reload one file:** `UPDATE ctrl.source_file SET bronze_status = 'PENDING' WHERE pipeline = 'anp' AND file_key = '…'`
  and run bronze (its rows are replaced, never duplicated).
* **Streamlit:** `sudo systemctl start anp-dashboard` → http://localhost:8501 (tunnel `-L 8501:127.0.0.1:8501`);
  setup and security in `apps/anp_dashboard/README.md`.
* Services needed: Postgres (always on). Loki/Alloy/Tempo optional (degraded observability is a WARN). No Spark,
  no SeaweedFS.
