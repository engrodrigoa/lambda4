# Projeto Lambda (lambda4)

> Contexto do projeto Lambda (atualizado em 2026-10-21, tracing fase 2). Referência técnica: `docs/architecture.md`.

## Papel do assistente
Instrutor + assistente de engenharia de dados/IA. Guiar com boas práticas de modelagem de pipelines,
qualidade de código, segurança e observabilidade. Explicar o que está sendo feito **antes** de executar
ações com root/sudo. Respostas em português (pt-BR).

## Escopo
- Este projeto é **somente Lambda** (laboratório local). Não misturar com o contexto "datalabs".
- Repositório: https://github.com/engrodrigoa/lambda4 (branch `master`) — dono: engrodrigoa.
- Leitura liberada em todo o GitHub `engrodrigoa` para referências.

## Máquina
| Item | Valor |
|---|---|
| Host | `lambdaserver` — Ubuntu 24.04.5, kernel 6.8 |
| HW | i7 (8 threads), 32 GB DDR3, SSD 238 GB (`/`), HDD USB 320 GB (datalake/scratch); HDD 1 TB NTFS interno (`sdb`) fora do projeto |
| Usuário | `lambda` (sudo sem senha). Senha: ver com o dono — **nunca gravar no repo** |
| Acesso | SSH via Tailscale a partir de `odyssey` (Windows, pessoal) |
| Tailscale | `100.69.229.85` / nome `lambdaserver` (estável; IP da LAN muda: atual 192.168.1.54 via DHCP) |
| Recursos | Pode usar o máximo da máquina nos pipelines |

Regra de acesso: sempre usar o IP/nome Tailscale, nunca o IP da LAN.

## Armazenamento (design)
- Um único HDD USB bus-powered (WD3200BPVT 320 GB, ponte JMicron JMS578, `usb-JMicron_Tech_DB12345681BC-0:0`), montado por UUID no `/etc/fstab` com `nofail`:
  - `/mnt/datalake` (ext4, ~194 GB) — dados do lake (SeaweedFS, warehouse Iceberg, modelos, vetores).
  - `/mnt/scratch` (ext4, ~98 GB) — temporários/shuffle do Spark.
- Estado (2026-10-18): OK após replug. SMART PASSED, 0 setores realocados/pendentes. Load_Cycle_Count alto (~140k, estacionamento de cabeça) — vigiar.
- Fragilidade conhecida: sem alimentação própria, o disco falha (`Hardware Error ASC=0x44`) após mover/desplugar. Se ocorrer: trocar porta/cabo (USB 3 traseira), replugar, depois `sudo mount -a`.
- Disco é pequeno (320 GB): planejar retenção/limpeza dos dados do lake. Backup do essencial vai para o GitHub (código) — dados do lake não têm backup.
- HDD interno SATA de 1 TB (`sdb1`, NTFS "HardDrive", UUID `EECC70A6CC706B2B`) montado em `/mnt/hdd1tb` (ntfs3, rw, `nofail` no fstab; fstab anterior em `/etc/fstab.bak-20261018`). Marca de "sujo" limpa com `ntfsfix -d` em 2026-10-18; SMART PASSED. Contém ~210 GB de arquivos antigos do dono (`airela`, `backup`, `fotos celular jordana`, `Games`, `Google Drive`, `lambda2`).
  - **REGRA: nunca apagar, mover ou limpar arquivos existentes nesse disco.** Só escrever em `/mnt/hdd1tb/lambda-dump/` (backups, exports, dados frios).
  - NTFS não guarda permissões/links Linux nem serve para bancos ou SeaweedFS: usar só como despejo/backup.

## Scratch (SSD) — changed 2026-10-18
- Hot scratch lives on the SSD: `/srv/scratch/{spark-tmp,spark-work,rfb-extract}` (owner `lambda:datalake`, setgid). Spark `spark.local.dir` and the worker `--work-dir` point there (worker override: `/etc/systemd/system/spark-worker.service.d/scratch-ssd.conf`; previous conf in `/etc/spark/spark-defaults.conf.bak-20261018`).
- `/mnt/scratch` (HDD USB partition) is now only cold/overflow temp space.
- Rule: temp CSVs from extract go to `/srv/scratch/rfb-extract` and are deleted after the bronze commit.

## Project 1 — RFB full via PySpark (decisions 2026-10-18)
- Goal: whole RFB CNPJ base, PySpark, medallion to gold, SCD2 history of CNPJ attribute changes, orchestrated by Airflow, learning-oriented but production-like.
- Layout: `pipelines/commons/*.py` (config, logger, spark session, checks, manifest — DRY across projects), `pipelines/rfb/*.py`, `airflow/dags/`, `infra/`, `docs/adr/`.
- Landing: local ZIPs, configurable via `LANDING_URI` (file:// or s3://), default `/mnt/hdd1tb/lambda-dump/landing/rfb/ref=YYYYMM/`. Reuse the datalabs download (37-file contract, retries, ZIP size validation; see datalabs ADR 0009).
- Pilot month: **2026-09**. First prove the whole pipeline with 1 month; code must already be structured for many months (`ref_month` param everywhere, ordered backfill).
- Bronze retention: **3 months**. Silver SCD2 keeps full history (stores only changes).
- Table format: **Delta Lake 4.1.0** (decided 2026-10-18, ADR 0001; goal: Databricks Data Engineer Associate). Named tables in an embedded Hive metastore on Postgres (`hive_metastore`). Iceberg stays available as catalog `lambda`.
- Keys: **natural keys** (estabelecimento `cnpj_basico+ordem+dv`; empresa/simples `cnpj_basico`; socios composite, validated on pilot data). No md5. SCD2 change detection uses Spark `xxhash64` over tracked columns, compared only between versions of the same key.
- Serving: DuckDB over gold for DBeaver.

## Model policy (set by the owner)
- Use **Opus 5.5** to write/generate code (precision); **Sonnet 5.5** for general discussion and planning (token economy). Prioritize Opus 5.5 when in doubt about code correctness.

## Control vs audit vs logs (owner's rule)
- **`ctrl`** = Delta tables in bucket `s3a://ctrl/`. ONLY reference/resume control for downloads: which `ref_month` was downloaded, when, status, and where to resume (e.g. 202608 downloaded -> proceed to 202609). History is kept.
- **`audit`** = Postgres schema `audit` in database `lambda` (read by Grafana via `grafana_ro`). Pipeline logs and audit: runs, tasks, dq events, full and parsed log lines, in the structure best for Grafana tables/panels. Do not mix with ctrl.
- Structured JSON logs (one event per line) go to Loki via Alloy; metrics via Pushgateway/Prometheus; alerts via Alertmanager.
- Failure policy: **BLOCK** only for model-breaking failures (contract file missing/unreadable, bronze 0 rows, month out of order for SCD2, natural-key duplicates in silver); everything else is **WARN** (continue, log to audit, metric, alert). Bad CSV rows go to quarantine, never silently dropped. Severity lives in one place in `pipelines/commons`.
- Every script exposes as much pipeline-health information as possible, task by task and for the whole orchestration (observability and reliability are the premise of every lab).

## Running PySpark here
- Python: `~/.envs/env-data/bin/python` (pyspark 4.1.0, delta-spark 4.1.0, duckdb). Run with
  `SPARK_HOME=/opt/spark SPARK_CONF_DIR=/etc/spark PYSPARK_PYTHON=$HOME/.envs/env-data/bin/python`.
- Config (cluster, Delta, Hive metastore, s3a, secrets): `/etc/spark/spark-defaults.conf` (mode 600). Template: `infra/spark/spark-defaults.conf.template`.
- Extra jars: `infra/spark/install_jars.sh`. Metastore schema: `infra/hive/README.md`. Smoke test: `infra/smoke/smoke_delta.py`.
- Buckets (SeaweedFS S3): `bronze`, `silver`, `gold`, `ctrl`, `warehouse` (Iceberg), `mlflow`. Secrets: `~/.config/lambda/secrets/` (never in git).
- Learning report for the owner: `docs/learning/`.

## Pipeline code (state 2026-10-21)
- Reference: `docs/architecture.md` (flow, DAG, lineage per step, data model, observability map, runbook). ADRs 0001-0004. Study reports `docs/learning/01..04` (pt-BR).
- `pipelines/commons/`: settings (`lake_uri`, env file), refmonth, context (Airflow ctx, `airflow_task_id`), logger (JSON lines with `trace_id`/`span_id`), audit (best-effort sink + spool), metrics (Pushgateway), tracing (OTel SDK spans, no-op when off), dq (THE severity registry), task (`TaskRun`/`run_task`, exit 0/99/1, SIGTERM -> recorded failure, one span per attempt), http, landing, spark, ctrl (Delta control + `InMemoryCtrl`), csv_split, quarantine, lake (DuckDB views over Delta via the metastore), delta_stats (`audit.delta_table_stats`), scd2 (generic SCD2 MERGE, rollback, as_of, table property `lambda.scd2.last_month`), finalize_run, airflow_callbacks.
- `pipelines/rfb/`: contract (37 files, columns, fixed CODES), source, rfb00_preflight, rfb01_download, rfb02_extract, rfb03_bronze, silver_model + rfb04_silver (SCD2), rfb05_gold (`--target dims|estabelecimento_atual|socio_atual|fato_mudanca_cadastral|agg_estabelecimentos_mensal`), rfb06_maintenance.
- DAG `rfb_pipeline` = `airflow/dags/dag_rfb.py` (symlinked into `~/airflow/dags/lambda4`): 24 tasks, param `ref_month`, pools `rfb_spark`(1) / `rfb_extract`(1), skip on exit 99, finalize `all_done` (fails if the run failed). Created PAUSED; **the owner runs it himself** (never trigger it without being asked).
- Real data: first full DAG run 2026-10-21 (`manual__2026-10-21T11:01:33.444773+00:00`, ref 2026-09): SUCCESS in 3 h 33 min,
  23/23 tasks, 0 BLOCK, 3 WARN (socios 292 exact dups + 31 conflicting keys, empresas 1 conflicting key -> quarantine).
  Slowest: estabelecimentos.silver 53 min (dedup window spills ~15 GB to disk: AQE coalesces to 32 partitions),
  maintenance 69 min (OPTIMIZE rewrites the fresh 96-file silver tables), gold.estabelecimento_atual 17 min.
  Datalake 47 GB used after the run (VACUUM keeps 168 h of old files).
- Tests: `~/.envs/env-data/bin/python -m pytest` (48 tests; `tests/spark/` = local Spark + Delta in a temp folder, never the lake; `-m "not spark"` to skip). Lint: `ruff check pipelines tests airflow infra`.
- Known issue: the Delta SQL extension breaks `spark.sql(..., args=...)` (UNBOUND_SQL_PARAMETER). Use the DataFrame API; literal SQL only with values from code.
- Known noise: `ERROR HiveAlterHandler: Failed to alter table` after Delta writes is harmless.
- **Spark is pinned to 4.1.0** (ADR 0002). Never upgrade Spark or Delta alone.

## Observability (state 2026-10-21, ADR 0004 phases 1 and 2)
- Postgres `audit`: pipeline_run, task_run (+airflow_task_id), dq_event, log_event (monthly partitions, 90 d), quarantine_summary, health_check, delta_table_stats, views v_last_runs / v_task_latest / v_dq_open / v_logs_recent / v_trace_spans. DDL: `infra/postgres/audit_schema.sql` (idempotent).
- Loki: `job=lambda-pipelines` (pipeline JSON events) and `job=airflow-tasks` (raw Airflow task logs, labels dag_id/task_id/attempt/level). Airflow logs live in `/var/log/lambda/airflow`.
- Prometheus: `job=lambda_pipeline` (Pushgateway batch metrics), `airflow_*` (Airflow OpenTelemetry -> Alloy OTLP :4318 -> remote write; Prometheus runs with `--web.enable-remote-write-receiver`). Rules: `infra/prometheus/rules/lambda_pipeline.yml` (reload: `sudo systemctl reload prometheus`).
- Grafana folder "Lambda4 · RFB": dashboards as code `infra/grafana/build_dashboards.py` -> `infra/grafana/install.sh`; `infra/grafana/validate_dashboards.py` runs every query. Do not edit dashboards in the UI (not persisted).
- Traces (phase 2, installed 2026-10-21): `pipelines/commons/tracing.py` (OTel SDK 1.44 in env-data) -> OTLP/HTTP
  Alloy `127.0.0.1:4318` -> Tempo 3.1 (`127.0.0.1:4417` gRPC in, `:3200` API; on demand; `/var/lib/tempo`, 14 d;
  config `infra/tempo/`, `apt-mark hold tempo`). `trace_id = md5(run_id)` (same as `audit.v_trace_spans`); span per
  TaskRun attempt (`<task_id>[<entity>]`), timed `log_event` (`seconds`/`latency_ms`) = child span, dq checks = span
  events, root span `<pipeline>.run` by `finalize_run` (or by a standalone manual task). Flags:
  `LAMBDA_OTEL_ENABLED` (default 1), `LAMBDA_OTEL_ENDPOINT`. No collector at start-up -> no-op with one warning.
- Airflow traces on (`AIRFLOW__TRACES__OTEL_ON` + `OTEL_TRACES_EXPORTER=otlp_proto_http` +
  `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` in `~/.config/airflow.env`, versioned in `infra/airflow/observability.env`):
  plus `OTEL_SERVICE_NAME=airflow` (without it spans arrive as `unknown_service`; set 2026-10-21, applies after an
  Airflow restart), its own trace per DAG run (`dag_run.rfb_pipeline` -> `task_run.<t>` -> `worker.<t>`); the DAG's
  `TracedBashOperator` passes `TRACEPARENT`, our spans link to `worker.<task>`. Verified on the first real run: 146
  spans in one tree, every task attempt linked to Airflow's trace.
- Grafana datasources for Loki/Alertmanager/Tempo are versioned in `infra/grafana/datasources-observability.yaml`
  (-> `/etc/grafana/provisioning/datasources/obs-plus.yaml`; Loki "Trace in Tempo" derived field, Tempo -> logs/metrics).
  Alloy stores `trace_id`/`span_id` as Loki structured metadata. Tempo port 4317 is NOT used (Phoenix owns it).
- Spark Thrift Server (JDBC over Delta, DBeaver): `sudo systemctl start|stop spark-thrift` (on demand, 2 cores / 2 GB), `jdbc:hive2://localhost:10000`.
- `lab` is versioned in `infra/bin/lab` (install: `install -m 0755 infra/bin/lab ~/.local/bin/lab`).

## Owner access (from odyssey, via Tailscale + SSH)
- Postgres for DBeaver: role `dbeaver_ro` (read-only: `audit`, `analytics`, `analytics_elementary` in db `lambda`); the owner sets its password with `sudo -u postgres psql -c "\password dbeaver_ro"`. Never paste passwords in chat or git. `lambda` is superuser (peer auth on the server).
- DBeaver: use its SSH tab (host `lambdaserver`, user `lambda`) -> Postgres `127.0.0.1:5432`, db `lambda`.
- One tunnel for the UIs: `ssh -N -L 8080:127.0.0.1:8080 -L 3000:127.0.0.1:3000 -L 8088:127.0.0.1:8088 -L 4040:127.0.0.1:4040 -L 18080:127.0.0.1:18080 -L 5433:127.0.0.1:5432 -L 10000:127.0.0.1:10000 lambda@lambdaserver`.
- Reading Delta: `pipelines/commons/lake.py` -> `duckdb_lake()` gives DuckDB views with the metastore names (`rfb_bronze.*`, `ctrl.*`, `audit.quarantine`) over `delta_scan`; Spark (`build_spark(..., master="local[4]")`) for history/time travel/CDF. Notebook: `notebooks/rfb_bronze_explore.ipynb`, kernel **Python (lambda-data)**. Commit notebooks WITHOUT outputs (they contain cadastral data).
- Bronze lives in SeaweedFS S3 `s3://bronze/rfb/<entity>/` (Delta: `_delta_log/` + `_ref_month=YYYYMM/*.zstd.parquet`), physically under `/mnt/datalake/seaweedfs`.

## Stack (systemd nativo, sem Docker) — comando de saúde: `lab`
| Grupo | Serviço | Porta |
|---|---|---|
| dados | PostgreSQL 16 (+pgvector 0.6) — bancos: airflow, iceberg_catalog, lambda, mlflow, phoenix | 5432 |
| dados | SeaweedFS (master/volume/filer/S3) | 9333 / 9340 / 8888 / 8333 |
| orquestração | Airflow 3 (api-server, scheduler, dag-processor, triggerer) | 8080 |
| orquestração | dbt + Elementary (report) | 8090 |
| spark | master / worker / history / thrift (Spark **4.1.0** em `/opt/spark`, fixado pelo Delta) | 8088 / 8089 / 18080 / 10000 |
| observabilidade | Prometheus, Alertmanager, Grafana, Loki, Alloy (+OTLP 4318), Tempo (OTLP gRPC 4417), Pushgateway, node/postgres exporters | 9090 / 9093 / 3000 / 3100 / 12345 / 3200 / 9091 / 9100 / 9187 |
| ia | MLflow, Phoenix, Qdrant, Ollama | 5000 / 6006 / 6333 / 11434 |

Todos os serviços escutam em localhost; acesso remoto por **túnel SSH** (`lab urls`).

## Layout no servidor
- `~/.envs/env-{ai,airflow,data,dbt,mlflow,phoenix}` — venvs isolados por domínio (uv).
- `~/airflow/{dags,plugins,logs}`, `~/dbt/lambda_dbt`, `~/pipelines/`.
- `~/NN-*.sh` — scripts idempotentes de provisionamento (01-base … 10-ai-stack). `~/NN.log` = logs.
- `~/.local/bin/lab` — painel de saúde. Alias `s3l` = aws CLI contra SeaweedFS S3.

## Convenções (decididas em 2026-10-18)
- Código do projeto em `~/lambda4` (clone do repo); scripts de infra versionados em `infra/`.
- Git: branch principal **`master`**, push direto permitido. Criar branch só para testes específicos. CI/CD fica para depois de definirmos o que construir.
- **Commits, código, comentários e docs técnicos em inglês.** Conversa com o dono em pt-BR.
- Autenticação GitHub: `gh` (HTTPS, keyring) como `engrodrigoa`; identidade git global já configurada.
- Segredos fora do git (`.env`/arquivos em `~/.config/lambda/`, `.gitignore` obrigatório).
- Medalhão: bronze → silver (SCD2) → gold, em **Delta** no SeaweedFS S3, transformações em **PySpark**, orquestração Airflow.
- Todo pipeline: idempotente, com checks BLOCK/WARN (`pipelines/commons/dq.py`), audit no Postgres, métricas no Prometheus, logs no Loki, alertas no Prometheus/Alertmanager, dashboards como código.

## Ciclo de vida dos serviços (aplicado em 2026-10-18)
- **Sobem no boot (enabled):** postgresql, airflow (api-server, scheduler, dag-processor, triggerer), elementary-report, grafana, prometheus, alertmanager, pushgateway, node/postgres exporters, tailscaled.
- **Sob demanda (disabled no boot):** seaweedfs, spark-master/worker/history, spark-thrift, mlflow (depende de seaweedfs), qdrant, ollama, phoenix, loki, alloy, tempo. Ligar/desligar com `sudo systemctl start|stop <unit>` até existir `lab start|stop`.
- Alertas de "alvo fora do ar" para serviços sob demanda são esperados; refinar regras do Alertmanager depois.

## Pendências conhecidas
1. Considerar disco maior/com fonte própria para o datalake (320 GB, bus-powered).
2. Implementar `lab start|stop`; silenciar alertas "alvo fora do ar" dos serviços sob demanda.
3. Ajustes da 1ª execução real: spill do dedup do silver (partições AQE), barra de progresso do Spark nos logs (`spark.ui.showConsoleProgress=false`), silver gravando 96 arquivos (OPTIMIZE caro), variáveis de métricas OTel do Airflow obsoletas (migrar para `OTEL_EXPORTER_OTLP_METRICS_*`) + reiniciar o Airflow para aplicar `OTEL_SERVICE_NAME`.
4. CI (rodar `pytest` e `ruff` a cada push) depois do primeiro pipeline completo validado.
5. Backfill de meses anteriores: silver só aceita meses em ordem a partir do primeiro carregado (reconstruir para começar mais cedo).
