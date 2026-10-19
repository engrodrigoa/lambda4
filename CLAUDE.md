# Projeto Lambda (lambda4)

> RASCUNHO v0 — gerado em 2026-10-18 após inspeção da máquina. Itens marcados com `[?]` aguardam confirmação do dono.

## Papel do assistente
Instrutor + assistente de engenharia de dados/IA. Guiar com boas práticas de modelagem de pipelines,
qualidade de código, segurança e observabilidade. Explicar o que está sendo feito **antes** de executar
ações com root/sudo. Respostas em português (pt-BR).

## Escopo
- Este projeto é **somente Lambda** (laboratório local). Não misturar com o contexto "datalabs".
- Repositório: https://github.com/engrodrigoa/lambda4 (remoto, aparentemente vazio) — dono: engrodrigoa.
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
- Python: `~/.envs/env-data/bin/python` (pyspark 4.1.3, delta-spark 4.1.0, duckdb). Run with
  `SPARK_HOME=/opt/spark SPARK_CONF_DIR=/etc/spark PYSPARK_PYTHON=$HOME/.envs/env-data/bin/python`.
- Config (cluster, Delta, Hive metastore, s3a, secrets): `/etc/spark/spark-defaults.conf` (mode 600). Template: `infra/spark/spark-defaults.conf.template`.
- Extra jars: `infra/spark/install_jars.sh`. Metastore schema: `infra/hive/README.md`. Smoke test: `infra/smoke/smoke_delta.py`.
- Buckets (SeaweedFS S3): `bronze`, `silver`, `gold`, `ctrl`, `warehouse` (Iceberg), `mlflow`. Secrets: `~/.config/lambda/secrets/` (never in git).
- Learning report for the owner: `docs/learning/`.

## Stack (systemd nativo, sem Docker) — comando de saúde: `lab`
| Grupo | Serviço | Porta |
|---|---|---|
| dados | PostgreSQL 16 (+pgvector 0.6) — bancos: airflow, iceberg_catalog, lambda, mlflow, phoenix | 5432 |
| dados | SeaweedFS (master/volume/filer/S3) | 9333 / 9340 / 8888 / 8333 |
| orquestração | Airflow 3 (api-server, scheduler, dag-processor, triggerer) | 8080 |
| orquestração | dbt + Elementary (report) | 8090 |
| spark | master / worker / history (Spark 4.1.3 em `/opt/spark`) | 8088 / 8089 / 18080 |
| observabilidade | Prometheus, Alertmanager, Grafana, Loki, Alloy, node/postgres exporters | 9090 / 9093 / 3000 / 3100 / 12345 / 9100 / 9187 |
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
- Medalhão: bronze → silver → gold (Iceberg no SeaweedFS S3), transformações dbt, orquestração Airflow.
- Todo pipeline: idempotente, com testes dbt/Elementary, métricas no Prometheus, logs no Loki, alerta no Alertmanager.

## Ciclo de vida dos serviços (aplicado em 2026-10-18)
- **Sobem no boot (enabled):** postgresql, airflow (api-server, scheduler, dag-processor, triggerer), elementary-report, grafana, prometheus, alertmanager, node/postgres exporters, tailscaled.
- **Sob demanda (disabled no boot):** seaweedfs, spark-master/worker/history, mlflow (depende de seaweedfs), qdrant, ollama, phoenix, loki, alloy. Ligar/desligar com `sudo systemctl start|stop <unit>` até existir `lab start|stop`.
- Alertas de "alvo fora do ar" para serviços sob demanda são esperados; refinar regras do Alertmanager depois.

## Pendências conhecidas
1. Considerar disco maior/com fonte própria para o datalake (320 GB, bus-powered).
2. Serviços sob demanda ainda desligados; subir e validar com `lab` agora que o disco voltou.
3. Implementar `lab start|stop`; silenciar alertas dos serviços sob demanda.
4. VS Code desktop não instalado; acesso de edição remota é via `~/.vscode-server` (Remote-SSH a partir do odyssey).
5. Projeto 1 definido (RFB via PySpark). CI/CD só depois do primeiro pipeline funcionar.
6. Próximo: desenhar tabelas ctrl (Delta) e audit (Postgres), escrever `pipelines/commons` e `rfb01` (usar Opus 5.5 para código).
