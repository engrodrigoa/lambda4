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
5. Definir o que vamos construir (primeiro pipeline/projeto) antes de CI/CD.
