# Handoff — dbt_lambda / ANP (silver → observabilidade → gold)

> Original: sessão claude.ai de 07/11/2026, trazido para o repo em 08/11/2026.
> **Leia primeiro a seção 0**: ela corrige o que o original dizia errado ou que já mudou.

## 0. Correções e estado em 08/11/2026 (Claude Code, PR A `feat/dbt-anp-silver-close`)

- **Acesso no DBeaver:** a conexão "lambda" do DBeaver usa o papel **`dbeaver_ro`**, não `lambda`. O erro
  `permission denied for schema anp_silver` acontecia porque o dbt cria seus schemas sem grants. A correção está em
  `infra/postgres/anp_dbt_grants.sql` (idempotente): cria `anp_silver`, `anp_gold`, `anp_dq` e `anp_ref` com USAGE
  + SELECT + DEFAULT PRIVILEGES para `dbeaver_ro` e `grafana_ro`; o `dbt_project.yml` também declara `+grants`.
  Para ter admin no DBeaver, use uma segunda conexão com o usuário `lambda`, que é superuser
  (`sudo -u postgres psql -c "\password lambda"` define a senha; os pipelines continuam usando socket com peer).
- **OTel / Loki / Tempo existem:** o Alloy é o coletor OTLP na porta 4318; Loki e Tempo também estão instalados,
  todos como serviços **sob demanda** (`systemctl start alloy loki tempo`). O `Connection refused` só aparece
  porque estavam parados.
- **`dbt_project.yml`:** a `var` `anp_mensal_inicio` e o bloco `observability` (→ `anp_dq`) não existiam;
  foram aplicados no PR A, junto com as vars do portão de preço e da recência.
- **`assert_coleta_uma_fonte_por_mes`** não estava no repo; foi restaurado.
- **Linhas vazias:** o `anp03` agora as envia para a quarentena (`reason = 'empty_record'`, WARN
  `bronze.empty_records`). Os 3 arquivos foram recarregados (11.827 registros em quarentena) e o filtro na
  stg ficou só como defesa.
- **Asset** declarado uma vez, em `pipelines/commons/assets.py` (`ANP_BRONZE`).
- **Sobras removidas:** a view `anp_silver.stg_anp_precos` e a tabela `dbt_dev.anp_produto_grupo`.

---

## 1. Como trabalhar com o Rodrigo (regras do dono)

- **Passo a passo, explicando o porquê.** Ele quer aprender o workflow, não só receber o resultado.
- **Entrevistar antes de decidir.** Fazer as perguntas certas e deixar a decisão de negócio com ele.
- **Estruturar sempre em 3 faixas (swimlanes):** fluxo de **dados**, fluxo de **observabilidade** (saúde da carga) e fluxo de **qualidade**. Ele assimila melhor com um mapa visual por etapa.
- **Comunicar antes de executar** qualquer ação no servidor, no banco ou no git.
- **Nunca disparar DAGs sem pedido explícito** (regra também presente no `CLAUDE.md`).
- **Git:** branch + PR (ex.: `feat/dbt-anp-silver`). Nunca colocar link de sessão do Claude em commit ou PR.
- Português brasileiro, com termos técnicos-chave em inglês. Direto, sem bajulação. Apontar quando ele estiver desviando.

---

## 2. Ambiente (servidor `lambdaserver`, via Tailscale `ssh lambda`)

| Item | Valor |
|---|---|
| Repo | `~/lambda4` (GitHub `engrodrigoa/lambda4`), branch base `master` |
| Projeto dbt | `~/lambda4/dbt_lambda` |
| venv dbt | `~/lambda4/dbt_lambda/.venv` (dbt-core 1.12.6, dbt-postgres 1.11.0, dbt_utils 1.4.1) |
| venv Airflow | `~/.envs/env-airflow` (Airflow **3.3.2**, astronomer-cosmos **1.15.1**). Criada com **uv**, sem pip: `uv pip install --python ~/.envs/env-airflow/bin/python ...` |
| venv pipelines | `~/.envs/env-data` (Python/Polars dos scripts `pipelines.*`) |
| Airflow | 4 serviços systemd (api-server, dag-processor, scheduler, triggerer), `User=lambda`, `EnvironmentFile=~/.config/airflow.env` |
| CLI Airflow | função no `~/.bashrc`: `airflow() { ( set -a; source ~/.config/airflow.env; set +a; ~/.envs/env-airflow/bin/airflow "$@" ) }` |
| Pasta de DAGs | `~/airflow/dags/lambda4` → **symlink** para `~/lambda4/airflow/dags` |
| Postgres | banco `lambda`, **socket local + peer auth** (`host=/var/run/postgresql`, user `lambda`), **sem senha em lugar nenhum** |
| dbt `profiles.yml` | no próprio projeto, `host: /var/run/postgresql`, `password: ""`, target `dev`, schema default `dbt_dev` |
| `.env` do dbt | só `DBT_PROFILES_DIR=/home/lambda/lambda4/dbt_lambda` |

**Atenção:**
- Não instalar pacotes com `-U` na venv do Airflow. Um dry run mostrou que isso puxa o Airflow 3.4 e majors de sqlalchemy, protobuf e dateutil. O upgrade do Airflow foi **adiado** de propósito.
- O OTel Collector **não existe** (nada escuta em 4317/4318), mas o `airflow.env` aponta para `127.0.0.1:4318`. Os avisos `Connection refused` são ruído conhecido. Grafana, Prometheus, Pushgateway e os exporters existem; Tempo e Loki não.
- Existe um DAG antigo de laboratório, `~/airflow/dags/lambda_dbt.py` (projeto `~/dbt/lambda_dbt`, venv `env-dbt`, Elementary + `edr report`, `schedule=None`). **Manter** como referência de integração com o Elementary e aposentar depois.

---

## 3. Pipeline ANP (contexto upstream, já existente)

DAG `anp_pipeline` (`airflow/dags/dag_anp.py`, diário 08:00): `preflight → discover → download → bronze → finalize`.

- **Bronze** (`pipelines/anp/anp03_bronze.py`): `anp_bronze.precos_raw`, **particionada por LIST (`_source_kind`)**, com todas as colunas `text`, mais a linhagem (`_source_kind, _file_key, _file_sha256, _record_no, _run_id, _ingested_at`). É idempotente por arquivo: numa única transação faz DELETE do `_file_key`, depois COPY, e grava o `batch_profile`.
- `anp_bronze.batch_profile`: profile por arquivo × `batch_month` × produto. `produto='*'` é o total; `batch_month=0` são datas não parseáveis.
- `anp_bronze.precos_quarantine`: registros com número de campos errado (não entram no raw).
- Controle: `ctrl.source_file` / `source_file_event`. Auditoria: `audit.*`.
- A task `bronze` emite o asset `Asset(name="anp_bronze.precos_raw", uri="lambda://anp/bronze/precos_raw")`.
- Fontes: **semestral** (histórico até 2026-S1, não é mais baixado), **mensal** (consolida o mês fechado) e **semanal** (janela móvel de 4 semanas, avança 1 por semana).
- O `_file_key` tem o formato `<kind>/<grupo_arquivo>/<período>`. Grupos de arquivo: semestral `ca` | `glp`; mensal e semanal `gasolina-etanol` | `diesel-gnv` | `glp`.

---

## 4. Estado atual do `dbt_lambda`

### Estrutura

```
dbt_lambda/
├── dbt_project.yml        silver→anp_silver | gold→anp_gold | observability→anp_dq (table) | seeds→anp_ref
├── profiles.yml           peer auth via socket
├── packages.yml / package-lock.yml   dbt_utils 1.4.1
├── macros/generate_schema_name.sql   usa o custom schema literal (sem prefixo do target)
├── seeds/anp_produto_grupo.csv       8 produtos → product_group
├── models/bronze/_sources.yml        3 sources (raw, batch_profile, quarantine) + freshness + testes
├── models/silver/stg_anp__precos.sql (+ .yml)          view
├── models/silver/anp__precos_coleta.sql (+ .yml)       incremental
├── models/observability/dq_anp__carga_mes.sql (+ .yml) ← código escrito, NÃO executado ainda
└── tests/
    ├── assert_bronze_linhas_vazias.sql            (warn)
    ├── assert_coleta_uma_fonte_por_mes.sql
    └── assert_stg_reconcilia_batch_profile.sql    ← escrito, NÃO executado ainda
```

`dbt_project.yml` deve conter `vars: anp_mensal_inicio: '2023-01-01'` e o bloco `observability` (confirmar se foi aplicado).

### Modelos

**`stg_anp__precos`** (view, `anp_silver`). CTEs `src → nao_vazias → tipado → join seed`.
- Parsing **idêntico ao `PROFILE_SQL` do anp03**: CNPJ e CEP só dígitos, data `DD/MM/YYYY`, decimal com vírgula convertido para `numeric(12,4)`, `upper(trim)` nos textos.
- Filtra linhas 100% vazias com `num_nonnulls(...16 colunas...) > 0`.
- `row_hash = dbt_utils.generate_surrogate_key` sobre as **colunas brutas** (identidade da coleta publicada).
- `product_group` vem do **seed pelo produto**. `file_group` (do `_file_key`) fica só como linhagem.
- `source_priority`: mensal 1, semanal 2, semestral 3.
- 7 testes, todos PASS.

**`anp__precos_coleta`** (incremental, `anp_silver`): a silver.
- `delete+insert` com `unique_key=['mes_coleta','product_group']`. O `post_hook` cria o índice `(mes_coleta, product_group)` e roda `analyze`.
- Incremental: identifica os (mês × grupo) tocados por `_ingested_at > max(_ingested_at)` e **reconstrói esses meses inteiros**.
- `fonte_mes` = `min(source_priority)` por (mês, grupo), e só a fonte vencedora entra.
- Dedup por `row_hash` (`row_number`, mantendo o `_ingested_at` mais recente).
- Colunas extras: `qt_publicacoes` (observabilidade) e `fl_conflito_preco` (mesmo cnpj/produto/dia com mais de um preço).
- **Primeira carga completa: 13,5 min, 31.621.042 linhas.**
- Testes escritos: `unique`/`not_null` em `row_hash`, `accepted_values`, `accepted_range` de `qt_publicacoes` entre 1 e 4 para o semanal (warn), e o singular de uma fonte por mês. **Confirmar se foram executados e passaram.**

### Reconciliação validada manualmente (números de referência)

```
stg                      35.324.578
descartadas por fonte     3.703.394   (semestral 2023-01..2026-06 + todo o semanal atual)
fonte escolhida          31.621.184   = sum(qt_publicacoes) na silver ✅
duplicatas exatas               142
silver                   31.621.042   ✅
linhas em conflito de preço      44
linhas vazias no bronze      11.827   (= batch_profile com batch_month=0 ✅)
```

### DAG `anp_dbt` (`airflow/dags/dag_anp_dbt.py`)

`DbtDag` do Cosmos com `schedule=[BRONZE]` (disparado pelo asset).
- `ProjectConfig(DBT_DIR, install_dbt_deps=False)`: mudou o `packages.yml`, é preciso rodar `dbt deps` à mão.
- `ProfileConfig(profiles_yml_filepath=DBT_DIR/"profiles.yml")`, **sem Airflow Connection**.
- `ExecutionConfig(dbt_executable_path=.venv/bin/dbt, invocation_mode=SUBPROCESS)`.
- `RenderConfig(test_behavior=AFTER_EACH, source_rendering_behavior=WITH_TESTS_OR_FRESHNESS, emit_datasets=False)`.
- O `emit_datasets=False` é **obrigatório**: com host de socket, o Cosmos gera a URI `postgres:///var/run/...` sem host, e o Airflow 3 rejeita.
- Testado com `airflow dags test anp_dbt`: sucesso. O DAG está **pausado**.

### Git

- PR 1 (`feat/dbt-anp-staging`): **merged** no master.
- Branch atual: `feat/dbt-anp-silver`, com `_sources.yml` completo, `anp__precos_coleta`, `dq_anp__carga_mes`, testes novos e ajustes no DAG. **Ainda sem commit/PR.**

---

## 5. Decisões tomadas (não reabrir sem motivo)

1. **Bronze é imutável e de responsabilidade do Python.** O dbt só lê do bronze, nunca escreve. O schema padrão do target é `dbt_dev`, nunca o bronze.
2. **A precedência entre fontes é resolvida na silver, por mês × `product_group`**, não linha a linha: mensal > semanal > semestral. Quando o mensal chega, ele substitui o mês inteiro, e as coletas que só existiam no semanal saem da silver.
3. **O `product_group` deriva do produto (seed)**, porque o semestral `ca` não casa com os grupos do mensal. Um produto novo da ANP precisa entrar no seed, e o teste `not_null` acusa se não entrar.
4. **Dedup apenas de duplicatas exatas** (`row_hash` sobre as colunas brutas). Preços diferentes no mesmo posto/dia são **mantidos e sinalizados** (`fl_conflito_preco`), apenas com **contagem, sem limite**. A gold decide como agregar.
5. **A silver fica unificada** (todas as colunas do posto na linha), e o modelo dimensional fica só na gold.
6. Na source testa-se o **contrato técnico** (linhagem e grão `_file_key + _record_no`). As regras de negócio (`not_null` de cnpj, data, valor) ficam na staging.
7. Sem freshness na quarentena, porque ausência de linhas novas é o estado saudável.
8. Dois DAGs desacoplados, ligados pelo asset. O `anp_pipeline` não muda.
9. Peer auth e nenhuma senha: o padrão do servidor (`pipelines.commons.pg`) se estende ao dbt.
10. O upgrade do Airflow para 3.4 fica **adiado**. Quando acontecer, deve ser uma tarefa própria, com venv paralela, constraints oficiais, backup do metadata DB e rollback.

---

## 6. Achados de dados (usar no PR e no portfólio)

- **11.827 linhas 100% vazias** (rodapé de CSV) em `semestral/ca/2025-S1`, `mensal/diesel-gnv/2023-01` e `mensal/glp/2023-01`. Elas passam pelo contrato do anp03.
- O **semestral vai até 2026-06-30** e sobrepõe o mensal de 2023-01 a 2026-06. Nessa janela, os dois publicam **contagens idênticas** mês a mês, exceto em **2025-11** (semestral +8 em diesel-gnv e +31 em gasolina-etanol).
- O **mensal de 2026-04 não existe** para nenhum grupo. Não está no `ctrl.source_file`, então a ANP não publicou. A regra por mês cobre o buraco com o semestral automaticamente.
- O **semanal hoje não contribui com nenhuma linha**: ele vai de 2026-09-21 a 2026-10-30, e o mensal já cobre até 30/10. O caminho do semanal só será exercitado com dados de novembro.
- Os arquivos semanais de 2026-10-27 faltam nos 3 grupos (execução que não rodou). A janela de 4 semanas recuperou os dados pelo arquivo de 11-03.
- `DIESEL S50` existe só no semestral e foi pego pelo teste `not_null` do `product_group`.
- O caso **AUTO POSTO MANGUEIRA II** (CNPJ 00.008.226/0001-39, mesmo endereço) tem 2 preços por produto no mesmo dia, de forma recorrente. É o exemplo de `fl_conflito_preco`.

---

## 7. Próximos passos (em ordem)

### 7.1 Fechar a silver (branch `feat/dbt-anp-silver`)

1. Confirmar `dbt test -s anp__precos_coleta assert_coleta_uma_fonte_por_mes`.
2. Aplicar no `dbt_project.yml` o `vars.anp_mensal_inicio` e o bloco `observability` (`+schema: anp_dq`, `+materialized: table`).
3. `dbt run -s dq_anp__carga_mes` e `dbt test -s dq_anp__carga_mes assert_stg_reconcilia_batch_profile`. Esperado:
   - `unique_combination` e `dq_anp_silver_reconcilia_fonte_escolhida`: PASS.
   - `dq_anp_mes_fechado_sem_mensal`: **WARN 3** (abril/2026 × 3 grupos).
   - `assert_stg_reconcilia_batch_profile`: PASS.
4. Inspecionar `anp_dq.dq_anp__carga_mes` a partir de 2025-10 e conferir o funil (semestral descartado até 2026-06, abril/26 = semestral, semanal descartado em set/out).
5. `airflow dags reserialize`, conferir o Graph (sources → seed → stg → coleta → dq) e fazer um `airflow dags test anp_dbt` **só com autorização do dono**.
6. Escrever as descriptions dos modelos e colunas, rodar `dbt docs generate`, e depois commit + PR com os números da reconciliação e os achados da seção 6.

### 7.2 Observabilidade (complementos)

- **Continuidade semanal (warn):** no mês aberto (só semanal), toda semana até a data mais recente precisa ter coleta. Também alertar se o último arquivo semanal tiver mais de 21 dias (a janela está prestes a perder dado).
- **Freshness da própria silver.** Hoje só o bronze tem freshness. Se o `anp_dbt` parar, a silver envelhece sem alarme.
- Painel Grafana sobre `anp_dq.dq_anp__carga_mes` (funil por mês e fonte escolhida).
- Trazer o **Elementary** para o `dbt_lambda`, usando o `lambda_dbt` como modelo, e depois aposentar o `lambda_dbt`.

### 7.3 Gold (ainda não desenhada: **entrevistar o Rodrigo antes**)

Ponto de partida a discutir, sem nada decidido:

- `dim_posto`: candidata a **SCD2** (mudança de bandeira e endereço ao longo do tempo). Ele já tem interesse em SCD2 (projeto RFB). Pergunta: qual é a chave natural? CNPJ sozinho não basta, já que existem filiais e mudanças de endereço.
- `dim_produto` (do seed, com unidade de medida) e `dim_calendario`.
- `fct_preco_diario` (posto × produto × dia): a regra de agregação dos conflitos ainda está **aberta**. A sugestão inicial era preço médio + min/max + `qt_precos`.
- Consumidores conhecidos para orientar o grão: um painel Streamlit público de preços de combustíveis (projeto pessoal) e Power BI.
- Perguntas a fazer: quem consome, qual grão, quais métricas (média por município/UF, variação semanal, ranking de bandeira), e se a gold será incremental.

---

## 8. Dívidas técnicas e backlog

| Item | Detalhe |
|---|---|
| Incremental varre a staging inteira | o filtro por `mes_coleta` não chega ao bronze (coluna calculada na view). Opção: materializar a staging como table |
| Incremental reprocessa meses perdedores | se um arquivo novo só traz linhas que perdem a precedência, o `max(_ingested_at)` da silver não avança e o mês é reprocessado. Resultado correto, só custo |
| Asset declarado 2× | mover o `BRONZE` para `pipelines/commons/assets.py` e importar nos 2 DAGs (renomear de um lado só quebra o trigger em silêncio) |
| `anp03` | mandar linhas 100% vazias para a quarentena com `reason='empty_record'` (hoje elas inflam o `batch_profile.rows`) |
| OTel Collector | instalar ou ajustar o `airflow.env`; Tempo e Loki citados no docstring do `anp_pipeline` não existem |
| `execution_timeout` no `anp_dbt` | medir uma execução incremental real e definir com folga (a carga completa leva 13,5 min) |
| Upgrade do Airflow 3.4 | tarefa própria com rollback (ver decisão 10) |
| Usuário `dbt` no Postgres | menor privilégio (SELECT no bronze, CREATE em silver/gold/dq), para o banco garantir a regra "dbt não escreve no bronze" |

---

## 9. Comandos de referência

```bash
cd ~/lambda4/dbt_lambda && source .venv/bin/activate
dbt parse
dbt source freshness
dbt test -s source:anp_bronze
dbt run  -s stg_anp__precos anp__precos_coleta dq_anp__carga_mes
dbt test -s stg_anp__precos anp__precos_coleta dq_anp__carga_mes
dbt build --full-refresh -s anp__precos_coleta      # recarga completa (~13,5 min)

airflow dags reserialize 2>/dev/null | grep -iE "error|anp_dbt"
airflow dags test anp_dbt 2>&1 | grep -E "DAG TEST|Done\.|PASS=|WARN=|ERROR|DagRun"   # só com autorização

psql -d lambda -P pager=off -x -c "select ..."       # sem pager, modo expandido
```

```sql
-- tamanho do bronze (tabela particionada: somar as partições)
select pg_size_pretty(sum(pg_total_relation_size(inhrelid)))
from pg_inherits where inhparent = 'anp_bronze.precos_raw'::regclass;
```
