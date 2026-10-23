# Relatório de aprendizado 04 — Pipeline completo até a gold: Spark, modelagem, confiabilidade e observabilidade

Material de estudo e de defesa do projeto. Complementa os relatórios 01 a 03. A referência técnica (tabelas, linhagem
etapa a etapa, runbook) está em `docs/architecture.md`; as decisões estão nos ADRs 0001 a 0004.

> **Estado nesta etapa.** Tudo foi construído, mas **nada foi executado** de propósito: a execução fica com você, para
> observar o fluxo. O que já roda com dados reais: landing, extract e bronze de 2026-09 (relatório 03).
> O que foi verificado sem rodar o pipeline:
> - 38 testes automatizados, incluindo Spark e Delta locais com dados sintéticos (SCD2, tipagem da silver,
>   deduplicação, eventos de mudança da gold);
> - a DAG parseada pelo Airflow (24 tasks, pausada, nenhuma execução);
> - as 75 consultas dos dashboards rodadas contra as fontes reais;
> - as sondas do preflight chamadas uma a uma;
> - o Thrift Server testado por JDBC.
>
> **Silver e gold ainda não rodaram com os 73 milhões de linhas.** A primeira execução real é também o primeiro teste
> de desempenho delas.

---

## 1. O mapa em uma frase por etapa

| Etapa | O que faz | Script | Spark? |
|---|---|---|---|
| preflight | verifica se a plataforma está saudável (S3, Spark, metastore, discos, observabilidade) | `rfb00_preflight` | não |
| landing | baixa os 37 ZIPs do mês, com retomada e checksum | `rfb01_download` | só para gravar o `ctrl` (local) |
| extract | descompacta em pedaços UTF-8 alinhados por registro e conta tudo | `rfb02_extract` | não (multiprocess) |
| bronze | grava o mês como veio, em Delta, e reconcilia com o extract | `rfb03_bronze` | sim (cluster) |
| silver | tipa, limpa, deduplica e aplica o mês no histórico SCD2 | `rfb04_silver` | sim |
| gold | constrói as tabelas de consumo "como estava no mês" | `rfb05_gold` | sim |
| maintenance | compacta arquivos e limpa versões antigas | `rfb06_maintenance` | sim |
| finalize | fecha a execução no audit e publica métricas da execução | `commons/finalize_run` | não |

Todas as etapas usam a mesma "moldura" (`TaskRun`), que dá a cada uma:
- `run_id` em todos os registros;
- logs JSON;
- registro em `audit.task_run`;
- métricas no Pushgateway;
- checks BLOCK/WARN;
- código de saída 0/99/1.

---

## 2. Como o Spark trabalha neste fluxo

### 2.1 Anatomia mínima
- **Cluster standalone:** um *master* (8088), que distribui recursos, e um *worker* com 6 cores e 12 GB. Cada script
  cria uma **aplicação** com um **driver** (o processo Python, mais a JVM do driver) e **executors** no worker.
- **Lazy evaluation:**
  - *Transformações* (`select`, `where`, `join`) só montam um plano.
  - *Ações* (`count`, `write`, `collect`) disparam **jobs**.
  - Cada job vira **stages**, separados por *shuffle*.
  - Cada stage vira **tasks**, uma por partição.
- **Catalyst** otimiza o plano (predicate pushdown, column pruning).
- **AQE** (*Adaptive Query Execution*) reotimiza **durante** a execução: junta partições pequenas depois do shuffle,
  troca join por broadcast se uma tabela ficou pequena, e divide partições com *skew*.
- **Shuffle** é redistribuir dados entre tasks por uma chave (join, groupBy, distinct, window). É a operação cara: grava
  em disco local (o scratch no SSD) e trafega pela rede. **Reduzir shuffle é a principal alavanca de desempenho.**

### 2.2 Bronze: leitura em paralelo de um CSV que exige `multiLine`
- Um arquivo lido com `multiLine=true` vira **uma** task. Por isso o extract corta os CSVs em pedaços de cerca de 512 MB,
  e o Spark lê dezenas de pedaços em paralelo (41 em estabelecimentos).
- A gravação é uma *narrow transformation*: cada pedaço vira um arquivo Parquet, sem shuffle.
- `mode("overwrite").option("replaceWhere", "_ref_month = 202609")` gera **um commit atômico** no `_delta_log`. Os
  arquivos antigos da partição saem do log (logicamente) e os novos entram.
- As métricas do commit (`numOutputRows`) dispensam um `count()` extra.

### 2.3 Silver: onde o Spark realmente trabalha
1. **Tipagem e limpeza** (`select` com expressões): *narrow*, barata e paralela.
2. **`dropDuplicates()`** (duplicatas exatas): **shuffle** pela linha inteira.
3. **`row_number()` sobre `Window.partitionBy(chave)`** (conflitos): **shuffle** pela chave natural.
4. **`persist()`:** o resultado da janela é usado várias vezes (contagens, vencedores, perdedores). Sem persistir, cada
   ação recalcularia tudo desde a bronze. Regra: persista o que é reutilizado em mais de uma ação, e libere depois
   (`unpersist`).
5. **`MERGE` do Delta** (o SCD2):
   - Primeiro um **join** entre a origem (o mês) e o destino (o histórico), pela `_key`. Com dezenas de milhões de
     linhas dos dois lados é um *sort-merge join*: dois shuffles.
   - Depois o Delta **reescreve os arquivos** que contêm linhas alteradas (*copy-on-write*). Muitos arquivos tocados
     significam muita escrita.
   - `WHEN NOT MATCHED BY SOURCE` obriga a olhar o destino inteiro, para achar as chaves que sumiram.
   - O resultado é **um commit atômico**: ou o mês inteiro entra, ou nada entra.

**O padrão "staged union"** (por que o mesmo registro aparece duas vezes na origem):

| Linha na origem | `merge_key` | Casa com a versão atual? | Ação |
|---|---|---|---|
| CNPJ novo | `_key` | não | INSERT (NEW) |
| CNPJ igual, sem mudança | `_key` | sim, mesmo hash | nada |
| CNPJ mudou (cópia 1) | `_key` | sim, hash diferente | UPDATE: fecha a versão antiga |
| CNPJ mudou (cópia 2) | `NULL` | nunca | INSERT: abre a versão nova |
| CNPJ sumiu do mês | — | sem par na origem | UPDATE: fecha com motivo ABSENT |

Um `MERGE` só pode fazer UPDATE **ou** INSERT para a mesma linha casada. A cópia com chave nula força o INSERT da
versão nova no mesmo comando.

### 2.4 Gold: joins, broadcast e o risco de "fan-out"
- **Dimensões pequenas** (CNAE, municípios, códigos) usam `F.broadcast(...)`: o Spark manda a tabela inteira para cada
  task, e o join acontece **sem shuffle**. Use broadcast quando um lado cabe com folga na memória (dezenas de MB, no
  máximo); nunca com os 70 milhões de empresas.
- **Estabelecimento x empresa** (73 mi x 70 mi): *sort-merge join* pela `cnpj_basico`, com shuffle dos dois lados.
- **Fan-out:** se a chave do lado "1" não for única, o join multiplica linhas em silêncio. Por isso existe o check BLOCK
  `gold.rows_match_silver` (linhas da gold = estabelecimentos válidos no mês). Ele é a defesa clássica contra join
  duplicando dados.
- **"As of M":** toda tabela gold filtra o histórico pela janela de validade, então o mesmo código monta o mês atual ou
  um mês passado.
- `estabelecimento_atual` é particionada por `uf`: consultas por estado leem só a pasta daquele estado (*partition
  pruning*). Partição boa é **de baixa cardinalidade e usada em filtros**. Por CNPJ seria um desastre: milhões de
  pastas e arquivos minúsculos.

### 2.5 Manutenção
- **OPTIMIZE** junta arquivos pequenos (os `MERGE` mensais deixam muitos). Use depois de escritas que fragmentam; não
  adianta numa tabela já compacta.
- **VACUUM** apaga arquivos que nenhuma versão dentro da retenção usa. O efeito colateral é **encurtar o time travel**.
  Por isso o mínimo é 168 horas, e o check `maintenance.vacuum_retention` fiscaliza.
- O Delta cria *checkpoints* do log automaticamente (a cada 10 commits), para que ler a tabela não exija ler milhares
  de JSONs.

### 2.6 Configurações usadas, e por quê

| Configuração | Valor | Motivo |
|---|---|---|
| `spark.sql.shuffle.partitions` | 96 (silver/gold) | ponto de partida; o AQE junta partições pequenas |
| `spark.sql.adaptive.*` | ligado, com `skewJoin` | redistribui chaves "pesadas" no join |
| `spark.sql.parquet.compression.codec` | zstd | 4 a 13x menor que o CSV; disco escasso |
| pool `rfb_spark` = 1 | Airflow | cada app pega os 6 cores; duas ao mesmo tempo se atrapalham |
| `local[4]` nos notebooks | — | explorar sem ocupar o cluster |
| Thrift com 2 cores e 2 GB | — | consultas pelo DBeaver sem travar o pipeline |
| `spark.local.dir` no SSD | `/srv/scratch` | o shuffle é I/O aleatório; num HDD USB seria o gargalo |

### 2.7 Como ler o Spark UI durante a execução (`localhost:4040`)
- **Jobs / Stages:** qual etapa está rodando e quantas tasks faltam. Num stage, compare a **mediana com o máximo**
  da duração das tasks: um máximo muito maior indica **skew** (uma chave com muitos dados).
- **Shuffle Read/Write** e **Spill (memory/disk):** spill alto significa que falta memória para a partição. Saídas:
  mais partições ou mais memória.
- **SQL / DataFrame:** o plano físico. Procure `BroadcastHashJoin` (bom, nas dimensões) contra `SortMergeJoin` (nos
  joins grandes) e confira se os filtros foram empurrados para a leitura (`PushedFilters`).
- **Executors:** GC time alto (mais de 10% do tempo) indica pressão de memória.
- Depois que a aplicação termina, tudo fica no **History Server** (`localhost:18080`), lido do event log.

---

## 3. Modelagem: decisões e quando usar cada uma

### 3.1 Tipos e códigos
- **Código é texto, mesmo quando parece número:** CNPJ, CEP, CNAE, município. Eles têm zeros à esquerda, e o CNPJ, desde
  julho de 2026, pode ter **letras**. O dígito verificador usa `ascii(c) - 48`, a regra oficial que funciona para
  dígitos e letras. Conta aritmética com código é bug.
- **Datas:** `''`, `0` e `00000000` significam "sem data" (nulo, não é erro). Outros valores inválidos também viram
  nulo, mas **contados** (`silver.invalid_dates`).
- **Dinheiro:** `decimal(18,2)`, nunca `double`, por causa do arredondamento binário. O CSV usa vírgula decimal.

### 3.2 Tipos de SCD: quando usar

| Tipo | O que faz | Use quando | Não use quando |
|---|---|---|---|
| 0 | nunca muda | atributo imutável (data de abertura) | algo pode ser corrigido |
| 1 | sobrescreve | só o valor atual importa (contatos) | alguém pergunta "como era antes?" |
| 2 | nova versão com validade | histórico relevante (situação, endereço, CNAE) | o atributo muda sem significado (vira ruído) |
| 3 | coluna "valor anterior" | só a última mudança importa | precisa do histórico completo |
| 2+1 (híbrido) | versões para o que importa, sobrescrita para o resto | **o caso aqui** | — |

Aqui:
- **Estabelecimentos:** cadastro, atividade e endereço geram versão; telefone e e-mail são tipo 1. Mudam muito, têm
  dado pessoal, e versioná-los inflaria o histórico.
- **Domínios:** listas de códigos, em SCD1.

### 3.3 Granularidade e linha de base
- A validade é **mensal** (`YYYYMM`), porque a fonte é um retrato mensal. Precisão por dia seria inventar informação.
- O primeiro mês carregado é a **linha de base**: tudo entra como NEW, e o log de mudanças não registra "inclusões"
  nesse mês. Seriam 73 milhões de falsos eventos.
- Meses entram **em ordem**. O histórico não aceita um mês anterior ao primeiro carregado; para isso é preciso
  reconstruir.

### 3.4 Gold no estilo Kimball
- **Dimensões:** descrições dos códigos.
- **`estabelecimento_atual`:** uma tabela "larga" com tudo do CNPJ no mês. Ótima para consumo, IA e exportação. O
  custo é redundância, aceitável porque ela é reconstruída a cada mês.
- **`fato_mudanca_cadastral`:** **fato de eventos**, com uma linha por atributo alterado. Responde "o que mudou no CNPJ
  X entre os meses Y e Z".
- **`agg_estabelecimentos_mensal`:** **snapshot periódico agregado**. É pequeno e rápido para painéis.
- **Mínimo de dados pessoais na gold:** contatos ficam só na silver.

---

## 4. Confiabilidade

### 4.1 Catálogo de checks (`pipelines/commons/dq.py`)
- **BLOCK** (para a task):
  - preflight de S3, Spark, metastore e montagens;
  - arquivos do contrato presentes;
  - bronze com linhas, e reconciliação com o extract;
  - mês da bronze presente para a silver;
  - ordem dos meses;
  - chave natural única depois da deduplicação;
  - versões atuais da silver = linhas do mês;
  - gold = silver (contra fan-out).
- **WARN** (registra, alerta e segue):
  - espaço livre e componentes de observabilidade;
  - mês ainda não publicado;
  - variação de tamanho e de linhas contra o mês anterior;
  - taxa de quarentena;
  - duplicatas, exatas e conflitantes;
  - datas inválidas;
  - DV de CNPJ inválido;
  - taxa de mudança acima de 20%;
  - códigos órfãos.

**Regra:** BLOCK só para o que produziria um **modelo errado**. O resto é WARN, com registro e alerta.

### 4.2 Idempotência e reexecução
- **Bronze:** `replaceWhere` por mês.
- **Silver:** rollback do último mês seguido de reaplicação, guiado pela propriedade da tabela.
- **Gold:** overwrite completo, ou por partição.
- **Landing:** pula arquivos já válidos.

Reexecutar qualquer task com o mesmo mês dá o mesmo resultado. É isso que torna `retries` seguros.

### 4.3 O que o Airflow faz por você, e as armadilhas
- **Código 99:** a task fica *skipped*, e "mês não publicado" não vira falha falsa.
- **`retries` com `retry_delay`:** o landing tenta 3 vezes (fonte instável); o preflight não tenta de novo (falha rápido).
- **`execution_timeout` por task:** o Airflow mata a task com SIGTERM. O `run_task` transforma isso numa exceção, e o
  audit é fechado.
- **SIGKILL, OOM ou queda do processo:** o *callback* do Airflow marca a tentativa como FAILED no audit.
- **Armadilha clássica:** uma task final com `all_done` que sempre passa faz uma execução quebrada aparecer **verde**,
  porque o estado da DAG vem das tasks folha. Por isso o `finalize` **falha quando a execução falhou**.

---

## 5. Observabilidade (o foco do seu posicionamento)

### 5.1 Quatro sinais, mais o controle

| Sinal | Pergunta | Onde |
|---|---|---|
| Logs | o que aconteceu, evento a evento? | Loki (busca) + `audit.log_event` (SQL) |
| Métricas | quanto e quanto tempo, ao longo do tempo? | Prometheus (Pushgateway + OTel do Airflow) |
| Traces | em que ordem, e onde foi o tempo? | `audit.v_trace_spans` → painel Traces (fase 2: Tempo) |
| Auditoria | quem rodou o quê, com qual resultado, consultável como dado? | Postgres `audit` |
| Controle | de onde retomo? | Delta `ctrl` (não é observabilidade, é estado) |

### 5.2 Os cinco pilares de *data observability* no projeto
- **Freshness** (o dado está atualizado?): watermarks em `ctrl.layer_watermark`, o gauge
  `lambda_pipeline_last_success_timestamp_seconds` e o alerta `LambdaRfbStale` (mais de 40 dias).
- **Volume** (veio a quantidade esperada?): linhas por task e por tabela Delta, reconciliação entre camadas e variação
  contra o mês anterior.
- **Schema** (a estrutura mudou?): o contrato de colunas, o parse PERMISSIVE com quarentena e o schema fixo das
  tabelas.
- **Distribution** (os valores fazem sentido?): taxas de nulos, datas inválidas, DV inválido, duplicatas, taxa de
  mudança e códigos órfãos.
- **Lineage** (de onde veio?): `run_id` em tudo, (task → tabela → versão Delta) em `audit.delta_table_stats` e os
  *Assets* do Airflow.

### 5.3 KPIs e SLIs recomendados

| KPI | Fórmula / origem | Por que importa |
|---|---|---|
| Taxa de sucesso das execuções | `SUCCESS / total` em `audit.pipeline_run` | confiabilidade, a base de um SLO |
| Duração da execução (p50/p95) | `duration_s` | detecta degradação lenta |
| Duração por task e por camada | `audit.task_run`, `lambda_task_duration_seconds` | onde otimizar |
| Throughput (linhas/s) | `rows_out / duration_s` | eficiência do Spark |
| Volume escrito por tabela | `audit.delta_table_stats.rows_written` | anomalias de volume |
| Freshness | `now - última execução com sucesso` | SLA com quem consome |
| Taxa de quarentena | `rows_rejected / rows_in` | qualidade da origem |
| Falhas de check (BLOCK/WARN) | `audit.dq_event` | tendência de qualidade |
| Espera em fila no pool | `start - queued` (banco do Airflow) | contenção de recursos |
| Retries por task | `task_instance_history` | instabilidade (fonte, recursos) |
| Saúde dos arquivos | tamanho médio por arquivo, nº de arquivos | necessidade de OPTIMIZE |
| Saturação | CPU, memória, I/O de disco, cores do Spark | capacidade da máquina |

Isso equivale aos *golden signals* adaptados a batch:
- latência → duração;
- tráfego → linhas;
- erros → falhas e checks;
- saturação → pool, CPU e disco.

**Exemplos de SLO:**
- 95% das execuções mensais com sucesso;
- dados da gold atualizados até 3 dias após a publicação da Receita.

### 5.4 Os dashboards (pasta "Lambda4" (antes "Lambda4 · RFB") no Grafana)
1. **RFB · Pipeline health & tracing** (escolha o `run_id` no topo):
   - visão da execução (status, mês, duração, tasks com falha, linhas escritas, checks, quarentena);
   - trace (execução → tasks → etapas);
   - duração por task e tabela de tasks;
   - desempenho ao longo das execuções (duração, linhas, throughput);
   - tabelas Delta (estado atual, linhas, tamanho, crescimento, arquivos pequenos);
   - qualidade e reconciliação;
   - saúde da plataforma (health checks, CPU, memória, discos, cores do Spark).
2. **Airflow · Orchestration:**
   - heartbeat do scheduler, slots, tasks esperando pool, erros de import;
   - ocupação dos pools;
   - histórico de execuções, task instances e espera em fila;
   - falhas e retries.
3. **Logs · Airflow & pipeline:**
   - *tratados*: eventos por nível e por tipo, avisos e erros por task, tabela de avisos e erros, erros com traceback;
   - *brutos*: logs das tasks do Airflow (filtro por task e busca livre) e eventos JSON do pipeline.

Os dashboards são **código** (`infra/grafana/build_dashboards.py`). Editar no Grafana não persiste
(`allowUiUpdates: false`); edite o gerador e rode `infra/grafana/install.sh`. O
`infra/grafana/validate_dashboards.py` executa todas as consultas.

### 5.5 OpenTelemetry: é aplicável? Sim, e já está em uso
- **O que é:** um padrão aberto para produzir e transportar telemetria (traces, métricas e logs) com um protocolo
  único (OTLP), independente do fornecedor.
- **Já em uso:** o Airflow envia as métricas via OTLP para o **Alloy** (um coletor OTel), que converte e grava no
  Prometheus.
- **Fase 2 (proposta):**
  - instalar o **Grafana Tempo** (backend de traces);
  - criar spans no `TaskRun` com o SDK OpenTelemetry;
  - ligar os traces do Airflow 3.
  
  O ganho é um trace de verdade, com contexto propagado, e logs e métricas ligados pelo `trace_id`. É a fronteira
  atual de *observability engineering* e um diferencial no seu perfil.
- **Hoje:** o trace é montado com SQL a partir do audit. Ele já mostra a mesma hierarquia e o tempo de cada etapa.

### 5.6 Boas práticas aplicadas
- **Labels de baixa cardinalidade no Loki:** `run_id` fica no corpo do log, não no label.
- **Métricas de batch via Pushgateway com `pushadd`:** o "último sucesso" não se perde quando uma execução falha.
- **Observabilidade não derruba o pipeline:** gravação best-effort com spool e replay; preflight separando BLOCK de
  WARN.
- **Alertas versionados no git** (`infra/prometheus/rules/lambda_pipeline.yml`), com descrição do que fazer.
- **Dashboards como código, validados.**

---

## 6. Linhagem: "quem escreveu esta versão?"

```sql
-- todas as versões Delta escritas por uma execução, com a task do Airflow, o script e a aplicação Spark
SELECT t.airflow_task_id, t.task_id AS script_task, t.spark_app_id, s.table_name, s.operation, s.delta_version,
       s.rows_written, s.captured_at
FROM audit.task_run t JOIN audit.delta_table_stats s ON s.run_id = t.run_id AND s.task_id = t.task_id
WHERE t.run_id = '<run_id do Airflow>' ORDER BY s.captured_at;
```

Do outro lado:
- `DESCRIBE HISTORY rfb_silver.estabelecimento_hist` mostra a versão;
- a coluna `_run_id` de cada linha diz qual execução a gravou;
- o `spark_app_id` abre a aplicação no History Server.

---

## 7. Como executar e observar (passo a passo)

1. **Na sua máquina**, abra o túnel (está no CLAUDE.md). Os serviços sob demanda precisam estar no ar:
```bash
sudo systemctl start seaweedfs spark-master spark-worker spark-history loki alloy
```
2. **No Airflow** (`localhost:8080`):
   - **despause** a `rfb_pipeline`;
   - use **Trigger DAG** com `{"ref_month": "2026-09"}`;
   - acompanhe pela Grid; clique numa task para ver o log ao vivo.
3. **No Grafana** (`localhost:3000`), pasta "Lambda4" (antes "Lambda4 · RFB"):
   - *Pipeline health*: o run novo aparece no seletor; atualiza a cada 30 s;
   - *Airflow*: pools e fila;
   - *Logs*: avisos.
4. **No Spark UI** (`localhost:4040` durante uma task Spark): stages, shuffle, spill.
5. **Ao final:**
   - o `finalize` registra o resumo;
   - confira a reconciliação no painel;
   - abra `notebooks/rfb_lakehouse_explore.ipynb`.

**O que esperar:**
- landing, extract e bronze levam o que você já viu: cerca de 22 minutos para baixar (aqui é tudo local, pula), cerca
  de 8 minutos de extract e cerca de 18 minutos de bronze;
- silver e gold são **a incógnita medida pela primeira vez**. O `MERGE` de 73 milhões de linhas deve levar dezenas de
  minutos neste hardware.

**Riscos conhecidos, e onde olhar se falhar:**
- **Memória no `MERGE` e nos joins grandes:** spill e GC no Spark UI. Ação: aumentar `spark.sql.shuffle.partitions`
  ou a memória do executor.
- **HD USB (lake) lento ou desconectando:** I/O do `sdc` no painel de discos e `lab`.
- **Check BLOCK inesperado:** o painel *Checks of the run* mostra o nome, o valor observado e o limite.

---

## 8. Consultando as tabelas
- **Notebook com DuckDB** (`duckdb_lake()`): rápido para explorar; mesmos nomes do Spark.
- **Notebook com Spark** (`build_spark(..., master="local[4]")`): histórico, time travel, CDF.
- **DBeaver em Delta:** `sudo systemctl start spark-thrift`, depois conexão *Apache Spark* (Hive JDBC) em
  `jdbc:hive2://localhost:10000` pelo túnel. Desligue ao terminar (`stop`).
- **DBeaver no audit:** Postgres, papel `dbeaver_ro`.

---

## 9. Perguntas para defender o projeto
- **Por que Spark se o volume cabe em DuckDB?** Para aprender o motor do mercado (e da Databricks). Os conceitos
  (partições, shuffle, MERGE, AQE) escalam para clusters reais; numa máquina só, o DuckDB é mais rápido, e uso ele
  para explorar.
- **Por que Delta e não Iceberg?** Foco na certificação Databricks. Os conceitos são equivalentes, e a dependência real
  está no catálogo (ADR 0001).
- **Por que o Spark ficou na 4.1.0?** O Delta usa APIs internas do Spark e é compilado contra a 4.1.0; a 4.1.3 quebrou
  em tempo de execução (ADR 0002).
- **Como você garante que não perdeu linhas?** Contagem exata no extract, reconciliação BLOCK na bronze, versões
  atuais = linhas do mês na silver e gold = silver.
- **Como funciona o SCD2?** Um `MERGE` atômico por mês, com hash dos atributos e o padrão staged union; o reprocesso é
  idempotente por rollback (ADR 0003).
- **Como você sabe que o pipeline está saudável?** Preflight, checks, métricas, alertas, dashboards e trace (seção 5).
- **O que você melhoraria?**
  - Tempo e OTel SDK (traces reais);
  - liquid clustering ou Z-order na `_key` se o `MERGE` ficar lento;
  - CI rodando os testes;
  - `lab start/stop`;
  - checks de distribuição estatística;
  - roteamento de alertas (e-mail ou Telegram).

---

## 10. Glossário rápido
- **AQE:** reotimização do plano durante a execução.
- **Broadcast join:** a tabela pequena vai inteira para cada task, sem shuffle.
- **CDF (Change Data Feed):** linhas alteradas entre versões de uma tabela Delta.
- **Copy-on-write:** o `MERGE` reescreve os arquivos tocados.
- **Fan-out:** um join que multiplica linhas por causa de uma chave não única.
- **Partition pruning:** ler só as pastas de partição que o filtro pede.
- **replaceWhere:** sobrescrever só as linhas (ou partições) que atendem a um predicado.
- **Skew:** poucas chaves com a maior parte dos dados; algumas tasks demoram muito mais.
- **Spill:** dados que não cabem na memória e vão para o disco local.
- **Staged union:** a técnica do `MERGE` SCD2 em um único comando.
- **Watermark:** o registro de até qual mês cada camada já aplicou.
