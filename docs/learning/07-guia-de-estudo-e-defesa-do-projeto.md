# Relatório de aprendizado 07 — Guia de estudo e defesa do projeto (Spark, Delta, SCD2, Airflow, observabilidade)

Este é o documento para **estudar e sustentar o projeto numa entrevista**. Ele junta tudo o que foi feito até aqui
(relatórios 01–06, ADRs 0001–0005 e `docs/architecture.md`) em uma ordem de estudo: primeiro a visão do projeto,
depois o Spark a fundo, depois Delta, SCD2, orquestração e observabilidade, e por fim as perguntas de entrevista.

> **Como usar.** Leia as seções 1–2 de uma vez (30 min) e treine o pitch da seção 13. Estude a seção 3 (Spark) e a
> seção 6 (SCD2) com o código aberto ao lado. Na véspera de uma entrevista, releia as seções 10 (maturidade),
> 12 (perguntas) e 14 (cola de números). Onde aparece **[Maturidade]**, é um ponto que diferencia um projeto
> "de estudo" de um projeto "de produção" — destaque-o ao defender o trabalho.

> **Honestidade acima de tudo.** Em entrevista, diga sempre o que foi *medido* e o que foi *projetado*. Neste projeto:
> a primeira execução completa (3 h 33 min) é **medida**; os ganhos do tuning (seção 3.9) foram medidos em Spark local
> com dados sintéticos e são **projeção** para o cluster até a próxima execução. Isso é um ponto forte, não fraco.

---

## 1. O projeto em uma página

**Problema.** A Receita Federal publica todo mês a base aberta de CNPJ: 37 arquivos ZIP (~7 GB compactados), ~73 milhões
de estabelecimentos, ~70 milhões de empresas, ~50 milhões de registros do Simples/MEI e ~28 milhões de sócios. O
arquivo é um *retrato completo* (snapshot) do mês: não diz **o que mudou**. A pergunta de negócio é: *"o que mudou,
em qual CNPJ, entre quais meses?"*

**Solução.** Um pipeline de engenharia de dados em arquitetura **medalhão (bronze → silver → gold)** sobre um
**lakehouse** com **Delta Lake**, processado em **PySpark**, orquestrado por **Airflow**, com **histórico SCD2** dos
atributos cadastrais e **observabilidade completa** (logs, métricas, traces, auditoria, alertas, dashboards como
código). Tudo roda num servidor doméstico (i7, 32 GB, SSD + HDDs), sem Docker, serviços via `systemd`.

```
Receita Federal (37 ZIPs/mês)
   │  rfb01 landing: download com retomada, sha256, validação de tamanho
   ▼
landing (HDD 1 TB)  ──  rfb02 extract: ZIP → CSV em pedaços alinhados a registros (UTF-8)
   ▼
BRONZE  (Delta, tudo string, "como publicado", + colunas de linhagem, partição _ref_month)
   ▼  rfb04: tipagem, limpeza, dedup, SCD2 (um MERGE por mês)
SILVER  (Delta, histórico: uma linha por VERSÃO de cada CNPJ)
   ▼  rfb05: dimensões, "atual", fato de mudanças, agregado mensal
GOLD    (Delta, modelo Kimball)  ──►  DuckDB / DBeaver (Thrift JDBC) / notebooks
```

**Resultados reais (execução de 2026-09, `manual__2026-10-21T11:01…`).** 23 tarefas, 0 BLOCK, 3 WARN (sócios: 292
duplicatas exatas + 31 chaves conflitantes; empresas: 1 chave conflitante → quarentena), 3 h 33 min, 47 GB no lake.
O tuning posterior (ADR 0005) atacou os dois maiores gargalos (silver de estabelecimentos 53 min, manutenção 69 min).

**Por que o projeto é bom para portfólio.** Ele cobre o ciclo inteiro de um produto de dados real — ingestão
robusta, modelagem, processamento distribuído, qualidade, governança de erro, histórico temporal, orquestração,
observabilidade, tuning *guiado por medição* e documentação de decisões (ADRs). Cada uma dessas coisas é pergunta
frequente de entrevista.

---

## 2. A stack: o que é, para que serve, por que foi escolhido

| Peça | Função no projeto | Por que (e alternativa considerada) |
|---|---|---|
| **PySpark 4.1.0** | Motor de processamento distribuído: lê CSV/Delta, tipa, deduplica, faz MERGE, agrega | Volume (73 M linhas × 30 colunas) passa do que pandas faz bem; é a habilidade mais pedida no mercado. *Alternativas:* DuckDB/Polars (excelentes até ~centenas de GB num nó; usamos DuckDB só para servir/consultar), dbt (SQL sobre warehouse) |
| **Delta Lake 4.1.0** | Formato de tabela transacional sobre Parquet: ACID, MERGE, time travel, CDF, schema enforcement | Precisamos de `MERGE` (SCD2) e atomicidade; alinhado à certificação Databricks (ADR 0001). *Alternativa:* Iceberg (instalado como catálogo `lambda`, não usado no pipeline) |
| **Hive Metastore embutido (Postgres)** | Catálogo: nomes de tabela (`rfb_silver.estabelecimento_hist`) → local dos arquivos | Tabelas nomeadas sem subir outro serviço; o Postgres já existe (ADR 0001) |
| **SeaweedFS (S3)** | Armazenamento de objetos compatível com S3 (`s3a://bronze/…`) | Mesma API do S3 da AWS → o código roda na nuvem trocando só o endpoint |
| **Airflow 3** | Orquestração: DAG `rfb_pipeline`, dependências, retries, pools, parâmetro `ref_month` | Padrão de mercado; DAG como código; pool `rfb_spark=1` evita duas aplicações Spark disputando 6 cores |
| **Postgres `audit`** | Auditoria consultável por SQL: runs, tasks, eventos de qualidade, logs, stats de tabelas Delta | Responde "quem rodou o quê, com que resultado" com SQL e alimenta o Grafana |
| **Tabelas `ctrl` (Delta)** | Estado de controle: qual mês foi baixado/processado e de onde retomar (watermarks) | Separado do audit de propósito (regra do dono): `ctrl` = retomada; `audit` = histórico/observabilidade |
| **Loki + Alloy** | Logs estruturados (JSON, um evento por linha) coletados e pesquisáveis | Logs com `trace_id` ligam log ↔ trace |
| **Prometheus + Pushgateway + Alertmanager** | Métricas de jobs batch (push) e de infra, regras de alerta | Jobs batch curtos não são "scrapeáveis": usam Pushgateway |
| **OpenTelemetry + Tempo** | Traces distribuídos: onde o tempo da execução foi gasto, tarefa → etapa | Padrão aberto; ids determinísticos (`md5(run_id)`) ligam todos os processos |
| **Grafana (dashboards como código)** | Painéis de saúde do pipeline, qualidade, Airflow, logs, traces | Versionados em Python (`build_dashboards.py`), validados (`validate_dashboards.py` roda toda query) |
| **DuckDB** | Serving local: lê Delta com `delta_scan`, sem cluster | Consultas rápidas para DBeaver/notebooks sem ligar o Spark |
| **Spark Thrift Server** | JDBC sobre Delta para o DBeaver (sob demanda) | Acesso SQL externo ao lake |

**Decisão de infraestrutura que vale defender:** *sem Docker, serviços `systemd`, ciclo de vida sob demanda*
(serviços pesados só sobem quando necessários). Racional: um servidor único e limitado; menos camadas = menos pontos
de falha e mais aprendizado do que cada serviço realmente faz.

---

## 3. Spark — o que você precisa saber para sustentar

### 3.1 O papel do Spark neste projeto

O Spark é o **motor de transformação** das camadas bronze, silver e gold:
- **bronze:** lê os CSV (pedaços alinhados), aplica o schema "tudo string", separa linhas corrompidas (quarentena),
  grava Delta particionado por `_ref_month`;
- **silver:** tipa e limpa ~30 colunas, deduplica por chave natural, calcula hashes e executa o `MERGE` SCD2;
- **gold:** junta histórico + dimensões, constrói a visão "atual", o fato de mudanças e o agregado mensal;
- **manutenção:** `OPTIMIZE`, `VACUUM`, estatísticas das tabelas.

Ele roda em **modo standalone** (master `spark://127.0.0.1:7077` + 1 worker de 6 cores), com executor de 8 GB e
driver de 4 GB. Um **pool do Airflow de 1 slot** garante uma aplicação Spark por vez (ela ocupa todos os cores).

### 3.2 Arquitetura: driver, executors, cluster manager

- **Driver:** o processo Python/JVM que roda seu código, monta o plano e agenda o trabalho. Mantém o `SparkSession`.
- **Executors:** JVMs nos workers que executam as **tasks** e guardam cache/shuffle. Aqui: 1 executor, 6 cores,
  8 GB → 6 tasks em paralelo.
- **Cluster manager:** quem aloca recursos (aqui o *standalone* do próprio Spark; em produção: YARN, Kubernetes,
  Databricks).
- **Hierarquia de execução:** *aplicação* → **jobs** (um por *ação*) → **stages** (separados por *shuffle*) →
  **tasks** (uma por partição, por stage).

### 3.3 Transformações, ações e avaliação preguiçosa (lazy)

- **Transformação** (`select`, `filter`, `join`, `withColumn`, `groupBy…agg`): só descreve o plano; nada executa.
- **Ação** (`count`, `write`, `collect`, `first`, `show`): dispara a execução.
- Consequência prática: o **Catalyst** (otimizador) enxerga o plano inteiro e o reescreve (corta colunas
  não usadas, empurra filtros para a leitura, escolhe o tipo de join). Por isso preferimos a **API de DataFrame/SQL**
  a UDFs Python (que são caixas-pretas para o Catalyst e lentas: serializam linha a linha entre JVM e Python).
- Armadilha: cada ação **recalcula** o plano se nada foi persistido. Por isso o pipeline faz `persist()` do DataFrame
  deduplicado (`ranked`) e do source do MERGE: senão a leitura + tipagem + dedup seriam repetidas a cada `count()`.

### 3.4 Transformações estreitas × largas e o **shuffle**

- **Estreita** (*narrow*): cada partição de saída depende de **uma** partição de entrada (`select`, `filter`,
  `withColumn`). Roda no mesmo stage, sem movimentar dados.
- **Larga** (*wide*): a saída depende de **várias** partições de entrada (`groupBy`, `join` não-broadcast, `distinct`,
  `window`, `orderBy`, `repartition`). Exige **shuffle**.

**Shuffle = redistribuir dados entre executors para que linhas com a mesma chave fiquem juntas.** Como funciona:
1. *Shuffle write* (fim do stage anterior): cada task particiona seus dados pela chave (hash) em N pedaços e grava
   em **arquivos locais em disco** (comprimidos).
2. *Shuffle read* (início do stage seguinte): cada task busca, de **todos** os executors, o pedaço que lhe cabe.

Por que é caro: **serialização + escrita em disco + rede + leitura + possível ordenação**. É quase sempre o maior
custo de um job. Regras de ouro:
- menos shuffles é melhor (aqui: o dedup tinha 2, agora tem 1);
- filtre e projete colunas **antes** do shuffle (menos bytes trafegados);
- dados com **skew** (chave muito mais frequente que as outras) deixam uma task lenta segurando o stage inteiro.

No projeto, a window do dedup particiona por `cnpj` (14 caracteres): o Spark faz *hash partitioning* por essa chave,
ordena dentro de cada partição e numera as linhas (`row_number`).

### 3.5 Particionamento — três coisas diferentes com o mesmo nome

É a confusão clássica. Saiba separar:

1. **Partições de entrada (leitura).** Em quantos pedaços o Spark divide a *leitura*. Governadas por
   `spark.sql.files.maxPartitionBytes` (padrão 128 MB) e pelo número/tamanho dos arquivos. No bronze real foram 32
   tasks de leitura — **não** é partição de shuffle (equívoco que cometemos no começo da investigação).
2. **Partições de shuffle.** Em quantos pedaços os dados são redistribuídos depois de uma operação larga. Governadas
   por `spark.sql.shuffle.partitions` (padrão 200; aqui 96 e agora **192** no silver). Cada partição vira **uma
   task**. Poucas partições = tasks grandes (spill, falta de paralelismo); demais = overhead de agendamento e muitos
   arquivos pequenos. Referência prática: **100–200 MB por partição** de dados *em memória/shuffle* como ponto de
   partida; ajuste olhando spill e duração das tasks.
3. **Particionamento de tabela (`partitionBy`).** Como os **arquivos são organizados em pastas** no armazenamento
   (`_ref_month=202609/`, `uf=SP/`). Serve para *partition pruning* (ler só as pastas necessárias). Escolha colunas de
   **baixa cardinalidade** que aparecem em filtros. Particionar por coluna de alta cardinalidade gera milhões de
   arquivos minúsculos (o *small files problem*).

Funções para controlar partições em memória:
- `repartition(n)` / `repartition(n, col)`: **com** shuffle; aumenta ou redistribui por chave.
- `coalesce(n)`: **sem** shuffle; só reduz o número de partições juntando vizinhas (pode desbalancear).

### 3.6 AQE (Adaptive Query Execution)

Ligado por padrão no Spark 3.2+. O Spark replaneja em tempo de execução, usando estatísticas reais dos stages já
concluídos:
- **Coalescing de partições de shuffle:** junta partições pequenas (alvo `spark.sql.adaptive.advisoryPartitionSizeInBytes`,
  padrão 64 MB). Por isso configuramos um número *alto* de partições e deixamos o AQE reduzir.
- **Skew join:** divide partições gigantes em sub-partições.
- **Troca de estratégia de join** (sort-merge → broadcast) quando um lado fica pequeno.

Limite: o AQE só otimiza **depois** de um shuffle; ele não conserta um stage pesado *antes* dele (o nosso caso: o
stage de CPU alta ficava antes do primeiro shuffle).

### 3.7 Joins, window functions e cache

- **Broadcast hash join:** envia a tabela pequena inteira a todos os executors; **sem shuffle** do lado grande.
  Limite padrão: `spark.sql.autoBroadcastJoinThreshold` = 10 MB. É o que se usa para dimensões (gold ↔ `dim_*`).
- **Sort-merge join:** o padrão para dois lados grandes; shuffle + sort dos dois lados.
- **Window functions** (`row_number() over (partition by … order by …)`): shuffle por `partition by`, sort por
  `order by`. Dão o dedup determinístico e o `lag` (comparar com a linha anterior).
- **Cache/persist:** `persist(StorageLevel.MEMORY_AND_DISK)` guarda o resultado para reuso. Custa memória/disco e
  deve ser **liberado** (`unpersist`), como no `try/finally` do `scd2.apply_month`.

### 3.8 Memória, spill e GC

- Memória do executor (modelo unificado): uma fração (`spark.memory.fraction`, ~60 %) é compartilhada entre
  **execução** (shuffle, sort, aggregate) e **armazenamento** (cache). O resto é para estruturas do usuário/metadados.
- **Spill:** quando sort/aggregate não cabem na memória de execução, o Spark **despeja em disco** e depois mescla. É
  correto, mas lento. Métricas: *Spill (Memory)* e *Spill (Disk)* (o primeiro é o tamanho desserializado; o segundo,
  o gravado). Spill alto = partições grandes demais para a memória disponível por task.
- Memória por task ≈ memória de execução ÷ cores simultâneos. Com 6 cores, cada task tem ~1/6 da memória de execução:
  por isso partições **menores** (192) param de gerar spill.
- **GC:** tempo de coleta de lixo da JVM; acima de ~10 % do tempo da task é sinal de pressão de memória. Aqui era < 2 %
  (o gargalo era CPU, não memória).

### 3.9 O caso de tuning (a melhor história de entrevista deste projeto)

Use o formato STAR (Situação, Tarefa, Ação, Resultado):

- **Situação.** Primeira execução real: 3 h 33 min. `estabelecimentos.silver` levou 53 min e a manutenção 69 min. O
  Spark UI mostrava ~15 GB de spill em disco e ~30 GB em memória.
- **Tarefa.** Reduzir o tempo sem mudar o resultado.
- **Ação (método, não palpite).**
  1. Abri os *event logs* no History Server (API REST) e identifiquei **um único estágio de 25 min** (32 tasks).
  2. Métricas por task: CPU 239 s de 271 s, GC < 2 % → o gargalo era **CPU**, não memória/disco. O spill era sintoma.
  3. Plano SQL do estágio: `scan → Project (tipagem) → HashAggregate parcial` (o lado *map* do `dropDuplicates`), 73,4 M
     linhas entrando e 73,4 M saindo (redução zero).
  4. Reproduzi localmente com 13,2 M linhas sintéticas (o modelo local reproduziu o job real: 338 s × 5,56 ≈ 1880 s
     contra 1919 s medidos). A tipagem sozinha custava **73 µs de CPU por linha**.
  5. *Profiling* com Java Flight Recorder: ~30 % em `java.util.regex` — 4 `regexp_replace` por valor × 10 colunas de
     texto, cada um convertendo `UTF8String ↔ String` e rodando o motor de regex.
  6. Troquei por `translate` (um passe por caracteres) + uma regex guardada por `contains`; provei equivalência
     (`exceptAll` = 0 linhas diferentes em 500 mil valores, incluindo casos adversariais).
  7. Dedup: removi o `dropDuplicates` (aggregate + 2º shuffle que quase não remove nada) e passei a uma **única
     window** ordenada por todas as colunas (ordem total ⇒ vencedor determinístico).
  8. Subi as partições de shuffle de 96 para 192 → spill de 9,9 GB para 1,4 GB.
  9. Manutenção: o Delta trata como "pequeno" todo arquivo < 1 GiB e reescreve cada bloco de 1 GiB em **uma** task. Ligamos
     *optimized writes* (arquivos de ~256 MiB direto na escrita) e o `OPTIMIZE` agora só roda em tabelas com ≥ 8 arquivos de média < 64 MiB.
- **Resultado (medido localmente).** Tipagem 73 → 34 µs/linha (−53 %); trecho do dedup 338 → 182 s (−46 %); escrita
  do silver: 5 arquivos de 185 MB em vez de 35 de 27 MB, sem custo extra. **Projeção** para o cluster:
  `estabelecimentos.silver` 53 → ~38 min; manutenção 69 → ~10–15 min. A confirmar na próxima execução.

> **[Maturidade]** Medir antes de mexer, provar equivalência de saída, comparar variantes isolando uma mudança de cada
> vez, documentar a decisão (ADR 0005) e admitir o que é projeção. É exatamente o que se espera de um engenheiro
> pleno/sênior.

**Lição central para repetir em entrevista:** *"Spill e shuffle grande são sintomas. O Spark UI mostra onde o tempo está;
só depois de saber se o gargalo é CPU, I/O, memória ou skew eu escolho a alavanca."*

### 3.10 Formatos e compressão

- **Parquet:** formato colunar. Leitura só das colunas necessárias, *predicate pushdown* (estatísticas min/max por
  grupo de linhas), compressão por coluna. É a base do Delta.
- **zstd** (escolhido): comprime texto muito melhor que snappy e descomprime rápido; disco aqui é escasso.
- CSV na entrada: sem schema e sem estatísticas; por isso o bronze o converte uma vez para Delta/Parquet.

### 3.11 Como ler o Spark UI / History Server (para dizer em entrevista)

1. **Jobs → Stages:** qual estágio domina o tempo?
2. **Stage → Summary Metrics (quantis):** distribuição de duração, spill, shuffle read por task. Mediana ≈ máximo ⇒
   sem skew. Máximo muito maior ⇒ skew.
3. **Aba SQL:** o plano com métricas por operador (linhas, tempo de aggregate, "sort fallback", spill).
4. **Storage/Executors:** memória em uso, cache, tempo de GC.
5. Compare **CPU time** com **run time**: iguais ⇒ CPU-bound; CPU baixo ⇒ espera de I/O/rede.

---

## 4. Delta Lake — o que sustentar

- **O que é:** uma camada de metadados (o `_delta_log/`, JSONs + checkpoints Parquet) sobre arquivos Parquet. Cada
  commit adiciona um arquivo de log com as ações `add`/`remove` de arquivos. O estado da tabela em uma versão = o
  resultado de reaplicar o log.
- **ACID em storage de objetos:** atomicidade vem de o commit ser **um único arquivo de log criado de forma atômica**
  (*put-if-absent*). Leitores veem a tabela inteira na versão N ou N+1, nunca no meio. Isolamento por *snapshot* e
  controle de concorrência **otimista** (conflito ⇒ o commit falha e tenta de novo).
- **Limite conhecido (ADR 0001):** no S3 sem coordenador, o Delta é seguro com **um escritor por tabela**. Aqui só uma
  DAG escreve cada tabela. Com múltiplos escritores é preciso coordenador (LogStore DynamoDB, Unity Catalog).
- **MERGE:** `MERGE INTO … USING … ON … WHEN MATCHED/NOT MATCHED [BY SOURCE]`. Núcleo do SCD2. Implementação: join
  entre alvo e fonte para achar arquivos afetados, reescrita desses arquivos.
- **Time travel:** `VERSION AS OF` / `TIMESTAMP AS OF`, limitado pelo `VACUUM`.
- **Change Data Feed (CDF):** ligado nas tabelas silver; registra mudanças linha a linha (útil para auditar/gold incremental).
- **Schema enforcement e evolution:** o Delta recusa escrita com schema diferente, a não ser que se peça
  (`overwriteSchema`, `mergeSchema`).
- **OPTIMIZE:** compacta arquivos pequenos. **Z-ORDER:** reordena dados por colunas para melhorar *data skipping*.
- **VACUUM:** apaga arquivos que nenhuma versão dentro da retenção ainda precisa. **Retenção mínima de 7 dias
  (168 h)**: abaixo disso, leitores/escritores longos podem quebrar e o time travel é perdido.
- **Optimized writes / auto compaction:** reduzem arquivos pequenos *na escrita* (usamos optimized writes).
- **Particionamento:** aqui só onde faz sentido (`_ref_month`, `uf`, `ref_month` do fato).

**Por que Delta e não só Parquet:** Parquet puro não tem transação, `MERGE`, nem versão. Reescrever "a pasta inteira" a
cada mês não é seguro nem barato. **Por que Delta e não Iceberg (decisão defensável):** alinhamento com a certificação
Databricks; mas conheço a equivalência — Iceberg tem *snapshots/manifests* no lugar do `_delta_log`, evolução de
partição e catálogos abertos; o mercado converge (UniForm), e o *catálogo* é o lock-in real.

**Pegadinhas reais encontradas (cite como experiência):**
- Delta compila contra **classes internas** do Spark; `Spark 4.1.3 + Delta 4.1.0` quebrava (`NoClassDefFoundError`,
  `NoSuchMethodError`). Solução: fixar Spark em 4.1.0 (ADR 0002). *Nunca atualizar Spark ou Delta sozinho.*
- A extensão SQL do Delta quebra `spark.sql(..., args=...)` (parâmetros): uso a API de DataFrame.
- Inicialização do metastore com `autoCreateAll=true` causava deadlock no Postgres: schema criado **uma vez e
  explicitamente**.

---

## 5. Arquitetura medalhão: o que cada camada garante

| Camada | Contrato | Decisões aplicadas |
|---|---|---|
| **Bronze** | Cópia fiel da fonte: **tudo string**, sem perda | Colunas de linhagem (`_source_file`, `_ingested_at`, `_run_id`, `_ref_month`); linha corrompida ⇒ **quarentena**, nunca descarte silencioso; partição por mês; retenção de 3 meses |
| **Silver** | Dados **tipados, limpos, sem duplicatas, com histórico** | Códigos (CNPJ, CEP, CNAE) permanecem **string** (zeros à esquerda; CNPJ alfanumérico desde julho/2026); datas `yyyyMMdd` com `0`/`00000000` ⇒ NULL; dinheiro com vírgula ⇒ `decimal(18,2)`; texto: SUB→`?`, controles removidos, espaços colapsados; chaves **naturais**; SCD2 |
| **Gold** | Modelos de **consumo** (Kimball) | Dimensões (`dim_cnae`, `dim_municipio`…), `estabelecimento_atual` (1 linha por CNPJ, sem PII de contato, particionada por `uf`), `socio_atual`, `fato_mudanca_cadastral` (fato de evento), `agg_estabelecimentos_mensal` (snapshot periódico) |

**Por que bronze "tudo string":** a fonte é instável (datas inválidas, caracteres de controle, mudança de formato do
CNPJ). Guardar o original permite **reprocessar** o silver quando a regra de limpeza mudar, sem baixar nem reler os ZIPs da fonte.

**Por que quarentena:** linhas ruins vão para `audit.quarantine` com motivo e registro bruto. Nada some sem rastro.

**Por que chaves naturais (e não `md5` de tudo):** a chave de negócio (`cnpj_basico+ordem+dv`) tem significado, é
estável e permite *comparar versões da mesma chave*. Um hash serve só para detectar *mudança de atributos*, comparado
apenas entre versões **da mesma chave** — então 64 bits (`xxhash64`) bastam (não precisa de unicidade global).

---

## 6. SCD2 — o conceito e a nossa implementação

### 6.1 O conceito

*Slowly Changing Dimension* (dimensão que muda devagar): como guardar atributos que mudam ao longo do tempo.
- **Tipo 1:** sobrescreve o valor; **não** guarda histórico.
- **Tipo 2:** **nunca sobrescreve**; fecha a versão antiga e **insere uma nova**, com período de validade.
- (Tipo 3: coluna "valor anterior"; Tipo 4/6: variantes — saiba que existem.)

**Exemplo.** O CNPJ 11.222.333/0001-81 trocou de nome fantasia em setembro:

| cnpj | nome_fantasia | valid_from | valid_to | is_current | change_type | closed_reason |
|---|---|---|---|---|---|---|
| 11222333000181 | LOJA A | 202608 | 202608 | false | NEW | CHANGED |
| 11222333000181 | LOJA B | 202609 | *NULL* | true | CHANGED | *NULL* |

Perguntas que isso responde: "qual era o nome em agosto?" (`valid_from ≤ 202608 ≤ valid_to`), "quando mudou?",
"quais CNPJs mudaram de situação cadastral no mês?".

### 6.2 Por que SCD2 aqui

A fonte publica **retratos completos** mensais. Sem SCD2 você só tem "o estado atual" (ou 12 cópias gigantes). Com SCD2
guardamos **apenas as mudanças** (uma linha nova só quando algo rastreado muda) — o armazenamento cresce com a
*taxa de mudança*, não com o número de meses.

### 6.3 Nossa implementação (ADR 0003, `pipelines/commons/scd2.py`)

**Colunas:** dados de negócio + `_key`, `row_hash`, `t1_hash`, `valid_from`, `valid_to` (inteiros `YYYYMM`),
`is_current`, `change_type` (`NEW`|`CHANGED`), `closed_reason` (`CHANGED`|`ABSENT`), `_run_id`, `_updated_at`.

**Classificação das colunas (decisão de modelagem):**
- **Rastreadas (tipo 2):** endereço, situação cadastral, CNAE, nome fantasia… (mudou ⇒ nova versão);
- **Tipo 1:** telefone, e-mail, `cnpj_dv_valido` — mudam muito e são dado pessoal: guardamos só o valor atual, sem
  gerar "ruído" de versões nem histórico de PII.

**Detecção de mudança:** `row_hash = xxhash64(colunas rastreadas)` (nulo-seguro: NULL vira um token; sensível à ordem).
Hash diferente da versão corrente ⇒ mudou. `t1_hash` faz o mesmo para as colunas tipo 1.

**Um `MERGE` atômico por mês** (padrão *staged union*):
1. `staged` = todas as linhas do mês com `merge_key = _key` (casam com a versão corrente da chave)
   **+** as linhas que mudaram *de novo* com `merge_key = NULL` (nunca casam ⇒ viram **INSERT** da nova versão).
2. `MERGE … ON t._key = s.merge_key AND t.is_current`
   - `WHEN MATCHED AND hash difere` ⇒ **fecha** a versão (`valid_to = M-1`, `is_current=false`, `CHANGED`);
   - `WHEN MATCHED AND só t1_hash difere` ⇒ atualiza as colunas tipo 1 **no lugar**;
   - `WHEN NOT MATCHED` ⇒ **insere** (chave nova ou nova versão);
   - `WHEN NOT MATCHED BY SOURCE AND is_current` ⇒ a chave **sumiu** do mês: fecha com `ABSENT`.

Por que atômico: uma falha no meio não deixa "versão antiga fechada e nova não inserida" — o Delta faz o commit inteiro ou nada.

**Idempotência e ordem dos meses [Maturidade]:**
- Meses aplicados **em ordem** (BLOCK `silver.month_order`): primeira carga = baseline; `M == último+1` aplica;
  `M == último` ⇒ **rollback(M) e reaplica**; buraco ou mês passado ⇒ BLOCK.
- `rollback(M)` apaga as versões nascidas em M e **reabre** as que M fechou. A tabela guarda `lambda.scd2.last_month`
  como propriedade (um mês sem nenhuma mudança não deixa rastro nas linhas; os dados sozinhos não provam qual é o
  último mês).
- O watermark no `ctrl.layer_watermark` registra `RUNNING/DONE/FAILED` e protege contra "MERGE feito, watermark não".

**Consulta em um ponto do tempo (*as of*):** `valid_from <= M AND (valid_to IS NULL OR valid_to >= M)`. O gold usa isso
para reconstruir qualquer mês e para derivar o **log de mudanças** (diff atributo a atributo entre versões consecutivas).

**Pré-condições do MERGE:** exatamente **uma linha por chave** na fonte (por isso o dedup vem **antes**; depois dele, a
unicidade da chave natural é BLOCK). Duplicata exata ⇒ WARN; chaves iguais com valores diferentes ⇒ vencedor
determinístico (`dedup_order`) e perdedores para a quarentena.

### 6.3.1 Armadilhas e trade-offs (mostre que você os conhece)

- **Meses fora de ordem** corrompem o histórico: por isso BLOCK e `max_active_runs=1`.
- **Um escritor por tabela** (limite Delta em S3).
- **Custo do MERGE:** reescreve os arquivos que contêm chaves afetadas; mudanças espalhadas tocam quase todos os
  arquivos. Alternativas: *deletion vectors*, particionar/clusterizar pela chave.
- **Chave de sócios** não é limpa na fonte: composta (`cnpj_basico`, tipo, documento mascarado, nome); decisão
  validada com dados do piloto.
- **Backfill para trás** exige reconstruir o silver (por isso a limpeza descrita no runbook).
- **Rastrear demais** gera versões de ruído; **de menos** perde informação. A classificação tipo 1 × tipo 2 é decisão
  de negócio e de privacidade.

---

## 7. Orquestração (Airflow)

- **DAG `rfb_pipeline`:** 24 tarefas; parâmetro `ref_month`; criada **pausada** (execução manual); `max_active_runs=1`.
- **Dependências:** preflight → landing → (extract → bronze → silver) por entidade → gold (dims antes das tabelas que
  usam dimensões) → manutenção → `finalize` (`all_done`: roda sempre e **falha se a execução falhou**).
- **Pools:** `rfb_spark=1` (uma aplicação Spark por vez) e `rfb_extract=1`; `priority_weight` maior na trilha crítica.
- **Contrato de saída das tarefas:** `0` ok, `99` **skip** (ex.: mês não publicado), `1` falha. SIGTERM vira falha
  registrada. Callbacks fecham a linha de auditoria quando o Airflow mata a tarefa (timeout/OOM).
- **Princípios:** tarefas **idempotentes** (reexecutar o mesmo `ref_month` dá o mesmo resultado), **sem estado no
  Airflow** (o estado vive em `ctrl`), DAG enxuta (o trabalho está nos scripts testáveis fora do Airflow).

> **[Maturidade]** Separar a orquestração da lógica: cada tarefa é um módulo Python com CLI (`python -m
> pipelines.rfb.rfb04_silver --ref-month 2026-09 --entity …`), testável sem Airflow e reexecutável à mão.

---

## 8. Qualidade, confiabilidade e governança de erro

- **Registro único de severidade** (`pipelines/commons/dq.py`): **BLOCK** só para falhas que quebram o modelo (arquivo
  do contrato ausente/ilegível, bronze com 0 linhas, mês fora de ordem, chave natural duplicada no silver). **WARN** para
  o resto: continua, registra em `audit.dq_event`, emite métrica e alerta.
- **Quarentena:** linhas ruins vão para `audit.quarantine` (nunca descartadas em silêncio).
- **Checks que valem citar:** reconciliação de linhas entre camadas, taxa de mudança por mês (`silver.change_rate` > 20 %
  sinaliza anomalia), CNPJs com dígito verificador inválido, datas inválidas (`silver.invalid_dates`), órfãos no gold.
- **Landing robusto:** contrato de 37 arquivos, retentativas com backoff, validação de tamanho, sha256, retomada.
- **`ctrl` × `audit` × logs** (regra do dono): `ctrl` = só o estado de retomada; `audit` = histórico para análise
  (Postgres, lido pelo Grafana); logs = JSON estruturado para o Loki.
- **Contrato de dados explícito** (`pipelines/rfb/contract.py`): colunas esperadas por entidade, arquivos esperados.
- **Testes:** 59 testes (Spark local com Delta em pasta temporária, nunca o lake) + lint `ruff`.

---

## 9. Observabilidade — os quatro sinais e como se ligam

| Sinal | Pergunta que responde | Aqui |
|---|---|---|
| Log | *o que aconteceu?* | JSON, um evento por linha → Loki (`job=lambda-pipelines`, `job=airflow-tasks`) |
| Métrica | *quanto, com que frequência?* | Pushgateway/Prometheus (`lambda_*`, `airflow_*`), regras de alerta |
| Audit | *quem rodou o quê, com qual resultado?* | Postgres `audit` (SQL) |
| Trace | *onde o tempo foi gasto?* | OpenTelemetry → Alloy → Tempo: execução → tarefa → etapa |

**Correlação [Maturidade]:** `trace_id = md5(run_id)` em todos os processos (cada tarefa é um processo separado, então
propagar contexto seria frágil). O mesmo id aparece em audit, logs, spans e na view SQL `audit.v_trace_spans`. O
Airflow tem seu próprio trace por execução, e nossos spans se ligam ao `worker.<tarefa>` via `TRACEPARENT`.

**Princípio:** a observabilidade **nunca bloqueia** o pipeline (sem coletor ⇒ no-op com um aviso). Dashboards são código
(versionados e validados). Cardinalidade controlada (`run_id` nunca vira label de métrica).

---

## 10. O que torna este projeto maduro (checklist para destacar)

1. **Idempotência de ponta a ponta** — reexecutar um mês produz o mesmo estado (rollback + reaplicação no SCD2,
   partição substituída no bronze).
2. **Histórico temporal correto (SCD2) com ordem protegida** — não é só "um MERGE": há watermark, rollback, ordem de meses.
3. **Política de falha explícita (BLOCK × WARN) e quarentena** — falha-se só quando o modelo quebra; nada é descartado em silêncio.
4. **Contrato de dados e validação na borda** (37 arquivos, colunas, tamanho, sha256).
5. **Observabilidade como requisito**, não enfeite: logs/métricas/traces/audit/alertas, correlacionados por um id.
6. **Separação `ctrl` × `audit`** — estado de controle ≠ histórico analítico.
7. **Tuning guiado por medição** com prova de equivalência e ADR (seção 3.9).
8. **Decisões documentadas (ADRs 0001–0005)** com contexto, decisão e consequências — inclusive erros (deadlock do
   metastore, incompatibilidade Spark/Delta).
9. **Testes automatizados** em Spark local + lint; tarefas testáveis fora do Airflow.
10. **Segredos fora do git**, permissões mínimas (role `dbeaver_ro`, `grafana_ro`), PII de contato só como tipo 1 e fora do gold.
11. **Infra como código** (scripts idempotentes, dashboards em Python, regras de alerta versionadas).
12. **Operação consciente de recursos** (pools, ciclo de vida sob demanda, retenção, VACUUM ≥ 168 h, disco escasso).
13. **Honestidade sobre limites** (um escritor Delta, sem CI ainda, HDD USB frágil): saber o que *não* está pronto é maturidade.

---

## 11. Limitações e como eu evoluiria (resposta para "e em produção?")

| Hoje | Em produção na nuvem |
|---|---|
| SeaweedFS (S3 local) | S3/ADLS/GCS — o código já usa `s3a://`; só muda endpoint e credenciais (IAM) |
| Spark standalone, 1 worker | Databricks / EMR / Kubernetes, *autoscaling*, *spot* |
| Hive Metastore em Postgres | Unity Catalog (governança, lineage, permissões por tabela) |
| Airflow local, disparo manual | MWAA/Astronomer/Databricks Workflows, agendamento mensal, *sensor* de publicação |
| Delta, um escritor | Coordenador de commits (Unity Catalog), *deletion vectors* para MERGE |
| Sem CI | CI (pytest + ruff a cada push), ambientes dev/stg/prod, *data contracts* |
| Alertas locais | PagerDuty/Slack, SLOs de frescor e duração |
| Dados sem backup (só código no GitHub) | Replicação / versionamento de bucket |

---

## 12. Perguntas de entrevista (com a resposta curta)

**Spark**
1. *O que é shuffle e por que é caro?* Redistribuição de dados entre executors para juntar linhas por chave. Custa
   serialização, escrita em disco, rede e leitura; é o maior custo típico. Reduzo o número de shuffles, filtro/projeto antes e
   cuido de skew.
2. *Transformação × ação?* Transformação é lazy (plano); ação dispara a execução. Isso permite ao Catalyst otimizar o
   plano inteiro.
3. *Estreita × larga?* Estreita: uma partição de entrada → uma de saída (sem shuffle). Larga: depende de várias
   partições (groupBy, join, window) → shuffle e novo stage.
4. *`repartition` × `coalesce`?* `repartition` faz shuffle (aumenta/redistribui, por chave); `coalesce` só reduz juntando
   partições, sem shuffle.
5. *Quantas partições de shuffle?* Parto de ~100–200 MB por partição e ajusto por spill e duração das tasks; AQE ajuda
   a juntar as pequenas. Aqui 96 spillava no sort e 192 resolveu.
6. *O que é AQE?* Replanejamento em runtime com estatísticas reais: junta partições, divide skew, troca estratégia de
   join. Não conserta stage pesado anterior ao primeiro shuffle.
7. *Como tratar skew?* Identifico por quantis de duração/shuffle por task; AQE skew join, *salting* da chave, broadcast
   do lado pequeno, ou filtrar chaves quentes.
8. *Broadcast join?* Replica a tabela pequena em todos os executors; evita shuffle do lado grande (limite padrão 10 MB).
9. *O que é spill?* Despejo em disco quando sort/aggregate não cabem na memória de execução. É sintoma de partições
   grandes demais; resolve com mais partições/menos memória por task, não necessariamente mais memória.
10. *Como investiga um job lento?* Stage mais longo → quantis das métricas por task (CPU × GC × spill × espera) → plano SQL
    → reproduzo local → profiling → mudo uma coisa e meço.
11. *Por que evitar UDF Python?* Catalyst não otimiza, serializa linha a linha JVM↔Python. Prefiro funções nativas.
12. *Cache/persist?* Reaproveitar resultado caro usado em várias ações; escolher nível de armazenamento; sempre `unpersist`.
13. *Por que Parquet/zstd?* Colunar (lê só colunas usadas, pushdown), compressão por coluna; zstd comprime melhor que snappy.
14. *Particionamento de tabela × de memória?* Pastas no storage (pruning) × pedaços de dados nas tasks. São coisas distintas.
15. *Problema de arquivos pequenos?* Cada task grava um arquivo ⇒ muitos arquivos pequenos ⇒ leitura lenta e metadados
    pesados. Resolvo com optimized writes e OPTIMIZE seletivo.

**Delta / lakehouse**
16. *Como o Delta dá ACID em S3?* Log de transações com commits atômicos (put-if-absent) e controle otimista de
    concorrência; leitores usam snapshots.
17. *Delta × Parquet?* Delta adiciona transação, MERGE, time travel, schema enforcement, CDF.
18. *OPTIMIZE e VACUUM?* OPTIMIZE compacta; VACUUM apaga arquivos sem referência após a retenção (≥ 7 dias); VACUUM
    limita o time travel.
19. *Delta × Iceberg?* Mesma ideia (camada de metadados transacional); estruturas diferentes (log vs. snapshots/manifests);
    escolhi Delta pelo alinhamento Databricks e MERGE maduro.
20. *Medalhão: por que três camadas?* Bronze preserva o original (reprocessável), silver entrega dado confiável, gold
    entrega modelos de consumo. Isola mudanças de regra.

**SCD2 e modelagem**
21. *O que é SCD2 e quando usar?* Histórico por versões com validade; quando é preciso saber "como era" em uma data.
22. *Como detecta mudança?* `xxhash64` das colunas rastreadas, comparado só entre versões da mesma chave.
23. *Como evita corromper o histórico?* MERGE atômico, meses em ordem (BLOCK), rollback + reaplicação, watermark.
24. *E se a chave sumir da fonte?* `WHEN NOT MATCHED BY SOURCE` fecha a versão com `closed_reason = ABSENT`.
25. *Por que contatos são tipo 1?* Mudam muito, são dado pessoal, e o histórico não agrega valor ao negócio.
26. *Chave natural × surrogate?* Natural: estável e significativa aqui; surrogate seria útil em fatos com muitas FKs. Em
    sócios, chave composta validada com dados.

**Pipeline e operação**
27. *Como garante idempotência?* Partição substituída no bronze, rollback+reaplicação no silver, gold reconstruído,
    estado no `ctrl`.
28. *Qual a diferença entre BLOCK e WARN?* BLOCK quebra o modelo (para tudo); WARN registra e segue, com alerta.
29. *Como sabe que um mês foi baixado?* `ctrl.download_ref` / `layer_watermark` (Delta), não o Airflow.
30. *Como você observa o pipeline?* Quatro sinais ligados por `trace_id = md5(run_id)`; dashboards como código.
31. *O que mudaria em produção?* Seção 11.
32. *Qual foi o maior erro/aprendizado?* Achar que o spill era a causa (era CPU de regex); e o conflito Spark 4.1.3 ×
    Delta 4.1.0 ⇒ fixar versões.

---

## 13. Pitch pronto

**30 segundos.** *"Construí um lakehouse de ponta a ponta com a base de CNPJ da Receita: 73 milhões de estabelecimentos
por mês, em PySpark e Delta Lake, arquitetura medalhão, orquestrado no Airflow. A parte central é um histórico SCD2
que guarda só as mudanças entre meses, com MERGE atômico, idempotência e ordem de meses protegida. Tudo com
observabilidade completa — logs, métricas, traces e auditoria — e qualidade com política de BLOCK e WARN."*

**2 minutos.** Acrescente: o problema (retratos mensais, ninguém diz o que mudou), as decisões (tudo string no bronze,
tipos e dedup no silver, Kimball no gold, SCD2 com hashes), os números reais (3 h 33 min na primeira execução, 0 BLOCK) e o
tuning (diagnóstico pelo History Server, CPU de regex, 73 → 34 µs/linha, window única, arquivos do tamanho certo).
Termine com o que faria em produção (seção 11).

---

## 14. Cola de números e fatos

| Fato | Valor |
|---|---|
| Arquivos por mês / contrato | 37 ZIPs (~7,3 GB em landing); bronze ~5,5 GB em Delta/zstd |
| Linhas (2026-09) | estabelecimentos 73,4 M · empresas 70,1 M · simples 50,4 M · sócios 28,3 M |
| Primeira execução completa | 3 h 33 min · 23/23 tarefas · 0 BLOCK · 3 WARN |
| Spark / Delta | 4.1.0 / 4.1.0 (fixados, ADR 0002) |
| Cluster | 1 worker, 6 cores, executor 8 GB, driver 4 GB |
| Partições de shuffle (silver) | 192 (era 96) |
| Tipagem de estabelecimentos | 73 → 34 µs/linha CPU |
| Dedup (local, 13,2 M linhas) | 338 → 182 s |
| Arquivos Delta alvo | ~256 MiB (optimized writes) |
| Manutenção | 69 → ~10–15 min (projeção) · silver 53 → ~38 min (projeção) |
| VACUUM | retenção ≥ 168 h |
| Testes | 59 |
| Retenção do bronze | 3 meses |
| Silver | histórico completo (só mudanças) |
| `change_rate` anômalo | > 20 % das chaves num mês |

---

## 15. Glossário rápido

- **Lakehouse:** data lake com garantias de warehouse (transações, schema) via formato de tabela aberto.
- **Catalyst / Tungsten:** otimizador de planos / motor de execução com geração de código (*whole-stage codegen*).
- **Stage / Task:** unidade entre shuffles / unidade por partição.
- **Skew:** distribuição desigual de dados entre partições.
- **Watermark (aqui):** registro em `ctrl` do último mês processado por camada (não é o *watermark* de streaming).
- **CDF:** Change Data Feed, log de mudanças linha a linha de uma tabela Delta.
- **Quarentena:** destino auditável das linhas rejeitadas.
- **Idempotência:** executar de novo produz o mesmo estado final.
- **ADR:** *Architecture Decision Record*, registro de uma decisão com contexto e consequências.

---

## 16. Plano de estudo (ordem sugerida)

1. **Spark em prática (2–3 dias).** Abra o Spark History (`localhost:18080`, via túnel) numa execução antiga e localize:
   o estágio mais longo, os quantis de uma task, o plano SQL. Refaça a análise da seção 3.9 com suas palavras.
2. **Reproduza o benchmark.** Em Spark local, gere dados sintéticos, rode `old_dedup` × `dedup` e veja o shuffle/spill
   mudar ao variar `spark.sql.shuffle.partitions` (96/192/288).
3. **Delta (1–2 dias).** Em um notebook: `DESCRIBE HISTORY`, `VERSION AS OF`, `OPTIMIZE`, `VACUUM DRY RUN`, `MERGE`
   com e sem `WHEN NOT MATCHED BY SOURCE`. Leia o `_delta_log/` de uma tabela.
4. **SCD2 (1 dia).** Execute dois meses sintéticos em `tests/spark/test_scd2.py` e desenhe no papel as versões
   resultantes; explique o *staged union* sem olhar o código.
5. **Airflow e observabilidade (1 dia).** Percorra uma execução no Grid, no Grafana e no Tempo e conte a história da
   execução pelos quatro sinais.
6. **Certificação (opcional).** Mapa para o Databricks Data Engineer Associate: Delta (MERGE, time travel, OPTIMIZE,
   VACUUM), medalhão, ingestão incremental, qualidade, orquestração, governança.
7. **Ensaie em voz alta** o pitch (seção 13) e três perguntas da seção 12 por dia.

---

## 17. Runbook rápido que você deve saber explicar (operação)

- **Ordem de meses.** O silver só aceita meses em sequência a partir do primeiro carregado. Para começar em um mês
  anterior ao já carregado, limpa-se o silver (tabelas `*_hist`, arquivos no S3 e watermarks `silver.*` das 4 entidades) e
  carrega-se **do mais antigo ao mais novo**.
- **Reprocessar o último mês:** disparar de novo com o mesmo `ref_month` (rollback + reaplicação).
- **Ligar a stack sob demanda:** `sudo systemctl start seaweedfs spark-master spark-worker spark-history loki alloy tempo`.
- **Nunca** mexer em arquivos do HDD de 1 TB do dono fora de `lambda-dump/`.
