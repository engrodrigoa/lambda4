# Relatório de aprendizado 06 — Tuning depois da 1ª execução real: CPU do silver e tamanho de arquivos Delta

Continua o relatório 05. Decisão registrada no ADR 0005; referência técnica em `docs/architecture.md`.

> **Estado (2026-10-21).** Mudanças aplicadas no código e medidas em Spark local com dados sintéticos. O efeito real
> aparece na próxima execução da DAG (que você dispara). Seção 6 diz o que conferir.

---

## 1. O método: medir antes de mexer

A execução de 2026-09 levou 3 h 33 min. A hipótese inicial era "o dedup do silver faz spill porque o AQE junta as
partições". Antes de mexer em configuração, abrimos os **event logs do Spark** (History Server, API REST
`/api/v1/applications/<app>/stages` e `/sql`) e o `audit.log_event`:

| O que olhamos | O que mostrou |
|---|---|
| Estágios do `estabelecimentos.silver` | 1 estágio de **25 min** (32 tasks × ~270 s), o resto pequeno |
| Métricas por task (quantis) | CPU 239 s de 271 s por task, GC < 2 % → **limitado por CPU**, não por memória/disco |
| Plano SQL desse estágio | `scan → Project (tipagem) → HashAggregate parcial` (o lado map do `dropDuplicates`) |
| Linhas | 73,4 M entram, 73,4 M saem do aggregate parcial: **redução zero** |
| Shuffle depois | 96 partições de ~160 MB, **sem** coalescing; o sort da window custou só 41 s |

Ou seja: o spill existia, mas era sintoma. A hipótese "AQE coalesce 96 → 32" não se confirmou (32 era o número de
*splits de leitura* do bronze, não de partições de shuffle).

Lição: **o Spark UI / History Server responde "onde está o tempo" com números**; configuração no escuro (mais
partições, mais memória) teria custado horas sem atacar a causa.

## 2. Reproduzir localmente com dados sintéticos

Para testar sem rodar a DAG real: um gerador cria 13,2 M linhas com o formato do bronze de estabelecimentos (mesmas 30
colunas texto, ~42 B/linha em parquet, contra 48 B no real; 0,3 % de chaves repetidas). Spark `local[6]`, 8 GB — a
mesma memória por core do executor do cluster. Partições escaladas para o mesmo volume por partição do real.

Primeira surpresa: **só a tipagem** (`transform`, sem dedup) custa **73 µs de CPU por linha**. Para 73,4 M linhas:
~1,5 h de CPU — a maior parte do estágio de 25 min.

### Profiling na JVM (Java Flight Recorder)

```bash
spark.driver.extraJavaOptions=-XX:StartFlightRecording=filename=t.jfr,settings=profile
jfr print --events jdk.ExecutionSample t.jfr     # amostras de pilha → contar os métodos mais frequentes
```

Resultado: ~30 % do tempo em `java.util.regex` (`Matcher.search`, `Pattern.matcher`…) e conversões
`UTF8String ↔ String`. O `clean_text` fazia **4 `regexp_replace` por valor** × 10 colunas de texto = 40 regex por
linha, cada uma convertendo a string do formato interno do Spark para `java.lang.String` e de volta.

Testes que **descartaram** hipóteses (também é resultado):
- desligar whole-stage codegen ou forçar `hugeMethodLimit` → pouco ganho;
- `-XX:-DontCompileHugeMethods` (método gerado grande demais para o JIT) → nenhum ganho.

## 3. As mudanças

### 3.1 Tipagem sem regex por passo (−53 % de CPU)

| Antes | Depois |
|---|---|
| `regexp_replace(\x1a → ?)` | `translate`: um passe, caractere a caractere: `\x1a`→`?`, CR/LF→espaço, demais controles apagados |
| `regexp_replace([controles] → '')` | (no mesmo `translate`) |
| `regexp_replace([\r\n]+ → ' ')` | (no mesmo `translate`; espaços duplos viram 1 no passo seguinte) |
| `regexp_replace(\s{2,} → ' ')` | a mesma regex, **só** quando o valor tem dois espaços ou TAB (`contains`, barato) |
| `code()`: `rlike ^[0-9]+$` sempre | `rlike` só quando o tamanho ≠ largura esperada (quase nunca) |

`translate(col, de, para)` troca cada caractere de `de` pelo da mesma posição em `para`; os que sobram em `de` (sem
par) são **apagados** — é isso que remove os caracteres de controle sem regex.

**Prova de equivalência:** saída comparada linha a linha (`exceptAll`) entre versão antiga e nova: 0 diferenças em
300 mil linhas sintéticas + 200 mil valores "adversariais" (CR/LF, TAB, controles, nulos, vazios, CNPJ alfanumérico).
Medido: **73,3 → 34,2 µs/linha**.

### 3.2 Dedup em uma única window

Antes: `dropDuplicates()` (hash aggregate sobre **todas** as colunas + shuffle) e depois `row_number()` por chave
(outro shuffle + sort). Em 2026-09 o `dropDuplicates` removeu 0 linhas de estabelecimentos (292 de 28,3 M em socios).

Depois: uma window por chave natural, ordenada por `dedup_order` e **depois por todas as outras colunas**:
- a ordem é total → o vencedor é determinístico (antes, um empate só em telefone era sorteio);
- linhas idênticas ficam adjacentes → `lag(struct(linha)) <=> struct(linha)` marca a duplicata exata;
- `_rn = 1` vence; outras linhas distintas da mesma chave são conflitos (quarentena), como antes.

Um shuffle + um sort, em vez de aggregate + dois shuffles. (`<=>` é a igualdade que trata NULL = NULL.)

Números na seção 5. Também subimos as partições de shuffle do silver de 96 para 192 (seção 5).

### 3.3 Arquivos do tamanho certo na escrita (optimized writes)

Cada task de escrita grava um arquivo. Com 96 partições de shuffle, cada tabela silver saía com 96 arquivos de
~55 MB; o gold `estabelecimento_atual` (particionado por `uf`) saiu com **2688** (96 tasks × 28 UFs).

O Delta tem **optimized writes**: antes de gravar, um shuffle agrupa os dados em "bins" de tamanho alvo, e cada bin vira
um arquivo. Ligado no silver e no gold (`DELTA_OPTIMIZED_WRITES` em `pipelines/commons/spark.py`):
- alvo **256 MiB** por arquivo (o que o Databricks escolhe sozinho para tabelas < 2,56 TB);
- `binSize` = 3 × alvo, porque o bin é medido em bytes de shuffle (linhas lz4), ~3× o parquet zstd no disco
  (15,6 GB de shuffle → 5,3 GB de `estabelecimento_hist` em 2026-09).

Custo: um shuffle a mais por escrita. Ganho: não precisa reescrever tudo depois.

### 3.4 OPTIMIZE só quando compensa

O que custava caro: os **padrões** do Delta são `minFileSize = maxFileSize = 1 GiB`. Todo arquivo < 1 GiB é
"pequeno", e cada bin de 1 GiB é reescrito por **uma única task** (um core). Ex.: bronze `simples` (239 MB) → 1 task de
242 s. A manutenção reescreveu ~20 GB recém-gravados.

Agora (`rfb06_maintenance --optimize auto`, padrão):
- OPTIMIZE só se a tabela tem **≥ 8 arquivos com média < 64 MiB** (regra pura `optimize_decision`, com teste);
- e o OPTIMIZE só reescreve arquivos < 64 MiB, em arquivos de até 256 MiB (4× mais bins em paralelo);
- cada tabela registra a decisão e o motivo no evento `table_maintained` (Loki / `audit.log_event`).

## 4. Higiene de logs e Airflow

- `spark.ui.showConsoleProgress=false` agora é padrão em `build_spark`: as barras `[Stage 3:> (12 + 6) / 96]` são
  linhas redesenhadas com `\r` — 63 linhas de ruído só no log da manutenção, também enviadas ao Loki.
- **Métricas OTel do Airflow**: no Airflow 3.2+ `[metrics] otel_host/otel_port/otel_interval_milliseconds/
  otel_ssl_active` estão obsoletas (um aviso em todo log de task). Migradas para as variáveis padrão do OpenTelemetry:
  `OTEL_METRICS_EXPORTER=otlp`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://127.0.0.1:4318/v1/metrics`,
  `OTEL_EXPORTER_OTLP_METRICS_PROTOCOL=http/protobuf`, `OTEL_METRIC_EXPORT_INTERVAL=30000`. `otel_on` e `otel_prefix`
  continuam (não obsoletos). Os nomes `airflow_*` e o `job="airflow"` não mudam: o job vem do `service.name`
  (`OTEL_SERVICE_NAME=airflow`). Conferido: todas as queries dos dashboards (`validate_dashboards.py`) OK.
- Airflow reiniciado (sem DAG run ativa) para aplicar o `OTEL_SERVICE_NAME`. Conferido emitindo um span pelo próprio
  bootstrap de tracing do Airflow com o mesmo env: chegou ao Tempo com `service.name=airflow`.

## 5. Números

Benchmark local (`local[6]`, 8 GB, 13,2 M linhas sintéticas; partições escaladas ao volume real). Cada variante
produz **exatamente os mesmos vencedores** (checksum `xxhash64` idêntico).

| Variante | Tempo (wall) | CPU total | Spill mem / disco |
|---|---|---|---|
| Antes: tipagem com 4 regex + `dropDuplicates` + window (96 partições no cluster) | 338 s | 1869 s | 7,3 / 2,6 GB |
| Tipagem nova + dedup antigo | 236 s | 1243 s | 7,3 / 2,6 GB |
| Tipagem nova + window única | 240 s | 1163 s | 9,8 / 3,3 GB |
| **+ 2× partições de shuffle (192 no cluster)** — adotado | **182 s** | **1024 s** | 1,0 / 0,35 GB |
| + 3× partições (288) | 179 s | 1025 s | 0 / 0 |

Leituras:
- **O modelo local é fiel**: 338 s × (73,4 M / 13,2 M) ≈ 1880 s, e o job real levou 1919 s.
- A tipagem é o maior ganho (−30 % de tempo sozinha). A window única economiza ~6 % de CPU e um shuffle, mas o
  sort sobre linhas largas fazia spill com 96 partições: dobrar as partições (~80 MB cada no real) tira quase todo o
  spill. 3× não ganha mais nada.
- Projeção para o real: ~1919 s → **~1010 s** (32 → ~17 min) neste trecho do `estabelecimentos.silver`.

Escrita do silver (1ª carga, MERGE de 13,16 M linhas, 192 partições escaladas):

| Variante | MERGE | Arquivos gerados | OPTIMIZE depois |
|---|---|---|---|
| Sem optimized writes | 169 s | 35 × 27 MB | regra dispara ("35 files averaging 26.6 MiB"): +76 s → 4 × 232 MB |
| **Com optimized writes** — adotado | **165 s** | **5 × 185 MB** | não precisa (média ≥ 64 MiB) |

O shuffle extra do optimized write saiu de graça (gravar 5 arquivos em vez de 35 compensa). O `binSize` é em MiB
(768 → 185 MB no sintético, que comprime ~4×; no real a razão é ~2,9× → ~260 MB por arquivo).

Projeção da manutenção: as 10 tabelas mais caras somaram 61 dos 69 min; silver, gold `estabelecimento_atual` e
`socio_atual` e o bronze (arquivos ~100 MB) deixam de fazer OPTIMIZE. Sobram VACUUM + estatísticas (~20 s por
tabela) e compactações pequenas (ctrl, bronze `socios`) → **~10–15 min**.

## 6. O que conferir na próxima execução

- Duração por task (Grafana *Pipeline health*, ou `audit.task_run`): esperado `estabelecimentos.silver` 53 → ~38 min,
  `maintenance` 69 → ~10–15 min.
- `audit.delta_table_stats`: silver e gold com dezenas de arquivos (não 96 / 2688) logo após a escrita.
- Loki `{job="lambda-pipelines"} |= "table_maintained"`: quais tabelas fizeram OPTIMIZE e por quê.
- Tempo, TraceQL `{resource.service.name="airflow"}`: a árvore `dag_run.rfb_pipeline` → `task_run.*` → `worker.*`.
- Logs das tasks no Airflow: sem barras de progresso e sem o aviso "OpenTelemetry configs have been deprecated".

## 7. Perguntas de entrevista que este trabalho responde

- **Como você investiga um job Spark lento?** Estágio mais longo → métricas por task (CPU vs. GC vs. spill vs.
  espera de shuffle) → plano SQL do estágio → reproduzir localmente → profiling (JFR) → mudar uma coisa e medir.
- **Spill é sempre o problema?** Não: aqui a CPU da tipagem dominava; o spill era consequência.
- **Por que regex é caro no Spark?** Cada chamada converte `UTF8String` ↔ `String` Java e roda o motor de regex;
  funções nativas por caractere (`translate`, `contains`, `length`) operam nos bytes direto.
- **Problema de arquivos pequenos?** Causa (um arquivo por task), efeito (leitura lenta, OPTIMIZE caro) e as duas
  alavancas: tamanho certo na escrita (optimized writes) e compactação seletiva (min/maxFileSize, limiar).
- **Como garantir que a otimização não mudou o resultado?** Comparação linha a linha (`exceptAll`) entre versões, com
  casos adversariais, e testes que fixam o comportamento (vencedor determinístico, duplicatas com NULL).
