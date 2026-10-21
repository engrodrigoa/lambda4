# Relatório de aprendizado 05 — Tracing distribuído: OpenTelemetry + Tempo no pipeline RFB

Material de estudo e de defesa do projeto. Continua o relatório 04 (observabilidade da fase 1). A referência técnica
está em `docs/architecture.md` (seção "Traces") e a decisão no ADR 0004 (fase 2).

> **Estado nesta etapa (2026-10-21).** Instalado e verificado: Tempo 3.1 (sob demanda), rota de traces no Alloy,
> datasource no Grafana, SDK do OpenTelemetry no `env-data`, spans no `TaskRun`, traces do Airflow ligados.
> Verificado com uma execução manual do `rfb00_preflight`: o trace `md5(run_id)` aparece no Tempo com 12 spans
> (execução → tarefa → 10 sondas), idêntico à árvore que a view SQL mostra. **Os traces do Airflow só aparecem quando
> a DAG roda** — isso fica para a sua primeira execução.

---

## 1. Por que traces, se já temos logs, métricas e audit?

Cada sinal responde a uma pergunta diferente:

| Sinal | Pergunta | Exemplo aqui |
|---|---|---|
| Log | *o que aconteceu?* | `file_downloaded: Estabelecimentos0.zip 512 MiB em 41 s` |
| Métrica | *quanto / com que frequência, ao longo do tempo?* | `lambda_task_duration_seconds` |
| Audit | *quem rodou o quê, com qual resultado?* (consultável por SQL) | `audit.task_run` |
| **Trace** | ***onde o tempo foi gasto, e em que ordem / dentro de quê?*** | execução → tarefa → arquivo |

Um trace é uma **árvore de spans**. Um span é "um trabalho com início, fim, nome e atributos", e aponta para o pai.
A pergunta que só o trace responde bem: "a execução levou 3 h; qual tarefa, e dentro dela qual etapa, comeu o tempo?"
— em um único gráfico de cascata (waterfall).

Na fase 1 nós já desenhávamos essa árvore a partir das tabelas de audit (`audit.v_trace_spans`). A fase 2 troca o
"trace reconstruído por SQL" por **spans reais do padrão OpenTelemetry**, guardados no **Grafana Tempo** — o mesmo
caminho usado em produção em empresas.

## 2. As peças

```
TaskRun / log_event / finalize_run  ──OTLP/HTTP :4318──┐
(OpenTelemetry SDK, pipelines/commons/tracing.py)       ├──> Alloy ──OTLP/gRPC :4417──> Tempo ──> Grafana
Airflow 3 (dag_run, task_run, worker.<task>) ──────────┘                                 └─ span metrics ──> Prometheus
```

- **OpenTelemetry (OTel)**: padrão aberto para gerar sinais (traces, métricas, logs). O **SDK** cria os spans dentro
  do processo; o **exporter OTLP** os envia pela rede.
- **OTLP**: o protocolo do OTel. Tem versão HTTP (porta padrão 4318) e gRPC (4317).
- **Alloy**: o coletor. É a **porta única** de entrada (4318). Vantagem: quem gera os dados não precisa saber para
  onde eles vão; trocar Tempo por outro backend é mudar só o Alloy.
- **Tempo**: o banco de traces. Guarda os spans e responde buscas por id e por **TraceQL**
  (ex.: `{resource.service.name="lambda-rfb" && span.layer="bronze"}`).
- **metrics-generator** do Tempo: transforma spans em métricas RED (Rate, Errors, Duration) no Prometheus —
  `traces_spanmetrics_calls_total`, `traces_spanmetrics_latency_bucket` com `span_name`, `pipeline`, `layer`.

Detalhe de portas: o Tempo recebe em **4417**, não em 4317, porque 4317 já é do Phoenix (stack de IA) e porque a
porta "pública" de OTLP é a do Alloy. O Tempo só é alcançável através do coletor.

## 3. A decisão central: o id do trace vem do `run_id`

Em sistemas web, o contexto do trace (o `traceparent`) viaja no cabeçalho HTTP de serviço para serviço. No nosso
pipeline cada tarefa é **um processo separado**, iniciado por workers do Airflow diferentes, às vezes com retries
horas depois. Passar o contexto de um para o outro seria frágil.

Solução: **todo processo calcula o mesmo id a partir do `run_id`**, que todos já conhecem:

| Span | id |
|---|---|
| trace (a execução inteira) | `md5(run_id)` — 128 bits, exatamente o tamanho de um trace id |
| raiz `rfb.run` | `md5(run_id + "/root")[:16]` |
| tarefa (uma tentativa) | `md5(run_id/task_id/attempt)[:16]` |
| etapas | aleatório (filhos da tarefa) |

Consequências boas:
1. todas as tarefas da execução, em processos diferentes, caem **no mesmo trace** sem trocar nada entre si;
2. são **os mesmos ids** que a view `audit.v_trace_spans` calcula em SQL: dado um `run_id` no audit, o trace no Tempo
   é `SELECT md5(run_id)`. Audit, logs e traces se juntam por uma chave só;
3. o span de cada tarefa já existe "conceitualmente" antes de o pai ser escrito: a raiz é emitida no fim (pelo
   `finalize_run`), e o Tempo monta a árvore quando ela chega.

Como forçar um id no SDK: o `TracerProvider` aceita um `IdGenerator`. O nosso (`_DeterministicIds`) devolve ids
aleatórios, exceto quando uma `ContextVar` diz "o próximo span tem estes ids" — é o que `start_task_span` e
`emit_run_span` fazem. (O próprio Airflow usa a mesma técnica internamente.)

## 4. O que vira span

| Origem | Span | Exemplo |
|---|---|---|
| `TaskRun` (entrada → saída) | `<task_id>[<entity>]` | `rfb03_bronze[estabelecimentos]` |
| `log_event(..., seconds=...)` ou `latency_ms=...` | filho da tarefa, terminado "agora", começando `seconds` atrás | `health_check:s3`, `extract_done:socios` |
| `task.check(...)` | **evento** no span da tarefa (não é span: não tem duração) | `dq_check status=FAIL` |
| exceção | `record_exception` + status ERROR | stack trace no span |
| `finalize_run` | raiz `rfb.run` com início = `pipeline_run.started_at` | status da execução |

Atributos do span da tarefa: `pipeline, task_id, layer, entity, ref_month, run_id, attempt, airflow_task_id`; no fim,
`status`, `exit_code`, contadores (`rows_in/out/rejected`, bytes, arquivos), `warn_count`, `block_count`,
`spark_app_id`. Com isso, TraceQL responde perguntas como "tarefas da silver acima de 10 min":
`{span.layer="silver" && duration > 10m}`.

O truque do "span retroativo": as etapas já medem a própria duração e registram `seconds` no log. Em vez de mexer em
cada script, o `log_event` cria o span **depois** do fato, com `start_time = agora - seconds`. Toda etapa cronometrada
de hoje e do futuro vira span sem código novo. Para blocos novos, há também `with tracing.span("nome", ...)`.

O nome do span de etapa leva um qualificador de **baixa cardinalidade** (`component`, `target`, `table`, `entity`),
nunca o nome do arquivo: nome de span vira label nas span metrics, e label com valores demais estoura o Prometheus.

## 5. E os spans do próprio Airflow?

Com `AIRFLOW__TRACES__OTEL_ON=True`, o Airflow 3 emite `dag_run.<dag>` → `task_run.<task>` → `worker.<task>`.
Mas o id do trace dele é **aleatório por DAG run** e não pode ser escolhido pela DAG (Airflow 3.3). Então são **dois
traces**:

- o **nosso**: o que o código fez (bytes, linhas, etapas, checks);
- o **do Airflow**: o que o orquestrador fez (fila, agendamento, worker).

Eles se conectam por **span link**: o `TracedBashOperator` da DAG injeta o contexto atual (o span `worker.<task>`)
na variável de ambiente `TRACEPARENT` (padrão W3C); o `tracing.py` lê e adiciona um link no span da tarefa. No Grafana,
o detalhe do span mostra o link e abre o trace do Airflow. Um link é a relação OTel para "relacionado, mas não
pai/filho" — o caso típico de lotes e filas.

## 6. Logs ↔ traces

- Cada linha JSON do pipeline ganha `trace_id` e `span_id` (o do span em que foi escrita; a linha de uma etapa aponta
  para o span da etapa).
- O Alloy copia os dois para o **structured metadata** do Loki: metadado por linha, pesquisável, mas **não é label**
  (label com um valor por execução seria cardinalidade explosiva). Consulta: `{job="lambda-pipelines"} | trace_id="…"`.
- Grafana: no Loki, um botão "Trace in Tempo" em cada linha com `trace_id` (derived field); no Tempo, "Logs for this
  span" busca as linhas do trace na janela de tempo do span.

## 7. Confiabilidade: tracing nunca derruba o pipeline

Mesma regra do audit e das métricas — observabilidade é *best-effort*:

| Situação | Comportamento |
|---|---|
| `LAMBDA_OTEL_ENABLED=0` | no-op total (os testes rodam assim) |
| SDK não instalado | no-op, uma linha de info |
| nada escutando em `:4318` ao iniciar (Alloy parado) | no-op, **um aviso**, sem esperar timeouts |
| Tempo parado, Alloy no ar | Alloy guarda numa fila em memória e tenta por até 10 min, depois descarta |
| exportação lenta | spans saem em lote em background; no fim, `flush` com teto de 5 s |

O preflight agora sonda `alloy` e `tempo` (WARN): a execução segue, mas fica registrado que ela pode não ter trace.

## 8. Como usar

```bash
sudo systemctl start alloy tempo          # junto com o resto da stack sob demanda
python -m pipelines.rfb.rfb00_preflight   # qualquer tarefa manual gera um trace completo
```

- Grafana → *RFB · Pipeline health & tracing* → escolha a execução → "Trace of the run (Tempo)".
- Explore → Tempo → TraceQL: `{resource.service.name="lambda-rfb"}`, `{span.task_id="rfb04_silver" && status=error}`.
- Vários scripts manuais como **uma** execução: `export LAMBDA_RUN_ID=manual__…`, rode os scripts e feche com
  `python -m pipelines.commons.finalize_run --pipeline rfb` (escreve a raiz do trace).
- Id do trace de qualquer execução: `SELECT run_id, md5(run_id) FROM audit.pipeline_run ORDER BY started_at DESC;`

## 9. Perguntas de entrevista que este trabalho responde

- **Trace vs. log vs. métrica?** Seção 1.
- **Como correlacionar sinais?** Uma chave comum (`trace_id = md5(run_id)`) em audit, logs, spans e na view SQL.
- **Como propagar contexto entre processos batch?** Ids determinísticos em vez de propagação; propagação W3C
  (`TRACEPARENT`) só para o link com o orquestrador.
- **Por que um coletor no meio?** Desacopla produtores e backend, centraliza lote/retry/fila, uma porta só.
- **Cardinalidade?** `run_id`/`trace_id` nunca como label; nome de span com qualificador de baixa cardinalidade;
  dimensões das span metrics limitadas a `pipeline` e `layer`.
- **E se o backend de observabilidade cair?** Seção 7: degrada, avisa, nunca bloqueia.
