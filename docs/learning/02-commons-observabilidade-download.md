# Relatório de aprendizado 02 — `commons`, controle, auditoria e o download do RFB

Continuação do relatório 01. Mesmo formato: **o que é**, **quando usar**, **quando NÃO usar**, **onde está no projeto**.

---

## 1. Uma "moldura" para toda task (`TaskRun`)

**O que é.** Todo script do pipeline roda dentro de `run_task(...)`. Ela cuida sozinha de:
- registrar o início e o fim (em `audit`);
- carimbar `run_id`, `task_id` e `ref_month` em todo log;
- enviar métricas;
- avaliar os checks de qualidade;
- traduzir o resultado em **código de saída**.

| Código | Significado | Exemplo |
|---|---|---|
| 0 | sucesso | mês baixado |
| 99 | nada a fazer (não é erro) | a Receita ainda não publicou o mês |
| 1 | falha | link rejeitado, arquivo faltando |

**Por que importa.** O orquestrador (Airflow) só enxerga o código de saída. Separar "pulei" de "falhei" evita alertas falsos
todo começo de mês, quando a Receita ainda não publicou.
**Quando usar:** em todo processo que roda sozinho.
**Quando NÃO usar:** em notebook exploratório. Lá a moldura atrapalha mais do que ajuda.
**No projeto:** `pipelines/commons/task.py`.

## 2. Controle, auditoria e logs: três coisas diferentes

| | Pergunta que responde | Onde | Exemplo |
|---|---|---|---|
| **Controle (`ctrl`)** | de onde eu continuo? | Delta, `s3a://ctrl/` | 202608 baixado, seguir para 202609 |
| **Auditoria (`audit`)** | como rodou? | Postgres `audit` | a task levou 19 min, 37 arquivos, 0 falhas |
| **Logs** | o que aconteceu, linha a linha? | Loki (todos) + `audit.log_event` (INFO+, 90 dias) | `Empresas0.zip: 537 MiB em 90 s` |

**Regra prática.** Se apagar a tabela faz o pipeline **perder o rumo**, é controle. Se apagar só faz você **perder a
visibilidade**, é auditoria ou log. Misturar os dois leva a dois erros comuns: um dashboard que depende de estado
operacional, ou um pipeline que quebra porque o banco de logs caiu.

## 3. Observabilidade nunca derruba o pipeline (spool)

**O que é.** Se o Postgres de `audit` estiver fora, os eventos vão para um arquivo local (`/srv/scratch/audit-spool/*.jsonl`) e
são reenviados **em ordem** na próxima vez que o banco responder. O horário gravado é o do evento, não o do reenvio.
**Quando usar:** sempre que a gravação for "nice to have" para o negócio (logs, métricas, auditoria).
**Quando NÃO usar:** para o **controle**. Se o `ctrl` não grava, o pipeline perde o ponto de retomada, então ali uma falha
deve parar a task.

## 4. Logs estruturados (JSON) e cardinalidade de labels

- Cada evento tem um **código estável** (`file_downloaded`, `source_auth_failed`) e uma mensagem para humanos. Dashboards
  e alertas usam o código, porque a mensagem pode mudar.
- No Loki, **labels** devem ter poucos valores (`pipeline`, `task`, `level`). Valores únicos por execução, como `run_id`
  e `ref_month`, ficam **no corpo** e são consultados com `| json`. Um label com milhões de valores degrada o Loki.
- Campos com cara de segredo (`token`, `password`, `secret`...) são mascarados (`***`) automaticamente.

## 5. Métricas de job em batch: Pushgateway e `pushadd`

O Prometheus **puxa** métricas de serviços que ficam no ar. Um job em batch nasce e morre antes da coleta, então ele **empurra**
o resultado para o Pushgateway, e o Prometheus coleta de lá.
- Usamos `pushadd` (POST): ele troca só as métricas enviadas. Por isso `lambda_task_last_success_timestamp_seconds`
  sobrevive a uma execução que falhou, e o alerta "sem sucesso há X dias" continua funcionando.
- `honor_labels: true` no Prometheus preserva os labels `pipeline` e `task` vindos do job.

**Quando NÃO usar o Pushgateway:** para serviços de longa duração (eles devem expor `/metrics`), ou para métricas por instância
que precisam sumir quando a instância morre.

## 6. Severidade centralizada (`dq.py`): BLOCK x WARN

Todo check precisa estar registrado em **um único dicionário**. Chamar um check que não está lá é erro de programação, e a task
falha. Assim, mudar a política ("isso agora bloqueia") é **uma linha revisada no git**, e não uma caça a `if` espalhados.
No download: `landing.contract_files_present` é BLOCK, porque a bronze precisa do mês inteiro. Espaço livre e variação de
tamanho contra o mês anterior são WARN.

## 7. Controle em Delta: Change Data Feed e `appendOnly`

- `ctrl.download_ref` guarda o **estado atual** (um registro por mês) e tem o **Change Data Feed** ligado. Toda mudança de
  status (DOWNLOADING → PARTIAL → DOWNLOADED) fica registrada e pode ser lida com `table_changes(...)`. É histórico sem
  tabela extra, e o CDF cai na prova da Databricks.
- `ctrl.download_file` é `delta.appendOnly = true`: o Delta **recusa** `UPDATE` e `DELETE`. É a trilha de auditoria dos
  downloads, que não pode ser reescrita.
- O mês vira `DOWNLOADING` **antes** de baixar. Se o processo morrer, o estado mostra que ficou pela metade, e a próxima
  execução retoma arquivo por arquivo.

**Quando usar CDF:** para alimentar processos incrementais ou para ter histórico de uma tabela de estado.
**Quando NÃO usar:** em tabelas enormes que ninguém lê de forma incremental, porque ele grava arquivos extras.

## 8. Download robusto contra uma fonte instável

Herdado do datalabs e mantido:
- **Contrato** dos 37 arquivos, em vez de listar a pasta (a listagem da Receita não informa nada).
- `404` também é tentado de novo: o servidor responde 404 e depois 200 para o mesmo arquivo.
- Grava em `.part` e só renomeia quando o tamanho bate e o ZIP abre. Um arquivo pela metade nunca passa por completo.
- O **SHA-256 é calculado durante o download**, sem reler 6 GB do disco.
- Mês completo no disco: a fonte **nem é contatada**.

## 9. Lição do dia: a extensão do Delta e os parâmetros do Spark SQL

`spark.sql("... WHERE pipeline = :p", args={"p": "rfb"})` é a forma segura de passar valores para SQL. Com a extensão SQL do
**Delta 4.1.0** no Spark 4.1, ela falha com `UNBOUND_SQL_PARAMETER`. Isolamos a causa testando cada extensão sozinha:
- sem extensão: funciona;
- só Iceberg: funciona;
- com Delta: falha.

A solução foi usar a **API de DataFrame** (`spark.table(...).where(F.col("pipeline") == "rfb")`), que é idiomática e não monta
SQL com valores.
**Lição geral:** testes com dublês (o `ctrl` em memória) não pegam incompatibilidade de plataforma. Sempre faça uma execução
real e curta antes da longa. Quando algo falha, isole uma variável por vez.

## 10. Testes que não dependem de serviços

Os 21 testes rodam em menos de 1 segundo, sem Postgres, Spark, Pushgateway nem internet:
- a fonte é simulada (404 intermitente, arquivo truncado, link rejeitado, mês não publicado);
- o `ctrl` é a versão em memória, com a **mesma interface** do Delta.

**Quando usar dublês:** para lógica (retomada, códigos de saída, severidade).
**Quando NÃO confiar só neles:** em integração com plataforma (veja a seção 9).
