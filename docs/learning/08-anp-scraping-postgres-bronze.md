# Relatório de aprendizado 08 — ANP: scraping, controle idempotente e bronze no Postgres com Polars

Segundo projeto do lab. Decisões no ADR 0006; referência técnica em `docs/architecture-anp.md`.

> **Estado (2026-10-22).** Código até a bronze pronto e testado (unitários + integração no Postgres real dentro de uma
> transação desfeita no fim). Nada foi executado contra o portal nem gravado no banco: o DDL, a primeira execução e
> a DAG ficam com você. Silver/gold são o seu projeto dbt.

---

## 1. Antes de desenhar: medir a fonte

O datalabs (modo dev) baixa só as 4 últimas semanas e os meses do ano corrente. Para o modo full, olhamos o portal
inteiro antes de escrever código:

| Pergunta | Como respondemos | Resposta |
|---|---|---|
| Quantos arquivos e quanto pesa? | links da página + GET de 1 byte (`Range: bytes=0-0`) em cada um | 224 arquivos, ~5,4 GB, ~40 M linhas |
| O layout muda com o tempo? | hash da 1ª linha de cada CSV | **um só layout** (16 colunas), com e sem BOM |
| "Vem tudo em CSV"? | magic bytes | não: semestres 2022-2 a 2026-1 em ZIP, e `ca-2014-01.csv` é um ZIP com nome `.csv` |
| Linhas quebradas? | contagem de `;` por linha | ~1 % com `;` a mais, **mas dentro de campo entre aspas** (CSV válido) |
| Os nomes são previsíveis? | lista de URLs | não: `precos-glp-12.csv`, `09-dados-abertos-precos-2026-09-glp.csv`, erro de digitação `02-cados-...` |
| O servidor aguenta rajadas? | o próprio teste | não: `Connection reset by peer` depois de muitos requests paralelos |

Lição: **cada fato medido virou uma regra no código** (`pipelines/anp/contract.py` lista todos). Sem essa medição,
o pipeline teria quebrado no ZIP disfarçado e teria perdido dois semestres de GLP com nome fora do padrão. Quem
achou esses dois foi o próprio check `discover.unclassified_links`, na primeira vez que o parser rodou sobre a página
real.

## 2. Arquitetura: por que landing local e não SeaweedFS

Quem lê o arquivo é um processo Polars local, e quem recebe é o Postgres. O S3 seria mais um serviço sob demanda
ligado só para guardar arquivo, sem ninguém lendo via S3. O HDD de 1 TB vira o **arquivo permanente dos brutos**:

```
/mnt/hdd1tb/lambda-dump/landing/anp/semestral/ca/2014-S1__3f9a1c0d2b7e.zip
                                    └ kind   └ produto └ label  └ sha256[:12]
```

O hash no nome torna o arquivo **imutável**. Se a ANP republicar um semestre corrigido, a versão nova fica ao lado da
antiga: dá para refazer a bronze de qualquer versão. Isso importa porque o arquivo semanal é sobrescrito na origem
toda semana; se não guardarmos, ele some.

## 3. Idempotência: identidade do arquivo e controle na mesma transação

**Identidade.** Não usamos o nome do arquivo da ANP (irregular). Cada arquivo ganha um `file_key` estável:
`semestral/ca/2014-S1`, `mensal/glp/2026-09`, `semanal/glp/2026-10-20`. O semanal tem sempre a mesma URL, então a
identidade dele é a data "atualizado em" da página.

**Versão.** `sha256` do conteúdo. Conteúdo igual → nada a fazer. Conteúdo diferente → versão +1 e bronze pendente.
Conteúdo igual ao de **outro** arquivo (um semanal que a ANP não atualizou) → marcado como duplicado, não carrega duas
vezes.

**Mudança sem baixar.** O `discover` pede 1 byte de cada arquivo recente (`Range`) e lê o tamanho total no
`Content-Range` (o HEAD do gov.br não traz tamanho). Tamanho diferente do baixado → volta a PENDING. Semestres antigos
não mudam: só são reconferidos com `--full-recheck`, para não gastar requests à toa.

**Execução desnecessária.** Se não há nada para baixar nem para carregar, o `discover` sai com **99**, e a DAG pula o
resto. A execução diária sem novidade dura segundos.

**Controle na mesma transação (o ponto principal).** No RFB, o `ctrl` é Delta, e a consistência vem da ordem das
escritas. Aqui a bronze e o `ctrl` moram no mesmo Postgres, então a carga de um arquivo é **uma transação**:

```
BEGIN
  DELETE FROM anp_bronze.precos_raw        WHERE _file_key = 'semestral/ca/2014-S1'   -- versão anterior
  DELETE FROM anp_bronze.precos_quarantine WHERE _file_key = 'semestral/ca/2014-S1'
  COPY anp_bronze.precos_raw ...           -- linhas novas
  COPY anp_bronze.precos_quarantine ...    -- linhas recusadas
  check bronze.rows_match_source           -- BLOCK aqui dentro → ROLLBACK de tudo
  UPSERT ctrl.source_file  (LOADED)  +  INSERT ctrl.source_file_event (LOADED)
COMMIT
```

Ou o arquivo está carregado **e** marcado, ou nenhum dos dois. Rodar de novo nunca duplica linhas.

Detalhe de psycopg que o teste de integração pegou: com `autocommit=False` (o padrão), um `with conn.transaction()`
dentro de uma transação implícita vira um **savepoint**, que só é efetivado quando a conexão fecha. Por isso as
conexões de dados (`pipelines/commons/pg.py`) usam `autocommit=True`: aí cada `transaction()` é uma transação de
verdade. O mesmo teste pegou outro bug: um `NULL` explícito num `INSERT` **não** aciona o `DEFAULT now()` da coluna.

## 4. Leitura: por que duas passadas

O Polars é rápido (~520 mil registros/s aqui), mas tem um comportamento perigoso:

```python
pl.read_csv(b"a;b;c\n1;2;3\n4;5\n")       # -> [('1','2','3'), ('4','5',None)]   linha curta vira NULL em silêncio
pl.read_csv(b"a;b;c\n1;2;3;4\n")          # -> ComputeError                       linha longa quebra o arquivo todo
```

Então `reader.py` faz:

1. **Varredura com o `csv` do Python** (em C, rápido): decodifica (UTF-8 → cp1252 → UTF-8 com substituição), conta
   campos por registro e anota os registros com contagem errada.
2. Arquivo limpo em UTF-8 → **Polars** lê (caminho rápido). Senão → o frame é montado com os registros bons, e os
   ruins vão para `anp_bronze.precos_quarantine` com o motivo (`field_count_4`), nunca descartados em silêncio.

Bronze = **texto exatamente como veio** (o CNPJ com espaço na frente, `2,899` com vírgula, `02/01/2014`). Converter é
trabalho da silver, no seu dbt.

## 5. Severidade: o que para e o que avisa

| BLOCK (a task falha) | WARN (segue, registra, alerta) |
|---|---|
| página sem nenhum arquivo reconhecido | link que o contrato não conhece (o site mudou) |
| cabeçalho sem as colunas do contrato | período faltando na série (hoje: ca 2022-S1 e mensal 2026-04) |
| arquivo sem linhas | arquivo republicado ou retirado da página |
| linhas gravadas + quarentena ≠ registros lidos | falha de download/carga (tenta de novo na próxima execução) |
| nenhum download funcionou | taxa de quarentena > 0,1 %, datas fora do período do arquivo, encoding alternativo |

Detalhe de desenho: um arquivo recusado **não impede os outros**. Ele fica FAILED no `ctrl`, os demais carregam, e o
BLOCK sobe no fim do lote. Você descobre o problema sem perder a carga do resto.

## 6. O que fazer agora (você)

1. Aplicar o DDL: `psql -d lambda -v ON_ERROR_STOP=1 -f infra/postgres/anp_schema.sql`.
2. Primeiro teste pequeno, à mão (runbook em `docs/architecture-anp.md` §7): semanal, 3 arquivos.
3. Olhar o resultado em `ctrl.source_file`, `ctrl.source_file_event`, `anp_bronze.precos_raw` e no Grafana.
4. Carga completa pela DAG `anp_pipeline` (pausada até você ligar).
5. Montar o projeto dbt do zero sobre `anp_bronze.precos_raw` (sugestão de modelo no §5 da referência).

## 7. Perguntas para fixar

1. Por que o semanal precisa da data da página para ter identidade, e o semestral não?
2. O que aconteceria se o `UPDATE` do `ctrl` fosse feito **depois** do `COMMIT` da bronze e o processo morresse entre
   os dois?
3. Por que o Polars sozinho não basta para garantir que nenhuma linha quebrada entre na bronze?
4. Por que guardamos todas as versões no landing em vez de sobrescrever?
5. Onde a sobreposição semestral × mensal × semanal deve ser resolvida, e com qual prioridade?
