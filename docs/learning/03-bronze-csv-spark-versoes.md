# Relatório de aprendizado 03 — Da landing à bronze: CSV de verdade, Spark multiLine e versões fixadas

Continuação dos relatórios 01 e 02. Mesmo formato: **o que é**, **quando usar**, **quando NÃO usar**, **onde está**.

---

## 1. Meça antes de corrigir: o perfil dos arquivos

Antes de escrever o parser, varremos os 37 ZIPs do mês 2026-09 (26,7 GB de CSV, ~222 milhões de registros) direto
dos ZIPs, em paralelo, com um leitor CSV que respeita aspas. Resultado:

| Achado | Quantidade | Causa real |
|---|---|---|
| Linhas com número errado de colunas | **0** | — |
| Registros com quebra de linha dentro de aspas | 7 (estabelecimentos) | CSV válido (RFC 4180) |
| Valores com `"` dentro | ~14 mil | aspas **dobradas** (`""D""`), padrão RFC |
| Caracteres de controle (`\x1a`, `\x00`) | 21 | acentos perdidos na origem |

**Lição:** as "muitas falhas" conhecidas da RFB vêm sobretudo de **ler o arquivo do jeito errado**: linha a linha, sem
respeitar aspas. Um registro com quebra de linha vira dois pedaços desalinhados, e aí parece que "faltam colunas". O
arquivo, neste mês, é RFC 4180 bem-formado.
**Quando medir assim:** sempre que for escrever regra de reparo. Regra sem medição corrige o que não está quebrado e
estraga o que está certo. A regra antiga de "juntar colunas excedentes na última" teria produzido dados deslocados.

## 2. Reparar x pôr em quarentena

Decidimos **não reparar na bronze**. O que o leitor correto não conseguir interpretar vai inteiro para a quarentena
(`audit.quarantine`, em Delta), com arquivo, motivo e o texto bruto.
**Quando reparar:** quando você conhece o defeito, ele se repete e o reparo é verificável. E mesmo assim, na silver.
**Quando NÃO reparar:** sempre que o reparo for um palpite (como "juntar o excesso na última coluna"). Palpite em escala
vira milhões de linhas erradas que parecem certas.

## 3. Por que `multiLine` exige dividir os arquivos

- `multiLine=true` é obrigatório para os 7 registros com quebra de linha. Mas um arquivo lido em `multiLine` vira **uma
  única tarefa** no Spark, porque ele não sabe onde cortar com segurança. O `Estabelecimentos0` (6,5 GB) seria lido por um
  só núcleo.
- Solução: o `rfb02_extract` corta cada arquivo em **pedaços de ~512 MB, sempre no fim de um registro**, e o Spark lê cada
  pedaço em paralelo. Dá para achar o fim do registro sem fazer o parse, porque aqui todo campo vem entre aspas: um
  registro completo tem número **par** de aspas.
- De quebra, o extract **conta** os registros. Essa contagem é a verdade contra a qual a bronze é conferida.

**Quando dividir:** em arquivos grandes, não divisíveis (multiLine, gzip, ZIP) e lidos por um motor paralelo.
**Quando NÃO:** em arquivos pequenos (os domínios), porque é complexidade sem ganho.

## 4. A paridade de aspas tem um ponto cego (o teste pegou)

Uma aspa solta (ímpar) seguida da primeira linha de um registro multilinha legítimo (também ímpar) soma **par**. O divisor
"fechava" um registro falso colando várias linhas, sem disparar o limite de segurança. O teste unitário com aspas soltas
**mais** um registro multilinha mostrou 21 registros em vez de 100.
**Correção:** todo candidato com mais de uma linha física (raríssimo) passa por um parse CSV de verdade e precisa ter
exatamente o número de colunas da entidade. Se falhar, a primeira linha vai para a quarentena e o resto é relido.
**Lição geral:** heurística barata no caminho comum, verificação cara só no caso raro. E teste os casos **combinados**,
não só cada defeito isolado.

## 5. Reconciliação: o check mais importante da bronze

`linhas gravadas + linhas em quarentena == registros contados no extract` é **BLOCK**. Se não fecha, algo se perdeu ou
duplicou em silêncio, e nenhum modelo construído em cima é confiável.
- O número de linhas gravadas vem das **métricas do commit do Delta** (`DESCRIBE HISTORY` → `numOutputRows`), sem uma
  segunda leitura dos dados.
- A segunda leitura, para achar as linhas malformadas, **só roda se a conta não fechar**. No mês normal ela não custa nada.

## 6. Bronze idempotente com `replaceWhere`

A bronze é particionada por `_ref_month` e gravada com `mode("overwrite").option("replaceWhere", "_ref_month = 202609")`.
Reprocessar o mês substitui **só aquela partição**, numa transação atômica, e o mês anterior fica intacto.
**Quando usar:** em cargas por período, quando o período inteiro chega de novo.
**Quando NÃO:** em cargas incrementais por chave (aí é `MERGE`, como no SCD2 da silver).

## 7. zstd e o tamanho dos arquivos

- A compressão zstd na bronze deu **5,3x** em `socios` (2,7 GB de CSV viraram 519 MiB). Em disco escasso, faz diferença.
- Cada pedaço de 512 MB de CSV vira um arquivo Parquet de ~50 MB. Arquivos de dezenas a centenas de MB são saudáveis.
  Milhares de arquivos pequenos são o problema clássico de lakehouse, resolvido com `OPTIMIZE`.

## 8. Retenção que respeita quem depende do dado

A bronze guarda 3 meses, mas **nunca apaga um mês que a silver ainda não aplicou** (watermark `silver.<entidade>` = DONE
no `ctrl`). Sem essa trava, um backfill de 12 meses perderia os meses antigos antes de a silver lê-los.
**Regra geral:** retenção é uma decisão sobre **quem ainda precisa do dado**, não só sobre a idade dele.

## 9. Versões fixadas: por que voltamos o Spark para 4.1.0 (ADR 0002)

O Delta usa classes **internas** do Spark. No Spark 4.1.3, uma delas não existe mais e um construtor mudou de assinatura.
O resultado foram erros só em tempo de execução (`NoClassDefFoundError`, `NoSuchMethodError`), em operações específicas:
- a API `DeltaTable` em Python;
- `DELETE` em tabela com dados.

Os `pom.xml` do Maven mostraram que **todas** as versões do Delta para Spark 4.1 foram compiladas contra o Spark 4.1.0.
**Lições:**
- "4.1.x é compatível" vale para a API pública, não para quem usa as internas. Bibliotecas como o Delta fixam a versão
  exata.
- Antes de atualizar, leia de que versão a dependência depende (o pom). Atualize **as duas juntas**, como a Databricks faz
  em cada runtime.
- Mantenha o caminho de volta: a 4.1.3 continua em disco, a um symlink de distância.
- Smoke test que só cobre SQL não pega problema de API. Teste os caminhos que o pipeline realmente usa.

## 10. Ruído conhecido x erro real

`ERROR HiveAlterHandler: Failed to alter table` aparece depois de gravar tabelas Delta, mas a linha seguinte explica que o
Spark gravou no formato próprio dele (tabelas Delta não são compatíveis com Hive). É ruído. **Registrar o ruído conhecido**
(no CLAUDE.md) evita que alguém perca uma tarde investigando, ou pior, que se acostume a ignorar ERRORs de verdade.
