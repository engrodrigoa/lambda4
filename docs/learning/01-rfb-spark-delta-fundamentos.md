# Relatório de aprendizado 01 — Base do projeto RFB com Spark e Delta

Material de estudo (pt-BR). Para cada conceito: **o que é**, **quando usar**, **quando NÃO usar** e **onde aparece no projeto**.
Não é preciso decorar nada: a ideia é entender o raciocínio por trás de cada decisão.

---

## 1. Table formats: Parquet puro, Delta e Iceberg

**O que é.** Parquet é só um formato de arquivo colunar. Delta e Iceberg são camadas em cima de Parquet que
adicionam um **log de transações**: ACID, `MERGE/UPDATE/DELETE`, time travel, evolução de schema.

| | Parquet puro | Delta Lake | Iceberg |
|---|---|---|---|
| Upsert/MERGE | não | sim | sim |
| Time travel | não | sim (log) | sim (snapshots) |
| Forte em | arquivos frios, trocas simples | Spark e Databricks | multi-motor (Snowflake, AWS, Trino...) |
| Catálogo | nenhum | metastore ou Unity Catalog | catálogo é parte central do desenho |

**Quando usar Delta:** estudo e trabalho com Databricks; muito Spark; pipelines com `MERGE`, `OPTIMIZE`, CDF.
**Quando usar Iceberg:** vários motores lendo as mesmas tabelas; ecossistema neutro de fornecedor.
**Quando NÃO usar nenhum dos dois (Parquet puro):** dado que nunca será atualizado e é lido por uma ferramenta só.
**Ideia-chave de mercado:** os dois estão convergindo; o ponto de dependência real passou a ser o **catálogo**.
**No projeto:** ADR 0001. Delta para tudo; Iceberg continua instalado para comparar depois.

## 2. Arquitetura medalhão (bronze, silver, gold)

- **Bronze:** o dado como chegou, em string, só acrescentando, com colunas de auditoria. Serve para reprocessar sem baixar de novo.
- **Silver:** tipado, limpo, com chave natural única. É aqui que mora o SCD2.
- **Gold:** modelado para consumo (dimensões, fatos, tabela de mudanças).

**Quando usar:** quando a origem é suja ou instável e você quer separar "o que recebi" de "o que acredito".
**Quando NÃO usar:** dado pequeno e limpo, consumido uma vez. Três camadas viram só custo.
**Regra de ouro:** a bronze nunca é "consertada". Conserta-se na silver, e o erro fica visível.

## 3. Landing local ou direto no S3?

O RFB entrega ZIPs. O Spark **não lê ZIP em paralelo**, então é preciso extrair antes de qualquer jeito.
- **Landing local (`LANDING_URI`):** ZIPs são arquivo imutável e frio. Ficam no HDD de 1 TB, barato e grande.
- **S3:** entra a partir da bronze, onde a leitura e a escrita paralelas importam.
- A variável aceita `file://` e `s3://`, então mudar de ideia não muda código.
**Quando guardar o bruto no S3:** na nuvem, onde o S3 é barato e é o lugar natural.
**Quando NÃO:** aqui, com 192 GB de lake num HDD USB lento, seria desperdiçar o recurso mais escasso.

## 4. Spark numa máquina só: onde está o gargalo

- Um master e um worker com 6 cores e 12 GB. Dá para aprender a API, o Spark UI, shuffle e partições.
- **O gargalo real aqui é o disco**, não a CPU. Por isso o scratch (shuffle e arquivos temporários) foi para o SSD.
- Shuffle é o grande custo do Spark: dados trocados entre executores via disco e rede. Quanto menos shuffle, melhor.
**Quando Spark compensa:** volume grande (centenas de milhões de linhas ou mais), vários nós, ecossistema Databricks.
**Quando NÃO compensa:** 60 a 70 milhões de linhas em uma máquina. DuckDB ou Polars costumam ser mais rápidos.
Aqui usamos Spark **para aprender**, e isso é uma escolha legítima, só vale ter ciência disso.

## 5. Chaves: natural, substituta e hash

- **Chave natural:** já existe no dado e é única (CNPJ = `cnpj_basico + ordem + dv`). **Prefira quando existir.**
- **Chave substituta (surrogate):** inventada (número ou hash) quando não há chave natural limpa. Útil em sócios.
- **Hash de linha (`row_hash`):** resumo dos atributos acompanhados, só para **detectar mudança**.

| Função | Tamanho | Velocidade | Quando usar |
|---|---|---|---|
| `xxhash64` (Spark) | 64 bits | muito rápida | detectar mudança entre versões da mesma chave |
| `md5` | 128 bits | média | integração com sistemas que já usam md5; checksum não-seguro |
| `sha2(…,256)` | 256 bits | mais lenta | quando precisa de colisão praticamente impossível ou valor de verificação |

**Por que xxhash64 basta no SCD2:** comparamos o hash novo só com o hash anterior **da mesma chave**. A chance de
colisão nesse par é da ordem de 1 em 2^64. O risco de "aniversário" só aparece quando se exige unicidade entre
**todas** as linhas ao mesmo tempo; não é o caso.
**Quando NÃO usar hash como chave de junção:** se uma colisão puder ligar duas pessoas diferentes. Aí use a chave
natural ou um hash maior.
**Cuidado:** ao concatenar campos para o hash, use um separador e trate nulos, para `"a|bc"` e `"ab|c"` não darem o mesmo valor.
Nenhum desses é para segurança (senhas): para isso existem algoritmos próprios, como bcrypt e argon2.

## 6. SCD2 (histórico de mudanças)

**O que é.** Em vez de sobrescrever um registro quando algo muda, fecha-se a versão antiga (`valid_to`) e abre-se uma nova
(`valid_from`, `is_current`). Dá para dizer "este CNPJ mudou o atributo X entre os meses Y e Z".
**Quando usar:** quando a pergunta "como era antes?" tem valor (cadastro, endereço, situação, classificação).
**Quando NÃO usar:** atributo que muda sem significado de negócio (ex.: data de carga), ou tabela sem consulta histórica.
Também não vale em campo que já carrega o próprio histórico: a RFB já traz `data_situacao_cadastral`. O SCD2 rende
mais nos campos **sem data**: endereço, razão social, CNAE, porte, capital social.
**Como funciona o MERGE:** compara o `row_hash` do mês novo com o da versão vigente. Mudou, fecha e insere. Igual, não faz nada.
**Idempotência:** reprocessar um mês não pode duplicar. Meses entram **em ordem**; para desfazer, usa-se o rollback do Delta.

## 7. CSV sujo: ler sem perder nada em silêncio

Problemas reais da RFB: encoding ISO-8859-1, `;` solto dentro de texto, aspas quebradas, linhas em branco, colunas a mais ou a menos.
- Leitura em modo **PERMISSIVE**: o Spark guarda a linha bruta de quem falhou em vez de derrubar o job.
- Regra de reparo (a do datalabs): junta o excesso na última coluna, completa o que falta.
- O que não repara vai para **quarentena**, com arquivo e motivo. Nada some sem rastro.
- **Número de colunas esperado por entidade**: com um número único para todas, uma linha de `empresas` pode desalinhar em silêncio.
**Quando usar quarentena:** sempre que o dado vem de fora e você não controla a qualidade.
**Quando NÃO:** dado gerado por você, com schema controlado (aí um erro de parse é bug, e deve parar).

## 8. Observabilidade e confiabilidade (a premissa dos laboratórios)

Quatro coisas diferentes, que é comum misturar:
1. **Logs** (o que aconteceu, em JSON, uma linha por evento) -> Loki, e espelho no schema `audit` do Postgres para tabelas no Grafana.
2. **Métricas** (números ao longo do tempo: linhas, duração) -> Prometheus. Job em batch usa Pushgateway.
3. **Auditoria** (quem rodou o quê, quando, com que resultado): schema `audit`.
4. **Controle** (de onde retomar): tabelas Delta no bucket `ctrl`. **Só** referência de download e ponto de retomada.

**Falha BLOCK x WARN.** Parar o pipeline só quando seguir adiante geraria um modelo errado (arquivo ausente, bronze vazia,
mês fora de ordem, chave duplicada). O resto segue, e vira log, métrica e alerta. Parar por tudo cria pipeline que ninguém roda;
nunca parar cria pipeline que mente.
**Quando NÃO apertar o health check:** em laboratório e em dado de origem instável. Em produção crítica, o limite sobe.

## 9. Metastore Hive e a lição do deadlock

O Delta guarda os dados e o log no S3, mas **os nomes das tabelas** precisam de um catálogo. Usamos o metastore Hive embutido,
guardando seus metadados no Postgres.
**Lição que aconteceu de verdade:** na primeira execução, com a criação automática de tabelas do metastore ligada, o Spark
travou. Duas conexões da mesma JVM disputavam o mesmo lock no Postgres (uma com um `INSERT` aberto, outra tentando um
`ALTER TABLE` na mesma tabela). A saída padrão é **criar o schema uma vez, de forma explícita**, com o script oficial do
Hive, e desligar a criação automática. Diagnóstico: `jstack` no driver mais `pg_stat_activity` no Postgres.
**Regra geral:** criação automática de schema é cômoda em dev e arriscada em tudo que roda sozinho. Prefira migração explícita.

**Delta em S3 e um escritor por tabela.** Sem um coordenador de commits, dois jobs gravando a mesma tabela ao mesmo tempo podem
se sobrescrever. Aqui cada tabela tem um escritor (uma DAG). Na AWS usa-se um log store com DynamoDB; na Databricks, o Unity Catalog.

## 10. Segurança operacional (o que fizemos e por quê)

- **Segredos fora do git:** só um template com `${VARIÁVEIS}` vai ao repositório; os valores ficam em arquivo com permissão 600.
- **Mínimo privilégio:** o Grafana lê o schema `audit` com um usuário só de leitura (`grafana_ro`).
- **Verificar o que se baixa:** os jars do Maven foram conferidos pelo sha1 oficial; o SQL do Hive foi inspecionado antes de rodar.
- **Backup antes de editar configuração:** `*.bak-AAAAMMDD` ao lado de cada arquivo alterado.
- **Disco externo:** alimentado só pelo USB falhou ao ser movido. Sinal clássico de falta de energia. Plano: monitorar e ter cópia do que importa.

## 11. Comandos Delta para conhecer (e para a prova)

| Comando | Para quê | Quando NÃO usar |
|---|---|---|
| `DESCRIBE HISTORY` | ver as versões da tabela | — |
| `VERSION AS OF` / `TIMESTAMP AS OF` | ler o passado (time travel) | como substituto de backup |
| `MERGE INTO` | upsert, SCD2 | carga que só acrescenta (use `INSERT`/append) |
| `OPTIMIZE` | juntar arquivos pequenos | tabela já compacta; é caro |
| `VACUUM` | apagar arquivos antigos | antes da retenção desejada: **apaga o time travel** |
| `RESTORE` | voltar a uma versão | quando outro processo já consumiu a versão errada |
| Change Data Feed | ler só o que mudou | tabela que ninguém lê de forma incremental |

---

### Estado do ambiente ao fim desta etapa
Delta 4.1.0 + metastore Hive no Postgres + S3A funcionando no cluster Spark, validado com um teste de ponta a ponta
(`infra/smoke/smoke_delta.py`: criar, inserir, `MERGE`, histórico, time travel, e leitura pelo DuckDB). Scratch no SSD.
Schema `audit` criado no Postgres; bucket `ctrl` criado no S3.
Próximos passos: desenhar as tabelas `ctrl` e `audit`, escrever `pipelines/commons`, portar o download do mês 2026-09.
