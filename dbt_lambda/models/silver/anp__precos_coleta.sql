{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['mes_coleta', 'product_group'],
    on_schema_change = 'append_new_columns',
    post_hook = [
        "create index if not exists ix_anp_precos_coleta_mes_grupo on {{ this }} (mes_coleta, product_group)",
        "analyze {{ this }}"
    ]
) }}

-- Silver ANP: uma linha por coleta publicada distinta, com linha temporal sem sobreposição de fontes.
-- Regra: por (mes_coleta, product_group), vence a fonte de menor source_priority presente
--        (mensal 1 > semanal 2 > semestral 3); dentro dela, duplicatas exatas (row_hash) viram 1 linha.

with stg as (
    select *,
           date_trunc('month', data_coleta)::date as mes_coleta
    from {{ ref('stg_anp__precos') }}
    where data_coleta is not null
),

{% if is_incremental() %}
afetados as (
    -- (mês × grupo) tocados por arquivos novos desde a última carga: reconstruídos INTEIROS
    select distinct mes_coleta, product_group
    from stg
    where _ingested_at > (select max(_ingested_at) from {{ this }})
),
{% endif %}

escopo as (
    select * from stg
    {% if is_incremental() %}
    where (mes_coleta, product_group) in (select mes_coleta, product_group from afetados)
    {% endif %}
),

fonte_mes as (
    select mes_coleta, product_group, min(source_priority) as source_priority
    from escopo
    group by 1, 2
),

selecionado as (
    select e.*
    from escopo e
    join fonte_mes f using (mes_coleta, product_group, source_priority)
),

dedup as (
    select *,
           count(*)     over (partition by row_hash)                                   as qt_publicacoes,
           row_number() over (partition by row_hash
                              order by _ingested_at desc, _file_key desc, _record_no)  as rn
    from selecionado
)

select
    row_hash,
    mes_coleta,
    product_group,
    cnpj, produto, data_coleta,
    valor_venda, valor_compra, unidade_medida,
    revenda, bandeira, regiao, uf, municipio, bairro, logradouro, numero, complemento, cep,
    count(*) over (partition by cnpj, produto, data_coleta) > 1 as fl_conflito_preco,
    qt_publicacoes,
    source_priority, _source_kind, file_group, _file_key, _record_no, _run_id, _ingested_at
from dedup
where rn = 1