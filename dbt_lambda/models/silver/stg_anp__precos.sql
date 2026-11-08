{{ config(materialized='view') }}

with src as (
    select * from {{ source('anp_bronze', 'precos_raw') }}
),

produto_grupo as (
    select * from {{ ref('anp_produto_grupo') }}
),

nao_vazias as (
    -- linhas 100% vazias (rodapé de CSV) saem aqui; perda medida em tests/assert_bronze_linhas_vazias.sql
    select * from src
    where num_nonnulls(regiao_sigla, estado_sigla, municipio, revenda, cnpj_da_revenda,
                       nome_da_rua, numero_rua, complemento, bairro, cep, produto,
                       data_da_coleta, valor_de_venda, valor_de_compra,
                       unidade_de_medida, bandeira) > 0
),

tipado as (
    select
        -- chave de negócio (mesmas regras do PROFILE_SQL do anp03)
        nullif(regexp_replace(cnpj_da_revenda, '\D', '', 'g'), '')          as cnpj,
        nullif(upper(trim(produto)), '')                                     as produto,
        case when data_da_coleta ~ '^\d{2}/\d{2}/\d{4}$'
             then to_date(data_da_coleta, 'DD/MM/YYYY') end                  as data_coleta,

        -- medidas
        case when trim(valor_de_venda) ~ '^[0-9]+([.,][0-9]+)?$'
             then replace(trim(valor_de_venda), ',', '.')::numeric(12,4) end as valor_venda,
        case when trim(valor_de_compra) ~ '^[0-9]+([.,][0-9]+)?$'
             then replace(trim(valor_de_compra), ',', '.')::numeric(12,4) end as valor_compra,
        nullif(trim(unidade_de_medida), '')                                  as unidade_medida,

        -- atributos do posto
        nullif(upper(trim(revenda)), '')                                     as revenda,
        nullif(upper(trim(bandeira)), '')                                    as bandeira,
        nullif(upper(trim(regiao_sigla)), '')                                as regiao,
        nullif(upper(trim(estado_sigla)), '')                                as uf,
        nullif(upper(trim(municipio)), '')                                   as municipio,
        nullif(upper(trim(bairro)), '')                                      as bairro,
        nullif(upper(trim(nome_da_rua)), '')                                 as logradouro,
        nullif(upper(trim(numero_rua)), '')                                  as numero,
        nullif(upper(trim(complemento)), '')                                 as complemento,
        nullif(regexp_replace(cep, '\D', '', 'g'), '')                       as cep,

        -- precedência entre fontes
        split_part(_file_key, '/', 2)                                        as file_group,
        case _source_kind when 'mensal'    then 1
                          when 'semanal'   then 2
                          when 'semestral' then 3 end                        as source_priority,

        -- identidade da coleta publicada (hash sobre as colunas BRUTAS)
        {{ dbt_utils.generate_surrogate_key([
            'cnpj_da_revenda', 'produto', 'data_da_coleta',
            'valor_de_venda', 'valor_de_compra', 'unidade_de_medida',
            'revenda', 'bandeira', 'estado_sigla', 'municipio',
            'bairro', 'nome_da_rua', 'numero_rua', 'complemento', 'cep'
        ]) }}                                                                as row_hash,

        -- linhagem (lineage)
        _source_kind, _file_key, _record_no, _run_id, _ingested_at
    from nao_vazias
)

select
    t.*,
    g.product_group           -- grupo pelo PRODUTO (semestral 'ca' ≠ mensal gasolina-etanol/diesel-gnv)
from tipado t
left join produto_grupo g on g.produto = t.produto