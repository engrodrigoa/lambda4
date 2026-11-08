{{ config(severity='warn') }}
-- linhas 100% vazias no bronze (deveriam ir para a quarentena no anp03)
select _file_key, count(*) as linhas_vazias
from {{ source('anp_bronze', 'precos_raw') }}
where num_nonnulls(regiao_sigla, estado_sigla, municipio, revenda, cnpj_da_revenda,
                   nome_da_rua, numero_rua, complemento, bairro, cep, produto,
                   data_da_coleta, valor_de_venda, valor_de_compra,
                   unidade_de_medida, bandeira) = 0
group by 1