-- As linhas da silver com fl_preco_suspeito (poucas): o Grafana lista daqui, sem varrer a silver de 11 GB.
select
    data_coleta, mes_coleta, produto, uf, municipio, cnpj, revenda, bandeira,
    valor_venda, preco_mediana_ref,
    round(valor_venda / nullif(preco_mediana_ref, 0), 2)  as razao_mediana,
    unidade_medida, _source_kind, _file_key, _record_no, _ingested_at,
    current_timestamp                                    as calculado_em
from {{ ref('anp__precos_coleta') }}
where fl_preco_suspeito
