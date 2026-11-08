-- Município x UF observado nas coletas. Chave: md5(uf|municipio) (a ANP não publica o código IBGE).
select
    md5(uf || '|' || municipio)                          as municipio_sk,
    uf,
    municipio,
    min(regiao)                                          as regiao,
    min(data_coleta)                                     as primeira_coleta,
    max(data_coleta)                                     as ultima_coleta,
    count(distinct cnpj)                                 as qt_postos_historico
from {{ ref('anp__precos_coleta') }}
group by uf, municipio
