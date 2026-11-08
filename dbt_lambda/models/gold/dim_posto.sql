{{ config(
    materialized = 'table',
    indexes = [{'columns': ['posto_sk'], 'unique': True}, {'columns': ['cnpj', 'valid_from']}],
    post_hook = ["analyze {{ this }}"]
) }}

-- Posto (estabelecimento, CNPJ de 14 dígitos) em SCD2 derivado da LINHA DO TEMPO DAS COLETAS: uma versão a cada
-- mudança de razão social, bandeira ou endereço, com validade [valid_from, valid_to). Derivado do dado (não de um
-- dbt snapshot), porque os 20 anos de história chegam de uma vez: um snapshot só registraria mudanças daqui em diante.
--
-- Regras medidas em 2026-11-08 (ver docs/architecture-anp.md):
--  * atributos vêm das linhas de COMBUSTÍVEL quando o CNPJ as tem: no GLP a "bandeira" é a distribuidora do gás,
--    não a marca do posto. CNPJs só de GLP (revendas de gás) usam as próprias linhas (tipo_revenda = revenda_glp).
--  * trecho de bandeira BRANCA espremido entre dois trechos da MESMA marca = omissão da fonte (a ANP publicou BRANCA
--    no lugar da bandeira em vários meses; ex.: ago-nov/2005 a fatia BRANCA saltou de 28% para 60%) e herda a marca.
--    Sem isso, 22.829 trechos falsos viravam versões.
--  * dentro do mesmo dia os produtos do posto concordam nos atributos (medido: 0 divergências em 8,25 M dias-posto).

with linhas as (
    select cnpj, data_coleta, product_group,
           revenda, bandeira, cep, logradouro, numero, complemento, bairro, municipio, uf, regiao
    from {{ ref('anp__precos_coleta') }}
),

combustivel as (
    select distinct cnpj from linhas where product_group <> 'glp'
),

dia as (
    -- um conjunto de atributos por posto x dia (determinístico)
    select distinct on (l.cnpj, l.data_coleta)
           l.cnpj, l.data_coleta, (c.cnpj is null) as fl_revenda_glp,
           l.revenda, l.bandeira, l.cep, l.logradouro, l.numero, l.complemento, l.bairro,
           l.municipio, l.uf, l.regiao
    from linhas l
    left join combustivel c on c.cnpj = l.cnpj
    where l.product_group <> 'glp' or c.cnpj is null
    order by l.cnpj, l.data_coleta, l.bandeira, l.revenda, l.cep, l.logradouro, l.numero, l.complemento, l.bairro
),

trecho_bandeira as (
    select *, sum(muda) over (partition by cnpj order by data_coleta) as trecho
    from (
        select *, (bandeira is distinct from lag(bandeira) over (partition by cnpj order by data_coleta))::int as muda
        from dia
    ) d
),

trechos as (
    select cnpj, trecho, min(bandeira) as bandeira
    from trecho_bandeira
    group by 1, 2
),

suavizado as (
    select cnpj, trecho,
           case when bandeira = 'BRANCA'
                 and lag(bandeira)  over w = lead(bandeira) over w
                 and lag(bandeira)  over w <> 'BRANCA'
                then lag(bandeira) over w
                else bandeira end                               as bandeira_vigente
    from trechos
    window w as (partition by cnpj order by trecho)
),

atributos as (
    select t.*, s.bandeira_vigente,
           md5(concat_ws('|', t.revenda, s.bandeira_vigente, t.cep, t.logradouro, t.numero, t.complemento,
                         t.bairro, t.municipio, t.uf))         as hash_atributos
    from trecho_bandeira t
    join suavizado s using (cnpj, trecho)
),

versionado as (
    select *, sum(muda_versao) over (partition by cnpj order by data_coleta) as versao
    from (
        select *, (hash_atributos is distinct from lag(hash_atributos) over (partition by cnpj order by data_coleta))::int
                  as muda_versao
        from atributos
    ) a
),

versoes as (
    -- atributos idênticos dentro da versão (mesmo hash): min() só escolhe o valor
    select cnpj, versao,
           min(data_coleta)     as inicio,
           max(data_coleta)     as fim,
           count(*)             as dias_com_coleta,
           bool_or(fl_revenda_glp) as fl_revenda_glp,
           min(revenda) as revenda, min(bandeira_vigente) as bandeira, min(cep) as cep,
           min(logradouro) as logradouro, min(numero) as numero, min(complemento) as complemento,
           min(bairro) as bairro, min(municipio) as municipio, min(uf) as uf, min(regiao) as regiao
    from versionado
    group by cnpj, versao
),

periodo as (
    select cnpj, min(data_coleta) as primeira_coleta, max(data_coleta) as ultima_coleta
    from linhas
    group by cnpj
)

select
    md5(v.cnpj || '|' || v.inicio::text)                                     as posto_sk,
    v.cnpj,
    v.versao,
    -- a 1ª versão cobre desde a 1ª coleta de QUALQUER produto (ex.: GLP antes do 1º combustível)
    case when v.versao = 1 then least(v.inicio, p.primeira_coleta) else v.inicio end as valid_from,
    coalesce(lead(v.inicio) over (partition by v.cnpj order by v.versao), date '9999-12-31') as valid_to,
    lead(v.inicio) over (partition by v.cnpj order by v.versao) is null      as fl_atual,
    case when v.fl_revenda_glp then 'revenda_glp' else 'posto' end           as tipo_revenda,
    v.revenda,
    v.bandeira,
    v.cep,
    coalesce(v.logradouro, 'NAO INFORMADO')                                  as logradouro,
    coalesce(v.numero, 'S/N')                                                as numero,
    coalesce(v.complemento, 'NAO INFORMADO')                                 as complemento,
    coalesce(v.bairro, 'NAO INFORMADO')                                      as bairro,
    v.municipio,
    v.uf,
    v.regiao,
    md5(v.uf || '|' || v.municipio)                                          as municipio_sk,
    concat_ws(', ', v.logradouro, coalesce(v.numero, 'S/N'))                 as endereco,
    v.inicio                                                                 as primeira_coleta_versao,
    v.fim                                                                    as ultima_coleta_versao,
    v.dias_com_coleta,
    p.primeira_coleta,
    p.ultima_coleta
from versoes v
join periodo p using (cnpj)
