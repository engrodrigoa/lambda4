-- Observabilidade da silver ANP: por (mes_coleta, product_group), o que estava disponível,
-- qual fonte venceu, quanto foi descartado (fonte perdedora / duplicata exata) e sinais de anomalia.

with stg as (
    select date_trunc('month', data_coleta)::date as mes_coleta,
           product_group, _source_kind
    from {{ ref('stg_anp__precos') }}
    where data_coleta is not null
),

disponivel as (
    select mes_coleta, product_group,
           count(*)                                         as linhas_stg,
           count(*) filter (where _source_kind = 'mensal')    as linhas_mensal,
           count(*) filter (where _source_kind = 'semanal')   as linhas_semanal,
           count(*) filter (where _source_kind = 'semestral') as linhas_semestral
    from stg
    group by 1, 2
),

silver as (
    select mes_coleta, product_group,
           min(_source_kind)                             as fonte_escolhida,   -- 1 fonte por mês (testado)
           count(*)                                      as linhas_silver,
           sum(qt_publicacoes)                           as linhas_fonte_escolhida,
           count(*) filter (where fl_conflito_preco)     as linhas_conflito,
           count(*) filter (where fl_preco_suspeito)     as linhas_preco_suspeito,
           count(distinct data_coleta)                   as dias_com_coleta,
           max(data_coleta)                              as ultima_coleta,
           max(_ingested_at)                             as ultima_ingestao
    from {{ ref('anp__precos_coleta') }}
    group by 1, 2
)

select
    d.mes_coleta,
    d.product_group,
    concat_ws(',',
        case when d.linhas_mensal    > 0 then 'mensal'    end,
        case when d.linhas_semanal   > 0 then 'semanal'   end,
        case when d.linhas_semestral > 0 then 'semestral' end)   as fontes_disponiveis,
    s.fonte_escolhida,

    -- funil: stg → fonte escolhida → silver
    d.linhas_stg, d.linhas_mensal, d.linhas_semanal, d.linhas_semestral,
    s.linhas_fonte_escolhida,
    d.linhas_stg - s.linhas_fonte_escolhida                      as descartadas_fonte,
    s.linhas_fonte_escolhida - s.linhas_silver                   as descartadas_duplicata,
    s.linhas_silver,

    -- sinais
    s.linhas_conflito,
    s.linhas_preco_suspeito,
    s.dias_com_coleta,
    s.ultima_coleta,
    s.ultima_ingestao,
    s.fonte_escolhida <> 'mensal'
      and d.mes_coleta >= '{{ var("anp_mensal_inicio") }}'::date
      and d.mes_coleta <  (date_trunc('month', current_date) - interval '1 month')::date
                                                                 as fl_mes_fechado_sem_mensal,
    current_timestamp                                            as calculado_em
from disponivel d
left join silver s using (mes_coleta, product_group)