{{ config(tags=['gold']) }}
-- dim_posto SCD2: por CNPJ, versões sem sobreposição nem buraco, valid_from < valid_to e exatamente uma atual
with v as (
    select cnpj, posto_sk, valid_from, valid_to, fl_atual,
           lead(valid_from) over (partition by cnpj order by valid_from) as prox_inicio
    from {{ ref('dim_posto') }}
)
select cnpj, 'periodo_invalido' as problema from v where valid_from >= valid_to
union all
select cnpj, 'buraco_ou_sobreposicao' from v where prox_inicio is not null and prox_inicio <> valid_to
union all
select cnpj, 'atual_' || count(*) filter (where fl_atual) from v group by cnpj having count(*) filter (where fl_atual) <> 1
