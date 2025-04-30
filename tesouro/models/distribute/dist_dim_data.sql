{{ config(
    materialized='table',
    tags=['dist', 'dim']
) }}

with operacoes as (
    select distinct
        TO_DATE(data_operacao, 'DD/MM/YYYY') as data_operacao
    from {{ ref('trf_operacoes') }}
),

dim_date as (
    select
        data_operacao,
        extract(year from data_operacao) as ano,
        extract(month from data_operacao) as mes,
        extract(day from data_operacao) as dia,
        extract(week from data_operacao) as semana,
        extract(quarter from data_operacao) as trimestre,

        -- Dia da semana (0=Domingo, 6=Sábado)
        extract(dow from data_operacao) as dia_da_semana,

        -- Nome do dia da semana
        case extract(dow from data_operacao)
            when 0 then 'Domingo'
            when 1 then 'Segunda'
            when 2 then 'Terça'
            when 3 then 'Quarta'
            when 4 then 'Quinta'
            when 5 then 'Sexta'
            when 6 then 'Sábado'
        end as nome_dia_da_semana

    from operacoes
)

select * from dim_date
