{{ config(
            materialized='table',
            tags=['dist', 'dim']
          ) 
}}

with operacoes as (

    select distinct
        data_operacao
    from {{ ref('trf_operacoes') }}
),

dim_date as (
        SELECT
            data_operacao,
            EXTRACT(YEAR FROM data_operacao) AS ano,              -- Ano
            EXTRACT(MONTH FROM data_operacao) AS mes,              -- Mês
            EXTRACT(DAY FROM data_operacao) AS dia,                -- Dia
            EXTRACT(WEEK FROM data_operacao) AS semana,            -- Semana
            EXTRACT(DAYOFWEEK FROM data_operacao) AS dia_da_semana, -- Dia da semana (1=Domingo, 7=Sábado)
            EXTRACT(QUARTER FROM data_operacao) AS trimestre,      -- Trimestre
            CASE 
                WHEN EXTRACT(DAYOFWEEK FROM data_operacao) = 1 THEN 'Domingo'
                WHEN EXTRACT(DAYOFWEEK FROM data_operacao) = 2 THEN 'Segunda'
                WHEN EXTRACT(DAYOFWEEK FROM data_operacao) = 3 THEN 'Terça'
                WHEN EXTRACT(DAYOFWEEK FROM data_operacao) = 4 THEN 'Quarta'
                WHEN EXTRACT(DAYOFWEEK FROM data_operacao) = 5 THEN 'Quinta'
                WHEN EXTRACT(DAYOFWEEK FROM data_operacao) = 6 THEN 'Sexta'
                WHEN EXTRACT(DAYOFWEEK FROM data_operacao) = 7 THEN 'Sábado'
            END AS nome_dia_da_semana  -- Nome do dia da semana
        FROM operacoes
)
select * from dim_date