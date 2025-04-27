{{ config(
            materialized='table',
            tags=['dist', 'dim']
          ) 
}}

with base as (
    select distinct
        canal_operacao
    from {{ ref('trf_operacoes') }}
),

dim_canal as (
    select 
        md5(canal_operacao) AS id_canal,
        canal_operacao
    from base
)

select * from dim_canal