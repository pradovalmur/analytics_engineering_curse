{{ config(
            materialized='table',
            tags=['dist', 'dim']
          ) 
}}

with base as (
    select distinct
        titulo,
        vencimento_titulo
    from {{ ref('trf_operacoes') }}
),

dim_titulo as (
    select 
        md5(concat(titulo, vencimento_titulo)) AS id_titulo,
        titulo,
        vencimento_titulo
    from base
)

select * from dim_titulo