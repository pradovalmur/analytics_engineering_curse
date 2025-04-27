{{ config(
            materialized='table',
            tags=['dist', 'dim']
          ) 
}}

with base as (
    select distinct
        tipo_titulo,
        vencimento_titulo
    from {{ ref('trf_operacoes') }}
),

dim_titulo as (
    select 
        md5(tipo_titulo) AS id_tipo_titulo,
        tipo_titulo
    from base
)

select * from dim_titulo