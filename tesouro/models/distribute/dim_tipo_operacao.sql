{{ config(
            materialized='table',
            tags=['dist', 'dim']
          ) 
}}

with base as (
    select distinct
        tipo_operacao
    from {{ ref('trf_operacoes') }}
),

dim_tipo_operacao as (
    select 
        md5(tipo_operacao) AS id_tipo_operacao,
        tipo_operacao
    from base
)

select * from dim_tipo_operacao