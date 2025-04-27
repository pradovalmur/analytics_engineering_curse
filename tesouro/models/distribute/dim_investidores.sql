{{ config(
            materialized='table',
            tags=['dist', 'dim']
          ) 
}}

with investidores as (

    select 
        codigo_investidor,
        data_adesao,
        estado_civil,
        profissao,
        genero
    from {{ ref('trf_investidores') }}
),

dim_investidores as (
    select 
        codigo_investidor as id_investidor,
        data_adesao,
        estado_civil,
        profissao,
        genero
    from investidores 
)

select * from dim_investidores