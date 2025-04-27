{{ config(
            materialized='table',
            tags=['dist', 'dim']
          ) 
}}

with investidores as (

    select 
        codigo_investidor,
        cidade_investidor,
        estado as uf_investidor,
        pais_investidor
    from {{ ref('trf_investidores') }}
),
 
 dim_endereco as (
    select 
        codigo_investidor,
        cidade_investidor,
        uf_investidor,
        pais_investidor
    from investidores
 )

select * from dim_endereco