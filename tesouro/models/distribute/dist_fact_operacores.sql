{{ config(
            materialized='table',
            tags=['dist', 'fact']
          ) 
}}

with base as (
    select
        codigo_investidor,
        data_operacao,
        canal_operacao,
        tipo_operacao,
        tipo_titulo,
        titulo,
        valor_titulo,
        quantidade,
        valor_operacao
    from {{ ref('trf_operacoes') }} as o
    
),

fact_operacoes as (

    select
        md5(concat(b.codigo_investidor, data_operacao, b.titulo, quantidade)) as operacao_id,
        codigo_investidor,
        data_operacao,
        dc.id_canal,
        dto.id_tipo_operacao,
        dt.id_titulo,
        dtt.id_tipo_titulo, 
        valor_titulo,
        quantidade,
        valor_operacao
    from base as b
    inner join {{ ref('dist_dim_canal') }} as dc on b.canal_operacao = dc.canal_operacao
    inner join {{ ref('dist_dim_tipo_operacao') }} as dto on b.tipo_operacao = dto.tipo_operacao
    inner join {{ ref('dist_dim_titulo') }} as dt on  b.titulo = dt.titulo
    inner join {{ ref('dist_dim_tipo_titulo') }} as dtt on b.tipo_titulo = dtt.tipo_titulo
    inner join {{ ref('dist_dim_investidores') }} as di on b.codigo_investidor = di.id_investidor
) 

select * from fact_operacoes