{{ config(
            materialized='table',
            tags=['transform', 'investidores']
          ) 
}}

with first_operation as (
        select 
            codigo_investidor,
            data_operacao,
            ROW_NUMBER() OVER(PARTITION BY codigo_investidor ORDER BY data_operacao) AS first_operaations
        from {{  ref('stg_operacoes') }}  
),

base as (

    select
        
        b.codigo_investidor,
        case when data_adesao = '1900-01-01 00:00:00.000' 
            then fo.data_operacao 
            else data_adesao 
            end as data_adesao,
        estado_civil,
        genero,
        profissao,
        idade,
        uf_investidor,
        cidade_investidor,
        pais_investidor,
        Situacao_conta,
        operou_12_meses,
        
        ROW_NUMBER() OVER(PARTITION BY b.codigo_investidor ORDER BY  data_adesao DESC) AS rn

    from {{ ref('stg_investidores') }} as b
    left join first_operation as fo on b.codigo_investidor = fo.codigo_investidor and fo.first_operaations = 1

),


ajustado as (

    select distinct
        b.codigo_investidor,
        
        data_adesao,

        {{ tempo_conta("data_adesao", "anos") }} as tempo_conta_anos,
        {{ tempo_conta("data_adesao", "meses") }} as tempo_conta_meses,
        {{ tempo_conta("data_adesao", "dias") }} as tempo_conta_dias,
        
        estado_civil,
        g.genero_text as genero,
        lower(profissao) as profissao,
        idade,
        u.estado,
        lower(cidade_investidor) as cidade_investidor,
        lower(pais_investidor) as pais_investidor,
        case when Situacao_conta = 'A' then 'ativa'
            when Situacao_conta = 'D' then 'inativa'
            else 'outro' end as Situacao_conta ,
        case when operou_12_meses = 'S' then 'sim'
            when operou_12_meses = 'N' then 'não'
            else 'outro' end as operou_12_meses

    from base as b
    inner join {{ ref('genero') }} as g on b.genero = g.genero_id
    inner join {{ ref('uf') }} as u on b.uf_investidor = u.uf_investidor_id
   
    where b.rn = 1

),
final as (
    select * from ajustado
)

select * from final

