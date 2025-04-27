{{ config(
            materialized='table',
            tags=['transform', 'operacoes']
          ) 
}}


with base as (

    select
        codigo_investidor,
        data_operacao,
        tipo_titulo,
        vencimento_titulo,
        quantidade,
        valor_titulo,
        valor_operacao,
        tipo_operacao,
        canal_operacao

    from {{ ref('stg_operacoes') }}

),

ajustado as (

    select
        codigo_investidor,
        data_operacao,
        vencimento_titulo,
        lower(trim(tipo_titulo)) as tipo_titulo,
        concat(lower(trim(tipo_titulo)),'|', vencimento_titulo ) as titulo,
        cast(REPLACE(quantidade, ',', '.') as double) as quantidade,
        CAST(REPLACE(REPLACE(valor_titulo, '.', ''), ',', '.') AS DOUBLE) AS valor_titulo,
        CAST(REPLACE(REPLACE(valor_operacao, '.', ''), ',', '.') AS DOUBLE) AS valor_operacao,
        case when tipo_operacao = 'C' then 'compra'
            when tipo_operacao = 'V' then 'venda'
            else 'outro' end  as tipo_operacao,
        c.canal_operacao_text as canal_operacao

    from base as b
    inner join {{ ref('canal') }} as c on b.canal_operacao = c.canal_operacao_id

),
final as (
    select * from ajustado
)

select * from final
