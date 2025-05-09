
{{ config(
        materialized='table',
        tags=['staging', 'operacoes']
        )
}}

WITH raw AS (
    SELECT 
        "Codigo do Investidor" as codigo_investidor,
        "Data da Operacao" as data_operacao,
        "Tipo Titulo" as tipo_titulo,
        "Vencimento do Titulo" as vencimento_titulo,
        "Quantidade" as quantidade,
        "Valor do Titulo" as valor_titulo,
        "Valor da Operacao" as valor_operacao,
        "Tipo da Operacao" as tipo_operacao,
        "Canal da Operacao" as canal_operacao
    FROM read_csv_auto('C:\Users\prado\projetos\analytics_engineering_curse\curso\data\raw\operacoes.csv')
),
typed as (
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
    from raw
),

final as (
    select * from typed
)
select * from final

