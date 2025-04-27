
{{ config(
        materialized='table',
        tags=['staging', 'investidores']
        ) 
}}

WITH raw AS (
    SELECT 
        "Codigo do Investidor" as codigo_investidor,
        "Data de Adesao" as data_adesao,
        "Estado Civil" as estado_civil,
        "Genero" as genero,
        "Profissao" as profissao,
        "Idade" as idade,
        "UF do Investidor" as uf_investidor,
        "Cidade do Investidor" as cidade_investidor,
        "Pais do Investidor" as pais_investidor,
        "Situacao da Conta" as Situacao_conta,
        "Operou 12 Meses" as operou_12_meses
    FROM read_csv_auto('../curso/data/raw/investidores.csv')
),
typed as (
    select 
        codigo_investidor,
        data_adesao,
        estado_civil,
        genero,
        profissao,
        idade,
        uf_investidor,
        cidade_investidor,
        pais_investidor,
        Situacao_conta,
        operou_12_meses
    from raw
), final as (
    select * from typed
)
select * from final
