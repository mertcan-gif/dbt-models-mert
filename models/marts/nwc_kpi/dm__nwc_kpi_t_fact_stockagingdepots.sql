
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging','stockagingdepots']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_v_fact_stockagingdepotsraw') }}
