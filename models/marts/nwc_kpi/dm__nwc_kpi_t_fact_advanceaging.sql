
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','advanceaging']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_fact_advanceaging') }}
