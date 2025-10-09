
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','credits']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_v_fact_credits') }}
