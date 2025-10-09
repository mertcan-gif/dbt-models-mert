
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_v_fact_stockagingbydepots') }}
