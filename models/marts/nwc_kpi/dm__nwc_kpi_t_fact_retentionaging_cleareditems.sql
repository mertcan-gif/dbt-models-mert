
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','retentionaging']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_fact_retentionaging_cleareditems') }}
