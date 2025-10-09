
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','arap_2']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_fact_accountspayable_cleareditems') }}
