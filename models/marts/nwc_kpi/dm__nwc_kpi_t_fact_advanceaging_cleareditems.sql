
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','advanceaging','advanceaging_2']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_fact_advanceaging_cleareditems') }}
