
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_fact_stokdevir') }}
