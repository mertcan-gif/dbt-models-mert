{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','ingroup_arap_draft']
    )
}}

SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_ingroupaccountspayable') }}