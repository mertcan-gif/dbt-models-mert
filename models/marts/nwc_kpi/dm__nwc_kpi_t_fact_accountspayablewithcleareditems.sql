
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','arap_2_draft']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_fact_accountspayablewithcleareditems') }}
