{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

SELECT
*
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_employee') }}