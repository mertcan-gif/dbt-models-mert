{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
SELECT
*
FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_averagetransportationcosts') }} m


