{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}
SELECT
*
FROM {{ ref('stg__hr_kpi_v_fact_organizationstructurecodefalse') }}