{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
SELECT 
  *
FROM {{ ref('stg__to_kpi_t_fact_estimatedprogresspayment') }}