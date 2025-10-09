{{
  config(
    materialized = 'table',tags = ['enrg_kpi']
    )
}}	
SELECT 
*
FROM {{ ref('stg__enrg_kpi_v_dim_powerplants') }}
