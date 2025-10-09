{{
  config(
    materialized = 'view',tags = ['uygulama_gelistirme'],grants = {'select': ['s4hana_ug_user']}
    )
}}

SELECT  
    *
FROM {{ ref('stg__hr_kpi_t_sf_personnel_tickets_snapshots') }}