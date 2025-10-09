{{
  config(
    materialized = 'table',tags = ['fms_kpi']
    )
}}

SELECT 
    COALESCE(rls_region, 'NAN_TUR') AS rls_region,
    COALESCE(rls_group, 'NAN_TUR') AS rls_group,
    COALESCE(rls_company, 'NAN_TUR') AS rls_company,
    COALESCE(rls_businessarea, 'NAN_TUR') AS rls_businessarea,
    company,
    business_area,
    supply_type,
    documantation_date,
    license_plate,
    currency,
    cost_wsl,
    cost_hsl,
    category
FROM {{ ref('stg__fms_kpi_t_fact_expenses') }}