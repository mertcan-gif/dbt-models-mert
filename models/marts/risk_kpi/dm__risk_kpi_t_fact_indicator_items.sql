{{
  config(
    materialized = 'table',tags = ['risk_kpi']
    )
}}
SELECT
 p.rls_region
,p.rls_group
,p.rls_company
,p.rls_businessarea
,rp.*
FROM {{ ref('stg__risk_kpi_v_fact_indicator_items') }} rp
    LEFT JOIN {{ ref('dm__dimensions_t_dim_projects') }} p on p.business_area = rp.project_code
    