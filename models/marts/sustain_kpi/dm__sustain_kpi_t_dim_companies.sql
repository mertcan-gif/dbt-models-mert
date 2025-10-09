{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

WITH x as (
SELECT
company
FROM {{ ref('dm__sustain_kpi_t_fact_allscopes') }}
UNION ALL
SELECT
company
FROM {{ ref('dm__sustain_kpi_t_fact_energy') }}
UNION ALL
SELECT
company
FROM {{ ref('dm__sustain_kpi_t_fact_revenue') }}
UNION ALL
SELECT
company
FROM {{ ref('dm__sustain_kpi_t_fact_waste') }}
UNION ALL
SELECT
company
FROM {{ ref('dm__sustain_kpi_t_fact_water') }}
UNION ALL
SELECT
company_adjusted
FROM {{ ref('dm__sustain_kpi_t_fact_training') }}
UNION ALL
SELECT
comapny
FROM {{ ref('dm__sustain_kpi_t_fact_purchasing') }}
),
t as (
SELECT DISTINCT 
  CASE 
      WHEN company = 'HQ' THEN 'HOL'
      WHEN company like '%REC%' THEN 'REC'
    ELSE company end as company_name_for_rls
    ,company
    ,NULL AS [rls_region]
    ,NULL AS [rls_group]
    ,NULL AS [rls_company]
    ,NULL AS [rls_businessarea]
FROM x
    where company is not null
)
SELECT
 a.[rls_region]
,a.[rls_group]
,a.[rls_company]
,a.[rls_businessarea]
,t.company_name_for_rls
,t.company

FROM t
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} a on a.RobiKisaKod = t.company_name_for_rls