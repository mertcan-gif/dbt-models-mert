{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

WITH x as (
SELECT
project,
project_code_in_dimensions_table,
company_filter
FROM {{ ref('dm__sustain_kpi_t_fact_allscopes') }}
UNION ALL
SELECT
project,
project_code_in_dimensions_table,
company_filter
FROM {{ ref('dm__sustain_kpi_t_fact_energy') }}
UNION ALL
SELECT
project,
project_code_in_dimensions_table,
company_filter
FROM {{ ref('dm__sustain_kpi_t_fact_waste') }}
UNION ALL
SELECT
project,
project_code_in_dimensions_table,
company_filter
FROM {{ ref('dm__sustain_kpi_t_fact_water') }}
)
, summary as (
SELECT DISTINCT

  -- NULL AS [rls_region]
  --,NULL AS [rls_group]
  --,NULL AS [rls_company]
  --,NULL AS [rls_businessarea]
  project
  ,project_code_in_dimensions_table
  ,CASE 
      WHEN company_filter = 'HQ' THEN 'HOL'
      WHEN company_filter = 'RECA' THEN 'REC'
      WHEN company_filter = 'RECÜ' THEN 'REC'
      WHEN company_filter like '%REC%' THEN 'REC'
    ELSE company_filter end as company_name_for_rls
FROM x
  where project is not null
)
SELECT
	 rls_region = k.RegionCode
	,rls_group = CONCAT(k.KyribaGrup,'_',k.RegionCode)
	,rls_company = CONCAT(s.company_name_for_rls,'_',k.RegionCode)
	,rls_businessarea = CONCAT(s.project_code_in_dimensions_table,'_',k.RegionCode)
	,s.*
FROM summary s
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} k ON k.RobiKisaKod = s.company_name_for_rls
