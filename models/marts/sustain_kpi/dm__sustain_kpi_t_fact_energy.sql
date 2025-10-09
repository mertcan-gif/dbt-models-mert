{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}
WITH a as (
SELECT
  CASE 
      WHEN company = 'HQ' THEN 'HOL'
      WHEN company = 'RECA' THEN 'REC'
      WHEN company = 'RECÜ' THEN 'REC'
      WHEN company like '%REC Havacılık%' THEN 'REC'
    ELSE company end as company_filter
  ,CONCAT(a.company,'_',a.year) as company_year
  ,CONCAT(a.company,'_',a.year,'_',a.month) as company_year_month
  ,a.*
FROM {{ ref('stg__sustain_kpi_v_fact_energy') }} a
)
SELECT
	 rls_region = k.RegionCode
	,rls_group = CONCAT(k.KyribaGrup,'_',k.RegionCode)
	,rls_company = CONCAT(a.company_filter,'_',k.RegionCode)
	,rls_businessarea = CONCAT(a.project_code_in_dimensions_table,'_',k.RegionCode)
  ,a.*
FROM a
	LEFT JOIN  {{ ref('dm__dimensions_t_dim_companies') }} k ON k.RobiKisaKod = a.company_filter
