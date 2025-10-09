{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}

SELECT 
  rls_region = COALESCE(cm.RegionCode, 'NAR')
  ,rls_group = cm.KyribaGrup + '_' + COALESCE(cm.RegionCode, 'NAR')
  ,rls_company = hc.company + '_' + COALESCE(cm.RegionCode, 'NAR')
  ,rls_business_area = '_' + COALESCE(cm.RegionCode, 'NAR')
  ,rls_businessarea = '_' + COALESCE(cm.RegionCode, 'NAR')
  ,hc.year_month
  ,hc.company
  ,hc.total_count AS company_headcount
  ,rh.total_count AS rmh_headcount
FROM {{ ref('stg__rmh_kpi_t_fact_companyheadcount') }} hc 
LEFT JOIN {{ ref('stg__rmh_kpi_t_fact_companyheadcount') }} rh ON hc.year_month = rh.year_month 
																AND rh.company = 'RMH'
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON hc.company = cm.RobiKisaKod
WHERE hc.company IS NOT NULL
