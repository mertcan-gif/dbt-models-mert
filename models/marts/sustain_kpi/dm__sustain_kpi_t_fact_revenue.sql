{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

select
NULL AS [rls_region]
,NULL AS [rls_group]
,NULL AS [rls_company]
,NULL AS [rls_businessarea]
  ,CASE 
      WHEN company = 'HQ' THEN 'HOL'
      WHEN company = 'RECA' THEN 'REC'
      WHEN company = 'RECÜ' THEN 'REC'
      WHEN company like '%REC Havacılık%' THEN 'REC'
    ELSE company end as company_filter
,CONCAT(r.company,'_',r.year) as company_year
,CONCAT(r.company,'_',r.year,'_',r.month) as company_year_month
,r.*
from 
{{ ref('stg__sustain_kpi_v_fact_revenue') }} r