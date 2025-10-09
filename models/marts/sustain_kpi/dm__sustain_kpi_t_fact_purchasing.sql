
{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}
WITH a AS (
SELECT
    [proje_sirketi] AS comapny
    ,CASE 
      WHEN proje_sirketi = 'HQ' THEN 'HOL'
      WHEN proje_sirketi = 'RECA' THEN 'REC'
      WHEN proje_sirketi = 'RECÜ' THEN 'REC'
      WHEN proje_sirketi like '%REC Havacılık%' THEN 'REC'
    ELSE proje_sirketi end as company_filter
    ,[yerel_alim_mi] AS is_local
    ,[tdf_toplam_tutar_usd] as tdf
    ,[yil] as year
    ,[ay] as month
    ,[tdf_usd_normalized] as tdf_cost_usd
    ,[db_upload_timestamp]
FROM {{ ref('stg__sustain_kpi_v_fact_purchasing') }} 
)
SELECT
	 rls_region = k.RegionCode
	,rls_group = CONCAT(k.KyribaGrup,'_',k.RegionCode)
	,rls_company = CONCAT(a.company_filter,'_',k.RegionCode)
	,rls_businessarea = CONCAT('_',k.RegionCode)
  ,a.*
FROM a
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} k ON k.RobiKisaKod = a.company_filter