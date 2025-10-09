{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

WITH a AS (
SELECT
  CASE 
    WHEN grup_sirket = 'REC ALTYAPI' THEN 'RECA'
    WHEN grup_sirket = 'REC ÜSTYAPI' THEN 'RECÜ'
    ELSE grup_sirket
  END AS company
  ,CASE 
      WHEN grup_sirket = 'HQ' THEN 'HOL'
      WHEN grup_sirket = 'RECA' THEN 'REC'
      WHEN grup_sirket = 'RECÜ' THEN 'REC'
      WHEN grup_sirket like '%REC Havacılık%' THEN 'REC'
    ELSE grup_sirket end as company_filter
  ,[grup_sirket] as group_company_sharepoint
  ,[yil] as year
  ,ay as month
  ,[yonetimde_calisan_kadin_calisan_sayisi] as female_number_in_management
  ,[yönetimde_calisan_toplam_calisan_sayisi] as whole_management
  ,[kadin_calisan_sayisi] as female_number
  ,[toplam_calisan_sayisi] as total_number
  ,[db_upload_timestamp]
FROM {{ ref('stg__sustain_kpi_v_fact_employee') }} a
)
SELECT
	 rls_region = k.RegionCode
	,rls_group = CONCAT(k.KyribaGrup,'_',k.RegionCode)
	,rls_company = CONCAT(a.company_filter,'_',k.RegionCode)
	,rls_businessarea = CONCAT('_',k.RegionCode)
  ,a.*
FROM a
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} k ON k.RobiKisaKod = a.company_filter