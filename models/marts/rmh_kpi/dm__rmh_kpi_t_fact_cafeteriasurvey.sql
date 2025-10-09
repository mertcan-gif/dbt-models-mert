{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
SELECT
	rls_region = cm.RegionCode
	,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
	,rls_company = cm.RobiKisaKod + '_' + cm.RegionCode
	,rls_businessarea = '_' + cm.RegionCode
	,company = cm.RobiKisaKod
	,cs.*
  FROM {{ ref('stg__rmh_kpi_t_fact_cafeteriasurvey') }} cs
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON 'RMH' = cm.RobiKisaKod