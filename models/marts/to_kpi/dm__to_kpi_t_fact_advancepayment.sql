{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}

SELECT 	
	[rls_region] = cm.RegionCode
	,[rls_group] = CONCAT(cm.[KyribaGrup], '_', cm.RegionCode)
	,[rls_company] = CONCAT(adp.company, '_', cm.RegionCode)
	,[rls_businessarea] = CONCAT(adp.business_area, '_', cm.RegionCode)
	,adp.*
FROM {{ ref('stg__to_kpi_t_fact_advancepayment') }} adp
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON adp.company = cm.RobiKisaKod

