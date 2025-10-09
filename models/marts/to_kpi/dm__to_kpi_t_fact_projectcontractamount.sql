{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
SELECT 	
	[rls_region] = cm.RegionCode
	,[rls_group] = CONCAT(cm.[KyribaGrup], '_', cm.RegionCode)
	,[rls_company] = CONCAT(pca.company, '_', cm.RegionCode)
	,[rls_businessarea] = CONCAT(pca.project, '_', cm.RegionCode)
	,pca.*
FROM {{ ref('stg__to_kpi_t_fact_projectcontractamount') }} pca
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON pca.company = cm.RobiKisaKod