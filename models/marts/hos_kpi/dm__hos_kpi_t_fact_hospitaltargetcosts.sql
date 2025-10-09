{{
  config(
    materialized = 'table',tags = ['hos_kpi']
    )
}}

WITH project_company_mapping AS (
SELECT
	name1
	,WERKS
	,w.BWKEY
	,bukrs
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
)

SELECT 
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea = CONCAT(pcm.werks , '_' , c.rls_region)
  ,[business_area]
  ,[equipment_group]
  ,[equipment_group_description]
  ,[year]
  ,CAST([target_cost] AS money) target_cost
  ,COALESCE([currency], 'EUR') currency
FROM {{ source('stg_sharepoint', 'raw__hos_kpi_t_fact_hospitaltargetadjustments') }} hta
LEFT JOIN project_company_mapping pcm ON pcm.werks = hta.business_area
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} c ON c.RobiKisaKod = pcm.bukrs