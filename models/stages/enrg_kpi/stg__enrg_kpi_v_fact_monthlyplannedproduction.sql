{{
  config(
    materialized = 'view',tags = ['enrg_kpi']
    )
}}	
SELECT 
	[rls_region] = NULL
	,[rls_group] = NULL
	,[rls_company]= NULL
	,[rls_businessarea]= CONCAT(WERKS,'_TUR')
	,businessarea_code = WERKS
	,[year] = GJAHR
	,[month] = CAST(POPER AS int)
	,planned_production = cast(ZZOBU as float)
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zco_005_t_aupd') }}