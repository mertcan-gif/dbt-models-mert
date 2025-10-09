{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH _raw AS (
SELECT 
	 rls_region = cm.RegionCode
    ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
    ,rls_company = cm.KyribaKisaKod + '_' + cm.RegionCode
    ,rls_businessarea = TRIM(epr.[Project Code]) + '_' + cm.RegionCode
    ,cm.KyribaGrup as [group]
	,[Company Code] as company_code
	,CAST(Date AS DATE) AS date
	,[Project Code] as project_code
	,[Project Name] as project_name 
	,CAST([POC Revenue] as MONEY) as poc_revenue
  ,CAST(data_control_date AS date) AS data_control_date
FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_executivepocrevenue') }} epr
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(epr.[Company Code]) = cm.KyribaKisaKod
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(epr.[Project Code]) = t.werks
)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw

