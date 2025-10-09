{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH _raw AS (
  SELECT
	rls_region = 'TUR'
	,rls_group = '_TUR'
	,rls_company = '_TUR'
	,rls_businessarea = '_TUR'
	,TRIM([KPI]) AS kpi
	,TRIM([Info Box Text]) AS info_box_text
	,TRIM([Responsible Person]) AS responsible_person
	,TRIM([Data Source]) AS [data_source]
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_infoboxdescriptions') }} id
)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
