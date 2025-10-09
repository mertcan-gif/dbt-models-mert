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
  ,rls_businessarea = TRIM(business_area) + '_' + cm.RegionCode
  ,cm.KyribaGrup as [group]
  ,[company]
  ,[business_area]
  ,[project]
  ,[scope]
  ,CAST([content_requirement] AS money) AS content_requirement
  ,CAST([realized] AS money) AS realized
  ,CAST([agreed] AS money) AS agreed_amount
  ,CAST([date] AS date) date
  ,CAST(data_control_date AS date) AS data_control_date
FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_content') }} ct
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t01w on TRIM(ct.business_area) = t01w.werks
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(ct.company) = cm.KyribaKisaKod
	)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
