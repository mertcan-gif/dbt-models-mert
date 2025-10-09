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
  ,[Main Category] as main_category
  ,[Category] as category
  ,[Sub-Category] as sub_category
  ,CAST([Total Requirement] AS money) as total_requirement
  ,CAST([Realized] AS money) as realized 
  ,CAST([date] as date) as date
  ,CAST(data_control_date AS date) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_contentdetails') }} cd
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_t001w" t01w on TRIM(cd.business_area) = t01w.werks
    LEFT JOIN "dwh_prod"."dimensions"."dm__dimensions_t_dim_companies" cm on TRIM(cd.company) = cm.KyribaKisaKod
	)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw

