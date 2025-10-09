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
    ,t01w.name1 as [business_area_name]
    ,[categories]
    ,[sub_categories]
    ,CAST([nwc_amount] AS money) nwc_amount
    ,CAST([date] AS date) date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_nwcinfra') }} ni
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t01w on TRIM(ni.business_area) = t01w.werks
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(ni.company) = cm.KyribaKisaKod

)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
