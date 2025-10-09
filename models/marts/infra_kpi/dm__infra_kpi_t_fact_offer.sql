{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH _raw AS (
  SELECT 
    rls_region = cm.RegionCode
    ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
    ,rls_company = company + '_' + cm.RegionCode
    ,rls_businessarea = business_area + '_' + cm.RegionCode
    ,[group] = cm.KyribaGrup
    ,[company]
    ,[business_area]
    ,t001w.name1 AS businessarea_name
    ,[work_name]
    ,[category]
    ,[area]
    ,[type]
    ,[type2]
    ,CAST([start_date] AS date) AS start_date
    ,TRY_CAST([end_date] AS date) AS end_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_offer') }} ofr
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on cm.KyribaKisaKod = ofr.company
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = TRIM(ofr.business_area)
)

SELECT 
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
