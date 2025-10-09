{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH _raw AS (
  SELECT 
    rls_region = cm.RegionCode
    ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
    ,rls_company = d.company + '_' + cm.RegionCode 
    ,rls_businessarea = d.business_area + '_' + cm.RegionCode
    ,[group] = cm.KyribaGrup
    ,[company]
    ,[business_area]
    ,t001w.name1 AS businessarea_name
    ,[part]
    ,[discipline]
    ,CAST([total_progress] as float) AS [total_progress]
    ,CAST([actual_weighted_progress] as float) AS [actual_weighted_progress]
    ,CAST([date] AS date) AS [date]
    ,CAST(data_control_date AS date) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_design') }} d
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm ON cm.KyribaKisaKod = d.company
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = d.business_area
)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
