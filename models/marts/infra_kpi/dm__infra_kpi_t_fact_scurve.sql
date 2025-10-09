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
    ,t001w.name1 AS businessarea_name
    ,[category]
    ,CAST([planned_rate] AS float) as planned_rate
    ,CAST([realized_rate] AS float) as realized_rate
    ,CAST([date] AS date) date
    ,CAST(data_control_date AS date) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_scurvetable') }} sc
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(sc.company) = cm.KyribaKisaKod
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = TRIM(sc.business_area)
)
SELECT 
  *
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
