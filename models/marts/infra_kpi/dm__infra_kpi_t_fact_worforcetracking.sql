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
    ,t.name1 AS businessarea_name
    ,[rec_or_subcontractor]
    ,[subcontractor_name]
    ,[with_production_without_production]
    ,[discipline]
    ,ISNULL(TRY_CAST([average_number_of_personnel] AS float), 0) AS average_number_of_personnel
    ,CAST([date] AS date) AS date
    ,CAST(data_control_date AS date) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_workforcetracking') }} wft
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(wft.company) = cm.KyribaKisaKod
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(wft.business_area) = t.werks
)

SELECT 
  *
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
