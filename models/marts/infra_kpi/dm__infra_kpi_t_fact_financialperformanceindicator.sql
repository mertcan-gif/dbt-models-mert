
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
    ,rls_businessarea = TRIM(fpi.[Project Code]) + '_' + cm.RegionCode
    ,[Company Code] AS  company_code
    ,CAST([Date] AS DATE) AS  date
    ,CAST([data_control_date] AS DATE) AS data_control_date
    ,[Project Code] AS  project_code
    ,CAST(ciro_poc_hedef AS MONEY) AS ciro_poc_hedef
    ,CAST(ciro_poc_gerceklesen AS MONEY) AS ciro_poc_gerceklesen
    ,CAST(ciro_reel_hedef AS MONEY) AS  ciro_reel_hedef
    ,CAST(ciro_reel_gerceklesen AS MONEY) AS ciro_reel_gerceklesen
    ,CAST(gyg_poc_gyg AS MONEY) AS  gyg_poc_gyg
    ,CAST(gyg_poc_ciro AS MONEY) AS  gyg_poc_ciro
    ,CAST(gyg_reel_gyg AS MONEY) AS gyg_reel_gyg
    ,CAST(gyg_reel_ciro AS MONEY) AS gyg_reel_ciro
    ,CAST(kar_poc_beklenen AS MONEY) AS  kar_poc_beklenen
    ,CAST(kar_poc_nakit AS MONEY) AS  kar_poc_nakit
    ,CAST(kar_reel_beklenen  AS MONEY) AS kar_reel_beklenen 
    ,CAST(kar_reel_nakit AS MONEY) AS kar_reel_nakit
    ,CAST(end_poc_end AS MONEY) AS endirekt_poc_endirekt
    ,CAST(end_poc_mly AS MONEY) AS endirekt_poc_maliyet
    ,CAST(end_reel_end AS MONEY) AS endirekt_reel_endirekt
    ,CAST(end_reel_mly AS MONEY) AS endirekt_reel_maliyet
   FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_financial_performance_indicator') }} fpi
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(fpi.[Company Code]) = cm.KyribaKisaKod
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(fpi.[Project Code]) = t.werks 
)
SELECT  
  *
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
