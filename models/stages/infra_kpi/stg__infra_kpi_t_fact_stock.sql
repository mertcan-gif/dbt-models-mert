{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}
WITH raw_data AS (
  SELECT
    dso.[bukrs]
    ,[business_area]
    ,TRIM(iso.[project]) project
    ,iso.[storage]
    ,iso.[storage_category]
    ,dso.[WAERS]
    ,CAST(dso.[TOPLAM_MIKTAR] AS float) as toplam_miktar
    ,CAST(dso.[TOPLAM_TUTAR] AS money) as toplam_tutar
    ,CAST(dso.[Tarih] AS DATE) as date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_dim_infrastorages') }} iso
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_depobazlistokozet') }} dso ON TRIM(iso.business_area) = dso.werks
                                                                                    AND TRIM(iso.storage) = dso.lgort
  WHERE dso.[TOPLAM_MIKTAR] IS NOT NULL
  )
 
,group_data AS (
  SELECT
    CASE  
      WHEN project = 'MAG' THEN 'R004'
      WHEN project = 'KMO' THEN 'R003'
      ELSE project
    END AS business_area
    ,storage storage_no
    ,storage_category
    ,SUM(toplam_miktar) as total_quantity
    ,SUM(toplam_tutar) as total_amount
    ,waers currency
    ,date
  FROM raw_data rd
  GROUP BY bukrs
      ,business_area
      ,project
      ,storage
      ,waers
      ,storage_category
      ,date
  )
 
SELECT
  gd.business_area
  ,gd.storage_no
  ,gd.storage_category
  ,gd.total_quantity
  ,gd.total_amount
  ,gd.currency
  ,total_amount_eur = gd.total_amount * eur_value
  ,total_amount_usd = gd.total_amount * usd_value
  ,gd.date
FROM group_data gd
LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} curr on curr.currency = gd.currency
                                      AND curr.date_value = gd.date