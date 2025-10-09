{{
  config(
    materialized = 'table',tags = ['to_kpi','rmore']
    )
}}

/* 
Date: 20250915
Creator: Adem Numan Kaya
Report Owner: Ozan Duman - Isa Emre Mert 
Explanation: SAP PS ekranlari uzerinden gelen hakedisler ile ekpo tablosundaki gerceklesen tuketimler kiyaslanarak malzeme zaiyat miktari takip edilmektedir.
Detayli aciklama yazilacaktir.
*/


WITH table1 AS (
  SELECT
    ekpo.WERKS,
    ekko.LIFNR,
    ekpo.MATNR,
    ekpo.MEINS,
    'finalized' AS progress_payment_status,
    COUNT(DISTINCT ekpo.ebeln) AS n_of_ebeln,
    SUM(CAST(ekpo.MENGE AS DECIMAL(18,2))) AS production
  FROM
    {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }} as ekpo
    INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }} as ekko
      ON ekpo.EBELN = ekko.EBELN
  WHERE ekpo.EBELN LIKE '6%'
    AND (ekpo.LOEKZ <> 'L') --ekpo.LOEKZ IS NULL OR
    AND ekpo.IDNLF not in ('', 'MKFFT')
    AND ekpo.matnr <> ''
  GROUP BY
    ekpo.WERKS,
    ekko.LIFNR,
    ekpo.MATNR,
    ekpo.MEINS,
    ekpo.ebeln
),
 
-- Table 2: Non-finalized records filter
table2 AS (
  SELECT DISTINCT
    ekpo.WERKS,
    hkd.LIFNR,
    hkd.EBELN,
    'non-finalized' AS progress_payment_status
  FROM
    {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hakedis') }} as hkd
    INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }} as ekpo
      ON hkd.EBELN = ekpo.EBELN
  WHERE hkd.DURUM = '01'
    AND ekpo.loekz <> 'L'
),
 
-- Combined table: Non-finalized records with additional data
combined_table AS (
  -- Pozlog data
  SELECT
    t2.WERKS,
    t2.LIFNR,
    pzl.matnr,
    pzl.MEINS,
    t2.progress_payment_status,
    COUNT(DISTINCT pzl.EBELN) AS no_of_ebeln,
    SUM(CAST(pzl.METRAJ AS DECIMAL(18,2))) AS production
    -- Assuming METRAJ is the quantity field in pozlog
  FROM
    {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_002_t_pozlog') }} as pzl
    INNER JOIN table2 t2
      ON pzl.EBELN = t2.EBELN
  WHERE pzl.DEL_FLAG <> 'X'
    AND pzl.ONAY = 'X'
  GROUP BY
    t2.WERKS,
    t2.LIFNR,
    pzl.matnr,
    pzl.MEINS,
    t2.progress_payment_status
 
  UNION ALL
 
  -- Altyapı (Infrastructure) data
  SELECT
    t2.WERKS,
    t2.LIFNR,
    hkds.matnr,
    hkds.MEINS,
    t2.progress_payment_status,
    COUNT(DISTINCT hkds.EBELN) AS no_of_ebeln,
    SUM(CAST(hkds.METRAJ AS DECIMAL(18,2))) AS production
  FROM
    {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_002_t_hakeds') }} as hkds
    INNER JOIN table2 t2
      ON hkds.EBELN = t2.EBELN
  WHERE hkds.delflag <> 'X'
    AND hkds.ONAY = 'X'
  GROUP BY
    t2.WERKS,
    t2.LIFNR,
    hkds.matnr,
    hkds.MEINS,
    t2.progress_payment_status
 
  UNION ALL
 
  SELECT
    t2.WERKS,
    t2.LIFNR,
    zps.zzpoz AS MATNR,  
    zps.zzbirim AS MEINS,  
    'additional works' AS progress_payment_status,
    COUNT(DISTINCT t2.EBELN) AS no_of_ebeln,
    SUM(CAST(zps.ZZMETRAJ AS DECIMAL(18,2))) AS production
  FROM
    {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_003_t_001') }} as zps
    INNER JOIN table2 t2
      ON zps.ZZSOZLESME_NO = t2.EBELN
  WHERE zps.DEL_FLAG <> 'X'
    AND zps.ZZONAY = 'X'
    AND zps.zzsozlesme_no IS NOT NULL
    AND zps.zzisi_yapan_firma IS NOT NULL
  GROUP BY
    t2.WERKS,
    t2.LIFNR,
    zps.zzpoz,
    zps.zzbirim
),
 
unioned_table AS (
  -- Final union of all tables
  SELECT
    WERKS,
    LIFNR,
    MATNR,
    MEINS,
    progress_payment_status,
    n_of_ebeln,
    production
  FROM table1
 
  UNION ALL
 
  SELECT
    WERKS,
    LIFNR,
    MATNR,
    MEINS,
    progress_payment_status,
    no_of_ebeln,
    production
  FROM combined_table
),
 
final_production AS (
  SELECT
    werks as final_production_business_area,
    lifnr as final_production_subcontractor_code,
    matnr as final_production_product_code,
    MEINS as final_production_product_unit,
    SUM(n_of_ebeln) AS total_n_of_documents,
    SUM(production) AS total_production
  FROM unioned_table
  GROUP BY
    werks,
    lifnr,
    matnr,
    MEINS
),
 
consumption AS (
  SELECT
    mdoc.[werks] as consumption_business_area,
    mdoc.[matnr] as consumption_product_code,
    mdoc.[lifnr] as consumption_subcontractor_code,
    mdoc.[meins] as consumption_product_unit,
    SUM(CAST(mdoc.menge AS DECIMAL(18,2))) AS consumption_amount
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_matdoc') }} as mdoc
  WHERE 1=1
    AND LGORT IN ('TSRN', 'TSR1', 'TSR2', 'TSR3', 'TSR4', 'TSR5', 'TSR6')
    AND BWART IN ('Z05', 'Z06', 'Z29', 'Z30')
    AND (CANCELLED IS NULL OR CANCELLED <> 'X')
  GROUP BY
    mdoc.WERKS,
    mdoc.LIFNR,
    mdoc.MATNR,
    mdoc.MEINS
)

select
  rls_region
  ,rls_group
  ,rls_company
  ,rls_businessarea
  ,rls_key = concat(rls_businessarea,'-',rls_company,'-',rls_group)
  ,company = business_area_dim.company
  ,[group] = business_area_dim.[group]
  ,business_area = business_area_dim.business_area_code
  ,business_area_name = business_area_dim.business_area_tr_name
  ,mmatnr = right([mmatnr],8)
  ,pmatnr = right([pmatnr],8)
  ,subcontractor_name = lfa1.name1
  ,[start_date] = cast([bas_tarih] as date)
  ,end_date = cast([bit_tarih] as date)
  ,mz_long_txt = [mzzlongtx]
  ,pz_long_txt = [pzzlongtx]
  ,planned_material_loss_percentage = [zaiyat_yuzde]
  ,consumption.consumption_business_area
  ,consumption.consumption_product_code
  ,consumption.consumption_subcontractor_code
  ,final_production.total_n_of_documents
  ,final_production.final_production_business_area
  ,final_production.final_production_subcontractor_code
  ,final_production.final_production_product_code
  ,COALESCE(consumption.consumption_amount,0) as consumption_amount
  ,consumption.consumption_product_unit
  ,COALESCE(final_production.total_production,0) as final_production_total_production
  ,final_production.final_production_product_unit
  ,material_loss_amount = COALESCE(cast(final_production.total_production-consumption.consumption_amount as decimal(18,2)) ,0)
  ,material_loss_percentage = COALESCE(cast((final_production.total_production-consumption.consumption_amount)/final_production.total_production*100 as decimal(18,2)) ,0) 
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_025_t_zytesl') }} as zyt
  left join final_production 
    on final_production.final_production_product_code=right([pmatnr],8)
  left join consumption 
    on consumption.consumption_product_code=right([mmatnr],8)
    and consumption.consumption_subcontractor_code=final_production.final_production_subcontractor_code
    and consumption.consumption_business_area=final_production.final_production_business_area
  left join {{ ref('dm__dimensions_t_dim_businessareas') }} as business_area_dim 
    on final_production.final_production_business_area=business_area_dim.business_area_code
  left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} as lfa1 
    on final_production.final_production_subcontractor_code=lfa1.lifnr