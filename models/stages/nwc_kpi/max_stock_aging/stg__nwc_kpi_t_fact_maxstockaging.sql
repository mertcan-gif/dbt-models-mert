{{
  config(
    materialized = 'table',tags = ['nwc_kpi','maxstockaging']
    )
}}

SELECT
	reporting_date = CAST(id.Tarih AS DATE)
	,company = id.BUKRS
	,business_area = id.WERKS
	,business_area_description = NAME1
	,material = id.MATNR
	,material_short_text = MAKTX
	,mat.material_group --Dimension_StockMaterialGroup Excel'inden geliyor. Önceden MAKTX kolonu ile alıyorduk
	,product_group = MATKL
	,product_group_description = WGBEZ60
	,product_description = ZurktgrText
	,[unit] = Meins
	,currency = WAERS
	,quantity_total = [TOPLAM_MIKTAR]
	,amount_total = [TOPLAM_TUTAR]
	,maximum_stock_age = MAXSTOKYAS
	,average_stock_days = ORT_STOK_GUNU
	,wh_flag = '0' 
FROM {{ ref('stg__nwc_kpi_t_fact_stockagingihzarat') }} id
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt ON dt.[date] = CAST(Tarih AS DATE)
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_stockmaterialgroups') }} mat ON mat.hierarchy_id = id.prdha
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }}  TCURX ON id.WAERS = TCURX.CURRKEY
WHERE LEFT(lgort,2) <> '96'

UNION ALL

SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_maxstockagingexternalunits') }}

UNION ALL

SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_stockagingihzaratharic') }} WHERE wh_flag IS NULL



