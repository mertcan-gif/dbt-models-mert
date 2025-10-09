{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging']
    )
}}		
WITH raw_data_2 as (
        SELECT 
		reporting_date = CAST(Tarih AS DATE)
		,company = BUKRS
		,business_area = WERKS
		,business_area_description = NAME1
		,material = MATNR
		,material_short_text = MAKTX
		,material_group = mat.material_group --Dimension_StockMaterialGroup Excel'inden geliyor. Önceden MAKTX kolonu ile alıyorduk
		,product_group = MATKL
		,product_group_description = WGBEZ60
		,product_description = ZurktgrText
		,unit = Meins
		,currency = WAERS
		,[quantity_0_30] = CAST([Miktar_0_30] AS DECIMAL(18,5))
		,[amount_0_30] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST([Tutar_0_30] AS DECIMAL(18,5))/10 
						  ELSE CAST([Tutar_0_30] AS DECIMAL(18,5)) END
		,[quantity_30_90] = CAST([Miktar_30_90]AS DECIMAL(18,5))
		,[amount_30_90] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST([Tutar_30_90] AS DECIMAL(18,5))/10 
						  ELSE CAST([Tutar_30_90] AS DECIMAL(18,5)) END
		,[quantity_90_180] = CAST([Miktar_90_180]AS DECIMAL(18,5))
		,[amount_90_180] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST([Tutar_90_180] AS DECIMAL(18,5))/10 
						  ELSE CAST([Tutar_90_180] AS DECIMAL(18,5)) END
		,[quantity_180_360] = CAST([Miktar_180_360]AS DECIMAL(18,5))
		,[amount_180_360] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST([Tutar_180_360] AS DECIMAL(18,5))/10 
						  ELSE CAST([Tutar_180_360] AS DECIMAL(18,5)) END
		,[quantity_360_plus] = CAST([Miktar_360Plus] AS DECIMAL(18,5))
		,[amount_360_plus] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST([Tutar_360Plus] AS DECIMAL(18,5))/10 
						  ELSE CAST([Tutar_360Plus] AS DECIMAL(18,5)) END
		,[quantity_total] = CAST([ToplamMiktar]AS DECIMAL(18,5))
		,[amount_total] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST([ToplamTutar] AS DECIMAL(18,5))/10 
						  ELSE CAST([ToplamTutar] AS DECIMAL(18,5)) END
		,maximum_stock_age = [MaxStokYas]
		,average_stock_days = [OrtStokGunu]
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_stokozetsethistorical') }} [StokOzetSetHistorical]
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt ON dt.[date] = CAST(Tarih AS DATE)
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_stockmaterialgroups') }} mat ON mat.hierarchy_id = [StokOzetSetHistorical].prdha
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON [StokOzetSetHistorical].WAERS = TCURX.CURRKEY COLLATE DATABASE_DEFAULT
)
SELECT
*
FROM raw_data_2