{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging','stockexternalunits']
    )
}}

WITH raw_data_adines AS (

/** Adinesten gelen ham veridir. Günlük olarak gelmeye başladıktan sonra rapor tarihi ayın son gününe / düne göre filtreleme yapacak şekilde güncellenecek**/

	SELECT 
		reporting_date = CAST(s.Tarih AS DATE)
		,company = CASE WHEN BUKRS = 'TKM' THEN 'NS_RTB' ELSE BUKRS END
		,business_area = WERKS
		,business_area_description = NAME1
		,material = null /** Malzeme kodu bulunmadığından boş bırakılmıştır **/
		,material_short_text = MAKTX
		,material_group = CASE WHEN m.material_group IS NULL THEN N'Diğer' ELSE m.material_group END --Dimension_StockMaterialGroup Excel'inden geliyor. Önceden MAKTX kolonu ile alıyorduk
		,product_group = MATKL
		,product_group_description = WGBEZ60
		,product_description = ZurktgrText
		,unit = Meins
		,currency = WAERS
		,[quantity_0_30] = cast(replace([Miktar_0_30],',','.') AS money) 
		,[amount_0_30] = cast(replace([Tutar_0_30],',','.') AS money) 
		,[quantity_30_90] = cast(replace([Miktar_30_90],',','.') AS money) 
		,[amount_30_90] = cast(replace([Tutar_30_90],',','.') AS money) 
		,[quantity_90_180] = cast(replace([Miktar_90_180],',','.') AS money) 
		,[amount_90_180] = cast(replace([Tutar_90_180],',','.') AS money) 
		,[quantity_180_360] = cast(replace([Miktar_180_360],',','.') AS money) 
		,[amount_180_360] = cast(replace([Tutar_180_360],',','.') AS money)  
		,[quantity_360_plus] = cast(replace([Miktar_360Plus],',','.') AS money) 
		,[amount_360_plus] = cast(replace([Tutar_360Plus],',','.') AS money) 
		,[quantity_total] = cast(replace([ToplamMiktar],',','.') AS money) 
		,[amount_total] = cast(replace([ToplamTutar],',','.') AS money)   
		,[maximum_stock_age] = cast(m.max_stock_age as int) 
		,[average_stock_days] = CASE 
						WHEN CAST([ToplamTutar] AS MONEY) = 0 THEN 0
						ELSE CAST((15.00 * CAST(s.[Tutar_0_30] AS MONEY) + 60.00 * CAST(s.[Tutar_30_90] AS MONEY) + 135.00 * CAST(s.[Tutar_90_180] AS MONEY) + 270.00 * CAST(s.[Tutar_180_360] AS MONEY) + 361.00 * CAST(s.[Tutar_360Plus] AS MONEY)) / CAST([ToplamTutar] AS MONEY) AS INT)
					END
	FROM  {{ source('stg_adines', 'raw__nwc_kpi_t_adines_stokozetset') }} s
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt ON dt.[date] = CAST(s.Tarih AS DATE) 
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_stockmaterialgroupsadines') }} m ON m.matnr = s.Matnr

	WHERE 1=1
		AND (is_end_of_month=1 
		OR date = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE))
),

raw_data_ret AS (
	/** RET Yurtdışı operasyonlarının tutulduğu exceller ile iletilen veridir **/
	SELECT 
		[Rapor Tarihi] = cast([Rapor Tarihi] as date)
		,[Şirket kodu]
		,[Üretim Yeri] 
		,[Ad 1]
		,[Malzeme] 
		,[Malzeme kısa metni] 
		,[Malzeme Kısa Metin Grup]
		,[Mal grubu]
		,[Mal grubu tanımı 2] 
		,[Mal grubu tanımı 3] = null
		,[Malzeme Birimi] = null
		,[Para birimi] 
		,[Miktar_0_30] = CAST([Miktar_0_30] AS DECIMAL(18,5))
		,[Tutar_0_30] = CAST([Tutar_0_30] AS DECIMAL(18,5))
		,[Miktar_30_90] = CAST([Miktar_30_90] AS DECIMAL(18,5))
		,[Tutar_30_90] = CAST([Tutar_30_90] AS DECIMAL(18,5)) 
		,[Miktar_90_180] = CAST([Miktar_90_180]AS DECIMAL(18,5))
		,[Tutar_90_180] = CAST([Tutar_90_180] AS DECIMAL(18,5)) 
		,[Miktar_180_360] = CAST([Miktar_180_360]AS DECIMAL(18,5))
		,[Tutar_180_360] = CAST([Tutar_180_360] AS DECIMAL(18,5)) 
		,[Miktar_360Plus] = CAST([Miktar_360Plus] AS DECIMAL(18,5))
		,[Tutar_360Plus] = CAST([Tutar_360Plus] AS DECIMAL(18,5)) 
		,[ToplamMiktar] = CAST([ToplamMiktar]AS DECIMAL(18,5))
		,[ToplamTutar] = CAST([ToplamTutar] AS DECIMAL(18,5)) 
		,[MaxStokYas] = cast(null as int)
		,[OrtStokGunu] = cast(null as int)
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retstockdetails') }} rsd
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt ON dt.[date] = CAST([Rapor Tarihi] AS DATE)

	WHERE 1=1
		AND (is_end_of_month=1 
		OR date = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE))
)

SELECT * FROM raw_data_adines
union all 
SELECT * FROM raw_data_ret