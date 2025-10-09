{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging','stockexternalunits','maxstockaging']
    )
}}

WITH raw_data_adines AS (

/** Adinesten gelen ham veridir. Günlük olarak gelmeye başladıktan sonra rapor tarihi ayın son gününe / düne göre filtreleme yapacak şekilde güncellenecek**/

	SELECT 
		[Rapor Tarihi] = CAST(s.Tarih AS DATE)
		,[Şirket kodu] = BUKRS
		,[Üretim Yeri] = WERKS
		,[Ad 1] = NAME1
		,[Malzeme] = null 
		,[Malzeme kısa metni] = MAKTX
		,[Malzeme Kısa Metin Grup] = CASE WHEN m.material_group IS NULL THEN N'Diğer' ELSE m.material_group END --Dimension_StockMaterialGroup Excel'inden geliyor. Önceden MAKTX kolonu ile alıyorduk
		,[Mal grubu] = MATKL
		,[Mal grubu tanımı 2] = WGBEZ60
		,[Mal grubu tanımı 3] = ZurktgrText
		,[Malzeme Birimi] = Meins
		,[Para birimi] = WAERS
		,[ToplamMiktar] = cast(replace([ToplamMiktar],',','.') AS money) 
		,[ToplamTutar] = cast(replace([ToplamTutar],',','.') AS money)   
		,[MaxStokYas] = cast(m.max_stock_age as int) 
		,[OrtStokGunu] = CASE 
							WHEN CAST([ToplamTutar] AS MONEY) = 0 THEN 0
							ELSE CAST((15.00 * CAST(s.[Tutar_0_30] AS MONEY) + 60.00 * CAST(s.[Tutar_30_90] AS MONEY) + 135.00 * CAST(s.[Tutar_90_180] AS MONEY) + 270.00 * CAST(s.[Tutar_180_360] AS MONEY) + 361.00 * CAST(s.[Tutar_360Plus] AS MONEY)) / CAST([ToplamTutar] AS MONEY) AS INT)
						END
		,[wh_flag] = NULL
	FROM  {{ source('stg_adines', 'raw__nwc_kpi_t_adines_stokozetset') }} s
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt ON dt.[date] = CAST(s.Tarih AS DATE) 
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_stockmaterialgroupsadines') }} m ON m.matnr = s.Matnr

	WHERE 1=1
		AND (is_end_of_month=1 
		OR date = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE))
)


select * from raw_data_adines