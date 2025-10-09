{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging','maxstockaging']
    )
}}

with ihrazat_haric as (
	
	select distinct 
		Tarih
		,bukrs
		,werks
		,matnr
	from {{ ref('stg__nwc_kpi_t_fact_stockagingihzarat') }}

)


SELECT 
	[Rapor Tarihi] = CAST([StokOzetSet].Tarih AS DATE)
	,[Şirket kodu] = [StokOzetSet].BUKRS
	,[Üretim Yeri] = [StokOzetSet].WERKS
	,[Ad 1] = NAME1
	,[Malzeme] = [StokOzetSet].MATNR
	,[Malzeme kısa metni] = MAKTX
	,[Malzeme Kısa Metin Grup] = mat.material_group --Dimension_StockMaterialGroup Excel'inden geliyor. Önceden MAKTX kolonu ile alıyorduk
	,[Mal grubu] = MATKL
	,[Mal grubu tanımı 2] = WGBEZ60
	,[Mal grubu tanımı 3] = ZurktgrText
	,[Malzeme Birimi] = Meins
	,[Para birimi] = WAERS
	,[ToplamMiktar] = CAST([ToplamMiktar]AS DECIMAL(18,5))
	,[ToplamTutar] = CASE
							WHEN TCURX.CURRDEC = 3 THEN CAST([ToplamTutar] AS DECIMAL(18,5))/10 
						ELSE CAST([ToplamTutar] AS DECIMAL(18,5)) END
	,[MaxStokYas]
	,[OrtStokGunu]
	,wh_flag = CASE WHEN ds.Tarih IS NOT NULL THEN '1' ELSE NULL END
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_stokozetset') }} [StokOzetSet]
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt ON dt.[date] = CAST(Tarih AS DATE)
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_stockmaterialgroups') }} mat ON mat.hierarchy_id = [StokOzetSet].prdha
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON [StokOzetSet].WAERS = TCURX.CURRKEY 
	LEFT JOIN ihrazat_haric ds ON ds.bukrs = [StokOzetSet].Bukrs
								and ds.werks = [StokOzetSet].Werks
								and ds.matnr = StokOzetSet.Matnr
								and ds.Tarih = StokOzetSet.Tarih

WHERE 1=1
	AND (is_end_of_month=1 
	OR date = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE))