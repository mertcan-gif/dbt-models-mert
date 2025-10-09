{{
  config(
    materialized = 'view',tags = ['nwc_kpi','stockaging','stockagingdepots']
    )
}}
WITH raw_data AS (
	SELECT 
		[Rapor Tarihi] = CAST(Tarih AS DATE)
		,[Şirket kodu] = bukrs
		,[Üretim Yeri] = CASE 
							WHEN bukrs ='BNR' AND werks = 'BNRM' AND lgort IN ('1006','8002','8003','8004','TSR1','TSR2','TSR3','TSR4','R004') THEN 'R004'
							WHEN bukrs ='BNR' AND werks = 'BNRM' AND lgort IN ('R003','TSR5') THEN 'R003'
							WHEN bukrs ='BNA' AND werks = 'BNAM' AND lgort IN ('R003','TSR5','8005') THEN 'R003'
							WHEN bukrs ='HCA' AND werks = 'HCAM' AND lgort IN ('R003','3001','8005','TSR5') THEN 'R003'
							WHEN bukrs ='HCA' AND werks = 'HCAM' AND lgort IN ('1002','1003','1006','8006','9601','9602','9603','9606','R004','TSR1','TSR2','TSR3','TSR4') THEN 'R004'
							WHEN bukrs ='RMI' AND werks = 'RMIM' AND lgort IN ('TSR1','TSR2','TSR3','TSR4','TSRN') THEN 'R004'
							WHEN bukrs ='RMI' AND werks = 'RMIM' AND lgort IN ('TSR5') THEN 'R003'
						ELSE werks END
		,[Ad 1] = CASE 
						WHEN bukrs ='BNR' AND werks = 'BNRM' AND lgort IN ('1006','8002','8003','8004','TSR1','TSR2','TSR3','TSR4','R004') THEN CONCAT((SELECT TOP 1 NAME1 FROM s4_odata.raw__s4hana_t_sap_depobazlistokozet WHERE werks = 'R004'),' - ',bukrs)
						WHEN bukrs ='BNR' AND werks = 'BNRM' AND lgort IN ('R003','TSR5') THEN CONCAT((SELECT TOP 1 NAME1 FROM s4_odata.raw__s4hana_t_sap_depobazlistokozet WHERE werks = 'R003'),' - ',bukrs)
						WHEN bukrs ='BNA' AND werks = 'BNAM' AND lgort IN ('R003','TSR5','8005') THEN CONCAT((SELECT TOP 1 NAME1 FROM s4_odata.raw__s4hana_t_sap_depobazlistokozet WHERE werks = 'R003'),' - ',bukrs)
						WHEN bukrs ='HCA' AND werks = 'HCAM' AND lgort IN ('R003','3001','8005','TSR5') THEN CONCAT((SELECT TOP 1 NAME1 FROM s4_odata.raw__s4hana_t_sap_depobazlistokozet WHERE werks = 'R003'),' - ',bukrs)
						WHEN bukrs ='HCA' AND werks = 'HCAM' AND lgort IN ('1002','1003','1006','8006','9601','9602','9603','9606','R004','TSR1','TSR2','TSR3','TSR4') THEN CONCAT((SELECT TOP 1 NAME1 FROM s4_odata.raw__s4hana_t_sap_depobazlistokozet WHERE werks = 'R004'),' - ',bukrs)
						WHEN bukrs ='RMI' AND werks = 'RMIM' AND lgort IN ('TSR1','TSR2','TSR3','TSR4','TSRN') THEN CONCAT((SELECT TOP 1 NAME1 FROM s4_odata.raw__s4hana_t_sap_depobazlistokozet WHERE werks = 'R004'),' - ',bukrs)
						WHEN bukrs ='RMI' AND werks = 'RMIM' AND lgort IN ('TSR5') THEN CONCAT((SELECT TOP 1 NAME1 FROM s4_odata.raw__s4hana_t_sap_depobazlistokozet WHERE werks = 'R003'),' - ',bukrs)
					ELSE NAME1 END
		
		,[Malzeme] = MATNR
		,[Malzeme kısa metni] = MAKTX
		,[Malzeme Kısa Metin Grup] = mat.material_group --Dimension_StockMaterialGroup Excel'inden geliyor. Önceden MAKTX kolonu ile alıyorduk
		,[Mal grubu] = MATKL
		,[Mal grubu tanımı 2] = WGBEZ60
		,[Mal grubu tanımı 3] = ZurktgrText
		,[Malzeme Birimi] = Meins
		,[Para birimi] = WAERS
		,[Miktar_0_30] = CAST(MIKTAR_0_30 AS DECIMAL(18,5))
		,[Tutar_0_30] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST(TUTAR_0_30 AS DECIMAL(18,5))/10 
						  ELSE CAST(TUTAR_0_30 AS DECIMAL(18,5)) END
		,[Miktar_30_90] = CAST(MIKTAR_30_90 AS DECIMAL(18,5))
		,[Tutar_30_90] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST(TUTAR_30_90 AS DECIMAL(18,5))/10 
						  ELSE CAST(TUTAR_30_90 AS DECIMAL(18,5)) END
		,[Miktar_90_180] = CAST(MIKTAR_30_90 AS DECIMAL(18,5))
		,[Tutar_90_180] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST(TUTAR_90_180 AS DECIMAL(18,5))/10 
						  ELSE CAST(TUTAR_90_180 AS DECIMAL(18,5)) END
		,[Miktar_180_360] = CAST(MIKTAR_180_360 AS DECIMAL(18,5))
		,[Tutar_180_360] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST(TUTAR_180_360 AS DECIMAL(18,5))/10 
						  ELSE CAST(TUTAR_180_360 AS DECIMAL(18,5)) END
		,[Miktar_360Plus] = CAST(MIKTAR_360PLUS AS DECIMAL(18,5))
		,[Tutar_360Plus] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST(TUTAR_360PLUS AS DECIMAL(18,5))/10 
						  ELSE CAST(TUTAR_360PLUS AS DECIMAL(18,5)) END
		,[ToplamMiktar] = CAST(TOPLAM_MIKTAR AS DECIMAL(18,5))
		,[ToplamTutar] = CASE
							  WHEN TCURX.CURRDEC = 3 THEN CAST(TOPLAM_TUTAR AS DECIMAL(18,5))/10 
						  ELSE CAST(TOPLAM_TUTAR AS DECIMAL(18,5)) END
		,[MaxStokYas] = MAXSTOKYAS
		,[OrtStokGunu] = ORT_STOK_GUNU
		,LGOBE
		,lgort
		,depot_type = CASE
						  WHEN LEFT(lgort,2) = '80' THEN N'Teknik Ofis Depo'
						  WHEN LEFT(lgort,2) = '96' THEN N'İhzarat Depo'
						  WHEN LEFT(lgort,2) = '97' THEN N'Atıl Ambar Depo'
						  WHEN LEFT(lgort,3) = 'TSR' THEN N'Taşeron Depo'
						  WHEN lgort IN (
										'1000',
										'1001',
										'1002',
										'1003',
										'1006',
										'1007',
										'2000',
										'2001',
										'3000',
										'3001',
										'4000',
										'5000',
										'R003',
										'R004',
										'2002',
										'2003',
										'2006',
										'2007',
										'3002',
										'3003',
										'3006',
										'3007',
										'6100',
										'7000',
										'9000',
										'9100',
										'9200',
										'9201',
										'9300',
										'9400',
										'IT01',
										'4001',
										'4002',
										'4003',
										'4006',
										'5001',
										'5002',
										'5003',
										'5006',
										'9001',
										'9002',
										'9003',
										'9006',
										'1004',
										'1005',
										'9202',
										'9203',
										'9204',
										'9205',
										'9206',
										'9207',
										'P001',
										'6200',
										'D001',
										'D002',
										'7001',
										'7002',
										'7003',
										'7004',
										'7005',
										'7009',
										'7010',
										'7011',
										'R065'
						  				) THEN 'Kullanilabilir Stok' 
						ELSE 'Diğer' 
					END
			,depot_type_details = CASE
										WHEN LEFT(lgort,2) = '20' THEN N'Elektrik  Depo'
										WHEN LEFT(lgort,2) = '30' THEN N'Mekanik Depo'
										WHEN LEFT(lgort,2) = '40' THEN N'Kimyasal Depo'
										WHEN LEFT(lgort,2) = '50' THEN N'Yakit Depo'
										WHEN LEFT(lgort,2) = '80' THEN N'Teknik Ofis Depo'
										WHEN LEFT(lgort,2) = '90' THEN N'Transit Depo'
										WHEN LEFT(lgort,2) = '91' THEN N'İhracat Depo'
										WHEN lgort = '9201' THEN N'İşveren Ambar'
										WHEN LEFT(lgort,2) = '92' THEN N'Geçici Depo'
										WHEN LEFT(lgort,2) = '94' THEN N'Pazarlama Depo'
										WHEN LEFT(lgort,2) = '96' THEN N'İhzarat Depo'
										WHEN LEFT(lgort,2) = '97' THEN N'Atıl Ambar Depo'
										WHEN LEFT(lgort,3) = 'TSR' THEN N'Taşeron Depo'
										WHEN lgort = '1005' THEN N'Buyback Depo'
										WHEN lgort = '1004' THEN N'Beton Santrali'
										WHEN LEFT(lgort,2) = '10' THEN N'İnşaat Depo'
										WHEN LEFT(lgort,3) = 'R00' THEN N'İnşaat Depo'
										WHEN lgort = 'D001' THEN N'Çarşı BVU Depo'
										WHEN lgort = 'P001' THEN N'Çekmeköy Projesi'
										WHEN lgort = '7001' THEN N'Deprem Böl  DY'
										WHEN lgort = '9200' THEN N'Geçici Depo'
										WHEN lgort = '9300' THEN N'İşletme Depo'
										WHEN lgort = 'D002' THEN N'Kmo BVU Depo'
										WHEN lgort = '7000' THEN N'Merkez(Yenikent)'
										WHEN lgort = '7002' THEN N'Duran Varlık'
										WHEN lgort = '7003' THEN N'Duran Varlık'
										WHEN lgort = '7004' THEN N'Duran Varlık'
										WHEN lgort = '7005' THEN N'Duran Varlık'
										WHEN lgort = '7009' THEN N'Duran Varlık'
										WHEN lgort = '7010' THEN N'Duran Varlık'
										WHEN lgort = '7011' THEN N'RTK Depo'
										WHEN lgort = '6200' THEN N'Yediemin Depo'
										WHEN lgort = '6100' THEN N'İSG Depo'
										WHEN lgort = 'IT01' THEN N'IT Depo'
										WHEN lgort = 'R065' THEN N'Adalet Sarayı DY'
										ELSE 'Diğer' 
									END
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_depobazlistokozet') }} [StokOzetSet]
		LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }} dt ON dt.[date] = CAST(Tarih AS DATE)
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_stockmaterialgroups') }} mat ON mat.hierarchy_id = [StokOzetSet].prdha
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON [StokOzetSet].WAERS = TCURX.CURRKEY 

	WHERE 1=1
		AND (is_end_of_month=1 
			 OR DAY(date) = '16')
		AND lgort NOT IN (
						'C003',
						'1100',
						'C001',
						'R055',
						'C005',
						'P002',
						'9500',
						'100',
						'H009',
						'R057',
						'7014',
						'IT02'
						)	
		AND NOT (lgort = 'R065' AND werks = 'RMGM') 
)

SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE([Şirket kodu] ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(raw_data.[Üretim Yeri],''),'_',COALESCE(kuc.RegionCode,''))
	,raw_data.*
	,kuc.KyribaGrup
	,kuc.KyribaKisaKod
FROM raw_data
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON raw_data.[Şirket kodu] = kuc.RobiKisaKod