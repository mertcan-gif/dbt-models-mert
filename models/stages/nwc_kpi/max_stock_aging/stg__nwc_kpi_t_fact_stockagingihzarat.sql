{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging','maxstockaging']
    )
}}

	select 
		Tarih = CAST(ds.Tarih AS DATE)
		,t.bukrs
		,t.werks
		,t.matnr
		,lgort
		,TOPLAM_MIKTAR = CAST([TOPLAM_MIKTAR] AS DECIMAL(18,5))
		,TOPLAM_TUTAR = CASE
								WHEN TCURX.CURRDEC = 3 THEN CAST(TOPLAM_TUTAR AS DECIMAL(18,5))/10 
						ELSE CAST(TOPLAM_TUTAR AS DECIMAL(18,5)) END
		,prdha
		,WAERS
		,NAME1
		,MAKTX
		,matkl
		,WGBEZ60
		,ZURKTGRTEXT
		,meins
		,MAXSTOKYAS
		,ORT_STOK_GUNU
		
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_depobazlistokozet') }} ds
		left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON ds.WAERS = TCURX.CURRKEY 
		right join (
						SELECT DISTINCT bukrs,werks,matnr,Tarih
						FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_depobazlistokozet') }}
						WHERE LEFT(lgort,2) = '96'
							AND (DAY(DATEADD(D,1,Tarih))='01' 
								OR Tarih = CAST(DATEADD(DAY,-1,GETDATE()) AS DATE))
					 ) t on t.bukrs = ds.bukrs
						and t.werks = ds.werks
						and t.matnr = ds.matnr
						and t.Tarih = ds.Tarih
	-- WHERE LEFT(lgort,2) <> '96'