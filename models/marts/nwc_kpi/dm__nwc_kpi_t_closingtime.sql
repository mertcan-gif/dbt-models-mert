{{
  config(
    materialized = 'table',tags = ['nwc_kpi','closing_time']
    )
}}
	
WITH RAW_CTE AS (
	select 
		acdoca.rbukrs,
		acdoca.gjahr,
		acdoca.belnr,
		acdoca.buzei,
		acdoca.blart,
		acdoca.rbusa,
		t001w.name1,
		rwcur,
		wsl,
		CAST(acdoca.budat AS DATE) as kayit_tarihi,
		CAST(acdoca.bldat AS DATE) as belge_tarihi,
		FORMAT(CAST(acdoca.budat AS DATE),'yyyy-MM') as donem,
		CAST(bkpf.cpudt AS DATE) as giris_tarihi,
		satir.[satir_etiketi]
		,abs(hsl) as absolute_hsl
		,selection_date = 
				datefromparts(

				YEAR(CAST(acdoca.budat AS DATE)),
				month(CAST(acdoca.budat AS DATE)),1
				)
		,gun_farki = 
		DATEDIFF(DAY,
					datefromparts(
						YEAR(CAST(acdoca.budat AS DATE)),
						month(CAST(acdoca.budat AS DATE)),1
						),
				CAST(bkpf.cpudt AS DATE)
				)
		,giris_gun_farki =
		DATEDIFF(DAY,
					CAST(acdoca.bldat AS DATE),
					CAST(bkpf.cpudt AS DATE))

	from {{ ref('stg__s4hana_t_sap_acdoca_full') }} acdoca
		left join {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON 
											acdoca.rbukrs = bkpf.bukrs
											and acdoca.gjahr = bkpf.gjahr
											and acdoca.belnr = bkpf.belnr
		RIGHT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfi_046_t_satir') }} satir ON acdoca.blart = satir.blart
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} AS t001w ON acdoca.rbusa = t001w.WERKS 
	WHERE 1=1
		AND acdoca.gjahr = '2024'
		AND LEFT(acdoca.racct,3) = '740'
		and bkpf.bstat <> 'L'
		and bkpf.xreversed = 0
		and bkpf.xreversing = 0
)




SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.RBUKRS  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.RBUSA  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
FROM RAW_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_CTE.RBUKRS = kuc.RobiKisaKod 
