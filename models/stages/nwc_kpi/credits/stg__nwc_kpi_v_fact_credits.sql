{{
  config(
    materialized = 'view',tags = ['nwc_kpi','credits']
    )
}}
	
WITH RAW_CTE AS
(
	SELECT 
		ACDOCA.RBUKRS, --Şirket Kodu
		ACDOCA.BELNR,
		ACDOCA.BUZEI,
		ACDOCA.GJAHR,
		ACDOCA.BLART,
		ACDOCA.BUDAT,
		ACDOCA.BLDAT,
		BKPF.CPUDT,
		MAINACCOUNT = LEFT(RACCT,3), -- Borç Türü
		LFA1.LIFNR, -- Satıcı Kodu
		LFA1.NAME1 AS VENDOR, -- Satıcı Adı
		ACDOCA.RBUSA, --İş Alanı
		T001W.NAME1 AS PROJECTNAME, -- Proje Adı
		RWCUR, --Döviz Cinsi
		WSL = CASE
				  WHEN TCURX.CURRDEC = 3 THEN WSL/10 
			  ELSE WSL END,
		HSL

	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} LFA1 ON ACDOCA.LIFNR = LFA1.LIFNR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} T001W ON ACDOCA.RBUSA = T001W.WERKS 
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON
					ACDOCA.BELNR = BKPF.BELNR 
					AND ACDOCA.RBUKRS = BKPF.BUKRS
					AND ACDOCA.GJAHR = BKPF.GJAHR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY
	WHERE 1=1
		AND LEFT(RACCT,3) = '300'
		and (LEFT(RIGHT(ACDOCA.fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(ACDOCA.fiscyearper,6),2) is null)
		--AND CAST(H_BUDAT AS date) <= '2023-05-31'
		--AND LEN(ACDOCA.LIFNR)<>3
		--AND _BKPF.STBLG <> 'X'
		--AND ACDOCA.BUZEI <> '000'
		--AND ACDOCA.LIFNR <> ''
		--AND ACDOCA.AUGBL = ''
		--AND NETDT <> '00000000'

)
SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.RBUKRS ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.RBUSA ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
	,kuc.KyribaGrup
	,kuc.KyribaKisaKod
FROM RAW_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_CTE.RBUKRS = kuc.RobiKisaKod

