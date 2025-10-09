{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging']
    )
}}

WITH StokDevir AS
(
	SELECT 
		RBUKRS
		,RBUSA
		,RIGHT(MATNR,8) AS MATNR
		,a = SUM(CASE WHEN CAST(BLDAT AS date) <= '2023-12-31' THEN CAST(HSL as DECIMAL(18,2)) ELSE 0 END)  /* Dönem Başı Tutar*/
		,b = SUM(CAST(HSL as DECIMAL(18,2))) /* Güncel Tutar */ 
		--,c = SUM(CASE WHEN (BLART = 'WA' OR BLART = 'WL') AND (GKONT LIKE '740%' OR GKONT LIKE '%6210101038%') THEN HSL ELSE 0 END) /* Maliyet */
		,c = SUM(CASE WHEN CAST(BLDAT AS date) > '2023-12-31' AND (BLART = 'WA' OR BLART = 'WL') AND (GKONT LIKE '740%' OR GKONT LIKE '621%') THEN CAST(HSL as DECIMAL(18,2)) ELSE 0 END) /* Maliyet2 */
	FROM  {{ ref('stg__s4hana_t_sap_acdoca') }}
	WHERE 1=1
		AND	RACCT LIKE '15%'
		AND RACCT NOT LIKE '159%'	

	GROUP BY 
		RBUKRS
		,RBUSA
		,RIGHT(MATNR,8)
)
SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(RBUKRS ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RBUSA,''),'_',COALESCE(kuc.RegionCode,''))
	,RBUKRS
	,RBUSA
	,MATNR
	,a /* Dönem Başı Tutar*/
	,b /* Güncel Tutar */ 
	,c /* Maliyet */
	,TURNOVER = 
		CASE
		--OR (b>-1000 and b<1000)
			--WHEN ((a>-1000 and a<1000) and (b>-1000 and b<1000))  THEN 1 /* Yuvarlama farkı düzeltmesidir */
			--WHEN ((a>-100 and a<100) AND (b<1000)) THEN 1 
			WHEN ((a>-100 and a<100) OR (b>-100 and b<100)) THEN 1 
			WHEN (b>-1000 and b<1000) THEN 1
			WHEN (a = 0 AND b = 0) THEN 0 
			WHEN a + b = 0 THEN 0
			ELSE c*-1/((a+b)/2)
		END
FROM StokDevir s
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON s.RBUKRS = kuc.RobiKisaKod