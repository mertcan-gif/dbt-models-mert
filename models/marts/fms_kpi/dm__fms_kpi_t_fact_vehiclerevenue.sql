{{
  config(
    materialized = 'table',tags = ['fms_kpi']
    )
}}

SELECT 
	[rls_region] = cm.RegionCode
	,[rls_group] = cm.KyribaGrup + '_' + cm.RegionCode
	,[rls_company] = acd.rbukrs + '_' + cm.RegionCode
	,[rls_businessarea] = acd.rbusa+ '_' + cm.RegionCode
	,RBUKRS as vehicle_owner_company
	,acd.rbusa as businessarea
	,KUNNR as vehicle_using_company
	,CASE 
		WHEN LEN(AUFNR) > 1 THEN RIGHT(AUFNR, LEN(AUFNR) - 1)
		ELSE AUFNR 
	END AS license_plate
	,CAST(BUDAT as DATE) as invoice_date 
	,SUM(CAST(HSL as FLOAT)) as invoice_amount
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acd
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm on acd.rbukrs = cm.RobiKisaKod
WHERE 1=1
	AND RBUKRS = 'RMH'
	AND RACCT IN ('6000103004'
			,'6000103007'
			,'6100103004'
			,'6100103007'
			,'6790101003'
			)
GROUP BY
	 RBUKRS
	,KUNNR
	,AUFNR
	,BUDAT
	,RBUSA
	,cm.RegionCode
	,cm.KyribaGrup
HAVING
	SUM(CAST(HSL as FLOAT)) <> 0