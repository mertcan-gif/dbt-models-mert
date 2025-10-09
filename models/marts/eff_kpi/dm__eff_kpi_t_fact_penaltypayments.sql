{{
  config(
    materialized = 'table',tags = ['eff_kpi']
    )
}}

SELECT
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea = CONCAT(rbusa, '_', rls_region),
	rbukrs AS [company],
	rbusa AS business_area,
	t001w.name1 AS business_area_description,
	acdoca.blart AS document_type,
	acdoca.belnr AS document_number,
	document_date = CAST(bldat AS DATE),
	posting_date = CAST(budat AS DATE),
	right(fiscyearper, 4) AS [year],
	racct AS account_number,
	sgtxt AS account_description,
	hsl AS amount,
	rwcur AS [currency],
	usnam AS user_name
FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acdoca.rbusa = t001w.werks
LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acdoca.rbukrs = bkpf.bukrs
											   AND acdoca.gjahr = bkpf.gjahr
											   AND acdoca.belnr = bkpf.belnr
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON acdoca.rbukrs = dim_comp.RobiKisaKod
WHERE 1=1
	AND racct in (
	'7301208005',
	'7301209012',
	'7401208005',
	'7401209012',
	'7401212009',
	'7601208005',
	'7601209012',
	'7602101006',
	'7701208005',
	'7701209012',
	'7702412008'
)
	AND acdoca.blart <> N'SA'
	--fiscal yearda 13,14,15,16 alinmiyor, kapanış kayıtları oldugu icin
	AND (LEFT(RIGHT(acdoca.fiscyearper,6),2) NOT IN ('13','14','15','16','00'))
	--ters kayılar hariç bırakılır:
	AND xreversed = '0' 
	AND xreversing = '0'