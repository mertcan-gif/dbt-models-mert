{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH MAIN_CTE AS (
	SELECT
		acdoca.rbukrs AS [company],
		acdoca.rbusa AS [business_area],
		t001w.name1 AS [business_area_description],
		acdoca.gjahr AS [fiscal_year],
		acdoca.racct AS [account_number],
		CAST(budat AS DATE) AS [posting_date],
		acdoca.blart AS [document_type],
		mwskz AS [tax_type],
		[amount] =
					(
						(CASE WHEN acdoca.racct LIKE '600%' THEN hsl ELSE 0 END) -
						(CASE WHEN acdoca.racct LIKE '610%' THEN hsl ELSE 0 END) -
						(CASE WHEN acdoca.racct LIKE '381%' THEN hsl ELSE 0 END) -
						(CASE WHEN acdoca.racct LIKE '611%' THEN hsl ELSE 0 END) +
						(CASE WHEN acdoca.racct LIKE '649%' THEN hsl ELSE 0 END) +
						(CASE WHEN acdoca.racct LIKE '391%' THEN hsl ELSE 0 END) -
						(CASE WHEN acdoca.racct LIKE '191%' THEN hsl ELSE 0 END)
					),
		amount_type = 'Invoice'
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acdoca.rbusa = t001w.werks
	LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acdoca.gjahr = bkpf.gjahr
			AND acdoca.rbukrs = bkpf.bukrs
			AND acdoca.belnr = bkpf.belnr
	WHERE 1=1
		AND ((racct BETWEEN '6000203001' AND '6000204017') OR (racct LIKE '610%' OR racct LIKE N'611%' OR racct LIKE '381%' OR racct LIKE '391%' OR racct LIKE '649%' OR racct LIKE '191%' OR racct LIKE '102%'))
		AND acdoca.blart IN (N'DM' , N'DG' , N'DR' , N'DI')
		AND (racct LIKE N'191%' AND mwskz IN (N'2E' , N'3E' , N'E3') OR (racct LIKE N'391%' AND mwskz in (N'2A' , N'3A' , N'A3' , N'3T')) OR (racct NOT LIKE '191%' AND racct NOT LIKE '391%'))
		AND stblg = ''

	UNION ALL

	SELECT
		acdoca.rbukrs AS [company],
		acdoca.rbusa AS [business_area],
		t001w.name1 AS [business_area_description],
		acdoca.gjahr AS [fiscal_year],
		acdoca.racct AS [account_number],
		CAST(budat AS DATE) AS [posting_date],
		acdoca.blart AS [document_type],
		mwskz AS [tax_type],
		hsl AS [amount],
		amount_type = 'Cash Collection'
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acdoca.rbusa = t001w.werks
	LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acdoca.gjahr = bkpf.gjahr
			AND acdoca.rbukrs = bkpf.bukrs
			AND acdoca.belnr = bkpf.belnr
	WHERE 1=1
		AND racct LIKE '102%'
		AND acdoca.blart = N'S2'
		AND gkoar IN ('D', 'S')
		AND stblg = ''
)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(business_area, '_', rls_region),
	cte.*
FROM MAIN_CTE cte
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON cte.[company] = dim_comp.RobiKisaKod
WHERE dim_comp.KyribaGrup = N'RGYGROUP'