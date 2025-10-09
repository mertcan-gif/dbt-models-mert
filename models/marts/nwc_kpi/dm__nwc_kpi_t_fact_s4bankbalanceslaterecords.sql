{{
  config(
    materialized = 'table',tags = ['s4bankbalances_laterecords','fi_kpi']
    )
}}


WITH RAW_CTE AS
(

	SELECT
		ACDOCA.RBUKRS AS company
		,ACDOCA.RACCT AS account_number
		,ACDOCA.BELNR AS document_number
		,ACDOCA.BUZEI AS document_line_item
		,ACDOCA.GJAHR AS fiscal_year 
		,ACDOCA.BLART AS document_type
		,ACDOCA.BUDAT AS posting_date
		,ACDOCA.BLDAT AS document_date
		,BKPF.CPUDT AS entry_date
		,day_difference = DATEDIFF(DAY,ACDOCA.BLDAT,BKPF.CPUDT)
		,bank_country = (select top 1 is_foreign from {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_banks') }} bc WHERE bc.bank_code = SUBSTRING(RACCT,5,3))
		,bank_description = (select top 1 bank_name from {{ source('stg_fi_kpi', 'raw__fi_kpi_t_dim_banks') }} bc WHERE bc.bank_code = SUBSTRING(RACCT,5,3)) 
		,ACDOCA.RBUSA AS business_area
		,T001W.NAME1 AS business_area_description
		,RWCUR AS currency
		,HSL AS amount_in_try
		,amount_in_document_currency = CASE WHEN TCURX.CURRDEC = 3 THEN WSL/10 ELSE WSL END
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS ACDOCA
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} T001W ON ACDOCA.RBUSA = T001W.WERKS 
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON
					ACDOCA.BELNR = BKPF.BELNR 
					AND ACDOCA.RBUKRS = BKPF.BUKRS
					AND ACDOCA.GJAHR = BKPF.GJAHR
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON ACDOCA.RWCUR = TCURX.CURRKEY 
	WHERE 1=1
		AND LEFT(RACCT,3) IN ('102')
		AND ACDOCA.BLART <> 'UE'

	)

SELECT
	[rls_region]   = kuc.RegionCode
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(company,''),'_',COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(business_area,''),'_',COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*

FROM RAW_CTE
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON RAW_CTE.company = kuc.RobiKisaKod 
WHERE 1=1 --AND RACCT = '1020001001' AND BLDAT = '20230102'
	AND day_difference > 0
