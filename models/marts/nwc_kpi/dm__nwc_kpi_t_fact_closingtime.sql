{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','closing_time_draft']
    )
}}

WITH RAW_CTE AS (
	select 
		acdoca.rbukrs AS [company],
		acdoca.gjahr AS fiscal_year,
		acdoca.belnr AS document_number,
		acdoca.buzei AS document_line_item,
		acdoca.blart AS document_type,
		acdoca.rbusa AS business_area,
		t001w.name1 AS business_area_description,
		rwcur AS document_currency,
		wsl AS amount_in_document_currency,
		CAST(acdoca.budat AS DATE) AS posting_date,
		CAST(acdoca.bldat AS DATE) AS document_date,
		FORMAT(CAST(acdoca.budat AS DATE),'yyyy-MM') AS [period],
		CAST(bkpf.cpudt AS DATE) AS entry_date,
		satir.[satir_etiketi] AS tag,
		abs(hsl) AS abs_amount_in_company_currency,
		selection_date = 
				datefromparts(

				YEAR(CAST(acdoca.budat AS DATE)),
				month(CAST(acdoca.budat AS DATE)),1
				),
		day_difference = 
		DATEDIFF(DAY,
					datefromparts(
						YEAR(CAST(acdoca.budat AS DATE)),
						month(CAST(acdoca.budat AS DATE)),1
						),
				CAST(bkpf.cpudt AS DATE)
				),
		entry_day_difference =
		DATEDIFF(DAY,
					CAST(acdoca.bldat AS DATE),
					CAST(bkpf.cpudt AS DATE))

	from "aws_stage"."s4_odata"."stg__s4hana_t_sap_acdoca_full" acdoca
		left join "aws_stage"."s4_odata"."raw__s4hana_t_sap_bkpf" bkpf ON 
											acdoca.rbukrs = bkpf.bukrs
											and acdoca.gjahr = bkpf.gjahr
											and acdoca.belnr = bkpf.belnr
		RIGHT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_zfi_046_t_satir" satir ON acdoca.blart = satir.blart
		LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_t001w" AS t001w ON acdoca.rbusa = t001w.WERKS 
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
	,[rls_company] = CONCAT(COALESCE(RAW_CTE.[company]  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(RAW_CTE.business_area  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,RAW_CTE.*
FROM RAW_CTE
	LEFT JOIN "aws_stage"."dimensions"."raw__dwh_t_dim_companymapping" kuc ON RAW_CTE.[company] = kuc.RobiKisaKod