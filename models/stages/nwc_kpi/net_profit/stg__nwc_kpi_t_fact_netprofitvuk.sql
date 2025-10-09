{{
  config(
    materialized = 'table',tags = ['nwc_kpi','net_profit','net_profit_vuk']
    )
}}
	

WITH TAHAKKUK_OZET AS (
		SELECT 
			company = ACDOCA.RBUKRS,--ACDOCA=> RBUKRS
			business_area = ACDOCA.RBUSA, --ACDOCA=> RBUSA
			account_number = ACDOCA.RACCT,
			source = 'S4HANA',
			--posting_date = CAST(ACDOCA.BUDAT AS date),--ACDOCA=> BUDAT
			budat_eomonth = EOMONTH(CAST(ACDOCA.BUDAT AS date)),
			ACDOCA.HSL,
			ACDOCA.KSL,
			ACDOCA.OSL,
		[type] = CASE 
				WHEN RACCT = '6000201002' THEN 'HURDA'
				WHEN LEFT(RACCT,3) = '600' THEN 'Revenue'
				WHEN ((RBUKRS = 'REC' AND LEFT(RCNTR, 4) = 'RECX') OR (ACDOCA.RBUKRS = 'RMI' AND LEFT(RCNTR, 4) = 'RMIX')) THEN 'KAR'				
				ELSE 'Cost'
			END
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }} ACDOCA
			LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON (ACDOCA.GJAHR = [BKPF].GJAHR)
				AND (ACDOCA.BELNR = [BKPF].BELNR)
				AND (ACDOCA.RBUKRS = [BKPF].BUKRS)
		WHERE 1=1
			AND 
				( Left(RACCT, 3) = '740' OR
					( RBUKRS IN ('REC','RMI')
					  AND RACCT IN ('6000102001','6000202001') 
					  AND LEFT(GKONT,3) <> '350')
				  )
			AND (ACDOCA.BLART = 'WA' OR ([BKPF].XREVERSING = 0 AND [BKPF].XREVERSED = 0))
			AND ACDOCA.BLART <> 'SA'
			AND ACDOCA.BLART <> 'IA'
			--and (blart = 'IA' OR (LEFT(RIGHT(acdoca.fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(acdoca.fiscyearper,6),2) is null))
			and ((LEFT(RIGHT(acdoca.fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(acdoca.fiscyearper,6),2) is null))

		UNION ALL

		SELECT
			company 
			,business_area 
			,general_ledger_account
			,source = 'Adjustment'
			--,h_budat = CAST(h_budat AS DATE) --collate database_default
			,budat_eomonth = EOMONTH(CAST(posting_date AS DATE))
			,[amount_in_tl]
			,[amount_in_eur]
			,[amount_in_usd]
			,[type] = CASE 
						WHEN [type] = 'GELİR' THEN 'Revenue' 
						WHEN [type] = 'MERKEZ' THEN 'Cost'
						WHEN [type] = 'GIDER' THEN 'Cost'
						WHEN [type] = N'GİDER' THEN 'Cost'
						WHEN [type] = N'Faiz' THEN 'Interest'
						WHEN [type] = N'FAİZ' THEN 'Interest'
					ELSE [type] END
		FROM {{ ref('stg__nwc_kpi_v_fact_costrealizationadjustments') }}



		UNION ALL

/** Hedge için düzeltme exceli dışında SAP'den gelen verilerdir **/
		SELECT
			company = RBUKRS,
			business_area = CONCAT(RBUKRS,'M'), 
			account_number = RACCT,
			source = 'S4HANA',
			budat_eomonth = EOMONTH(CAST(BUDAT AS date)),
			HSL,
			KSL,
			OSL,
			[type] = 'Hedge'
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
		WHERE 1=1
			AND ((LEFT(RIGHT(fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(fiscyearper,6),2) is null))
			AND BLART <> 'IA'
			AND BLART <> 'SA'
			AND racct IN (
						'6490101001',
						'6490101007',
						'6490101008',
						'6490101009',
						'6490101011',
						'6490101013',
						'6490101024',
						'6490101025',
						'6490101026',
						'6590101001',
						'6590101003',
						'6590101004',
						'6590101007',
						'6590101008',
						'6590101011',
						'6590101013',
						'6590101014',
						'6590101019',
						'6590101020',
						'6590101021'
			)

		UNION ALL

/** Interest için düzeltme exceli dışında SAP'den gelen verilerdir **/
		SELECT
			company = RBUKRS,
			business_area = CONCAT(RBUKRS,'M'),
			account_number = RACCT,
			source = 'S4HANA',
			budat_eomonth = EOMONTH(CAST(BUDAT AS date)),
			HSL,
			KSL,
			OSL,
			[type] = 'Interest'
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
		WHERE 1=1
			AND ((LEFT(RIGHT(fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(fiscyearper,6),2) is null))
			AND BLART <> 'IA'
			AND BLART <> 'SA'
			AND RACCT NOT IN ('7801301009','7801301010','7801301013')
			AND
				(
					(LEFT(RACCT,3) IN ('780','642'))
						OR
					 (racct IN (
							'6000103010',
							'6000106011',
							'6010101014'
							)
					)
				)

),


TAHAKKUK_GELIR_GIDER AS (
SELECT *
FROM TAHAKKUK_OZET 
WHERE 1=1
	AND [type] <> 'KAR' 
	AND [type] <> 'GYG'

),

DATE_TABLE as
(
	SELECT 
	CAST(DATEADD(dd, number, '2023-01-01') AS DATE) Date
	FROM 
	master..spt_values m1
	WHERE 
	type = 'P' 
	AND DATEADD(dd, number, '2023-01-01') <= GETDATE()
),

DIMENSION_WITH_ALL_DATES AS (
	SELECT * 
	FROM (SELECT DISTINCT company,business_area,[type],account_number,source FROM TAHAKKUK_GELIR_GIDER) RAW_D
		CROSS JOIN (SELECT DISTINCT EOMONTH(Date) budat_eomonth FROM DATE_TABLE) DT
),

TOTAL_AMOUNTS_BEFORE_CUMULATIVE AS (
	SELECT
		dt.company
		,dt.business_area
		,currency = 'TRY'
		,dt.account_number
		,dt.source
		,budat_year = LEFT(dt.budat_eomonth,4)
		,dt.[type]
		,budat_eomonth = dt.budat_eomonth
		,total_amount_in_try = SUM(COALESCE(HSL,0))
		,total_amount_in_eur = SUM(COALESCE(KSL,0))
		,total_amount_in_usd = SUM(COALESCE(OSL,0))
	FROM DIMENSION_WITH_ALL_DATES dt
		LEFT JOIN TAHAKKUK_GELIR_GIDER rd ON dt.budat_eomonth = rd.budat_eomonth
							  AND dt.company = rd.company
							  AND dt.business_area = rd.business_area
							  AND dt.[type] = rd.[type]		
							  AND dt.account_number = rd.account_number
							  AND dt.source = rd.source		  
	GROUP BY
		dt.company
		,dt.business_area
		,dt.budat_eomonth
		,dt.[type]
		,dt.account_number
		,dt.source

),

CUMULATIVE_TOTALS AS (
	SELECT
		company
		,business_area
		,[type]
		,account_number
		,source
		,budat_year
		,budget_currency = CASE 
								WHEN rb.pb IS NOT NULL THEN rb.pb
								WHEN (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}  rb2 where dt.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) IS NULL THEN 'TRY'
								ELSE (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}  rb2 where dt.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) 
							END
		,budat_eomonth
		,SUM(total_amount_in_try) over (partition by company,business_area,type,budat_year,month(budat_eomonth),account_number,source order by budat_eomonth) as cumulative_total_monthly_try
		,SUM(total_amount_in_eur) over (partition by company,business_area,type,budat_year,month(budat_eomonth),account_number,source order by budat_eomonth) as cumulative_total_monthly_eur
		,SUM(total_amount_in_usd) over (partition by company,business_area,type,budat_year,month(budat_eomonth),account_number,source order by budat_eomonth) as cumulative_total_monthly_usd
		,SUM(total_amount_in_try) over (partition by company,business_area,type,budat_year,account_number,source order by budat_eomonth) as cumulative_try
		,SUM(total_amount_in_eur) over (partition by company,business_area,type,budat_year,account_number,source order by budat_eomonth) as cumulative_eur
		,SUM(total_amount_in_usd) over (partition by company,business_area,type,budat_year,account_number,source order by budat_eomonth) as cumulative_usd
	FROM TOTAL_AMOUNTS_BEFORE_CUMULATIVE dt
			LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb ON rb.gsber = dt.business_area 
														AND MONTH(dt.budat_eomonth) =  rb.budget_month
														AND YEAR(dt.budat_eomonth) =  rb.budget_year

)

	SELECT
		company
		,business_area
		,[type] = case 
					when [type] = 'Hedge' then 'HEDGE'
					when [type] = 'Interest' then 'INTEREST'
				end

		,account_number
		,source
		,budat_eomonth
		,budget_currency = CASE 
								WHEN rb.pb IS NOT NULL THEN rb.pb
								WHEN (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}  rb2 where t.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) IS NULL THEN 'TRY'
								ELSE (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}  rb2 where t.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) 
							END
		,cumulative_try = cumulative_try * -1  
		,cumulative_eur = cumulative_eur * -1
		,cumulative_usd = cumulative_usd * -1
		,cumulative_total = CASE 
								WHEN budget_currency = 'EUR' THEN cumulative_eur * -1
								WHEN budget_currency = 'USD' THEN cumulative_usd * -1
							ELSE cumulative_try * -1 END
		,cumulative_total_monthly = CASE 
										WHEN budget_currency = 'EUR' THEN cumulative_total_monthly_eur * -1
										WHEN budget_currency = 'USD' THEN cumulative_total_monthly_usd * -1
									ELSE cumulative_total_monthly_try * -1 END
		,order_rank = CASE 
						  WHEN [type] = 'Hedge' THEN '7'
						  WHEN [type] = 'Interest' THEN '8'
						ELSE '99' END
						  
	FROM CUMULATIVE_TOTALS t
			LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb ON rb.gsber = t.business_area 
															AND MONTH(t.budat_eomonth) =  rb.budget_month
															AND YEAR(t.budat_eomonth) =  rb.budget_year
