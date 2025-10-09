{{
  config(
    materialized = 'table',tags = ['nwc_kpi','net_profit','net_profit_other']
    )
}}
	

WITH ALL_DATA AS (

SELECT 
	*, 
	EOMONTH(BUDAT) AS budat_eomonth
FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
WHERE 1=1
	AND LEFT(RIGHT(fiscyearper,6),2) NOT IN ('13','14','15','16','00')
	AND blart <> 'IA'
	AND (RACCT = '6000201002' 
			OR (
				LEFT(RACCT,1) IN ('6','7')
				AND LEFT(RACCT,2) NOT IN ('60','61','62','63','66','69','71','72','73','74','77')
				AND (LEFT(RACCT,2) <> '78' OR RACCT IN ('7801301009','7801301010','7801301013'))
				AND LEFT(RACCT,3) NOT IN (
										'642',
										'646',
										'656'
										)
				AND racct NOT IN (
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
					)
			)
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
	FROM (SELECT DISTINCT rbukrs,rbusa,racct FROM ALL_DATA) RAW_D
		CROSS JOIN (SELECT DISTINCT EOMONTH(Date) budat_eomonth FROM DATE_TABLE) DT
),

TOTAL_AMOUNTS_BEFORE_CUMULATIVE AS (
	SELECT
		dt.rbukrs
		,dt.rbusa
		,dt.racct
		,currency = 'TRY'
		,budat_year = LEFT(dt.budat_eomonth,4)
		,budat_eomonth = dt.budat_eomonth
		,total_amount_in_try = SUM(COALESCE(HSL,0))
		,total_amount_in_eur = SUM(COALESCE(KSL,0))
		,total_amount_in_usd = SUM(COALESCE(OSL,0))
	FROM DIMENSION_WITH_ALL_DATES dt
		LEFT JOIN ALL_DATA rd ON dt.budat_eomonth = rd.budat_eomonth
							  AND dt.rbusa = rd.rbusa
							  AND dt.rbukrs= rd.rbukrs
							  AND dt.racct = rd.racct
	GROUP BY
		dt.rbukrs
		,dt.rbusa
		,dt.budat_eomonth
		,dt.racct


),

CUMULATIVE_TOTALS AS (
	SELECT
		company = rbukrs
		,business_area = rbusa
		,account_number = racct
		,posting_date = budat_eomonth
		,budat_year
		,budget_currency = CASE 
						WHEN rb.pb IS NOT NULL THEN rb.pb
						WHEN (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb2 where dt.rbusa  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) IS NULL THEN 'TRY'
						ELSE (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb2 where dt.rbusa  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) 
					END
		,SUM(total_amount_in_try) over (partition by rbukrs,rbusa,budat_year,month(budat_eomonth),racct order by budat_eomonth) as cumulative_total_monthly_try
		,SUM(total_amount_in_eur) over (partition by rbukrs,rbusa,budat_year,month(budat_eomonth),racct order by budat_eomonth) as cumulative_total_monthly_eur
		,SUM(total_amount_in_usd) over (partition by rbukrs,rbusa,budat_year,month(budat_eomonth),racct order by budat_eomonth) as cumulative_total_monthly_usd
		,SUM(total_amount_in_try) over (partition by rbukrs,rbusa,budat_year,racct order by budat_eomonth) as cumulative_try
		,SUM(total_amount_in_eur) over (partition by rbukrs,rbusa,budat_year,racct order by budat_eomonth) as cumulative_eur
		,SUM(total_amount_in_usd) over (partition by rbukrs,rbusa,budat_year,racct order by budat_eomonth) as cumulative_usd
	FROM TOTAL_AMOUNTS_BEFORE_CUMULATIVE dt
			LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb ON rb.gsber = dt.rbusa 
														AND MONTH(dt.budat_eomonth) =  rb.budget_month
														AND YEAR(dt.budat_eomonth) =  rb.budget_year
)

	SELECT
		company
		,business_area
		,type = 'OTHER'
		,account_number
		,source = 'S4HANA'
		,budat_eomonth = t.posting_date
		,budget_currency 
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
		,order_rank = '10'
	FROM CUMULATIVE_TOTALS t

	



	