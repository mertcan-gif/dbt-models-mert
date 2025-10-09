{{
  config(
    materialized = 'table',tags = ['nwc_kpi','net_profit']
    )
}}
	

WITH RAW_DATA AS (
/** FX DATA **/
	SELECT 	
		company = ACDOCA.RBUKRS,
		business_area = CASE
							WHEN racct IN ('6560101001', '6560101003', '6460101001', '6460101003') THEN CONCAT(ACDOCA.rbukrs,'M')
							ELSE ACDOCA.rbusa
						END,
		account_number = ACDOCA.RACCT,
		budat_eomonth = EOMONTH(CAST(ACDOCA.BUDAT AS date)),
		amount_in_try = ACDOCA.HSL, 
		amount_in_eur = ACDOCA.KSL,
		amount_in_usd = ACDOCA.OSL,
		source = 'S4HANA'
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} ACDOCA 
	WHERE 1=1
		AND budat >= '20230101'
		and blart <> 'SA'
		AND blart <> 'IA'
		AND Left(RACCT, 3) IN ('646','656') 
		and (LEFT(RIGHT(fiscyearper,6),2) NOT IN ('13','14','15','16','00') OR fiscyearper IS NULL)


	UNION ALL


	SELECT
		company 
		,business_area 
		,general_ledger_account
		,budat_eomonth = EOMONTH(CAST(posting_date AS DATE))
		,[amount_in_tl]
		,[amount_in_eur]
		,[amount_in_usd]
		,source = 'Adjustment'
	FROM {{ ref('stg__nwc_kpi_v_fact_costrealizationadjustments') }}
	WHERE [type] = 'FX'

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
	FROM (SELECT DISTINCT company = company 
						  ,business_area = business_area 
						  ,account_number = account_number
						  ,source = source
							FROM RAW_DATA) RAW_D
		CROSS JOIN (SELECT DISTINCT EOMONTH(Date) budat_eomonth FROM DATE_TABLE) DT
),

TOTAL_AMOUNTS_BEFORE_CUMULATIVE AS (
	SELECT
		dt.company
		,dt.business_area
		,dt.account_number
		,dt.source
		,currency = 'TRY'
		,budat_year = LEFT(dt.budat_eomonth,4)
		,budat_eomonth = dt.budat_eomonth
		,total_amount_in_try = SUM(COALESCE(amount_in_try,0))
		,total_amount_in_eur = SUM(COALESCE(amount_in_eur,0))
		,total_amount_in_usd = SUM(COALESCE(amount_in_usd,0))
	FROM DIMENSION_WITH_ALL_DATES dt
		LEFT JOIN RAW_DATA rd ON dt.budat_eomonth = rd.budat_eomonth
								AND dt.business_area = rd.business_area
								AND dt.company = rd.company
								AND dt.source = rd.source		
								AND dt.account_number = rd.account_number	  
	GROUP BY
		dt.company
		,dt.business_area
		,dt.budat_eomonth
		,dt.account_number
		,dt.source
)

,CUMULATIVE_TOTALS AS (
	SELECT
		company
		,business_area
		,account_number
		,source
		,budat_year
		,budget_currency  = CASE 
								WHEN rb.pb IS NOT NULL THEN rb.pb
								WHEN (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}  rb2 where dt.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) IS NULL THEN 'TRY'
								ELSE (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}  rb2 where dt.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) 
							END
		,budat_eomonth
		,total_amount_in_try
		,SUM(total_amount_in_try) over (partition by company,business_area,budat_year,month(budat_eomonth),account_number,source order by budat_eomonth) as cumulative_total_monthly_try
		,SUM(total_amount_in_eur) over (partition by company,business_area,budat_year,month(budat_eomonth),account_number,source order by budat_eomonth) as cumulative_total_monthly_eur
		,SUM(total_amount_in_usd) over (partition by company,business_area,budat_year,month(budat_eomonth),account_number,source order by budat_eomonth) as cumulative_total_monthly_usd
		,SUM(total_amount_in_try) over (partition by company,business_area,budat_year,account_number,source order by budat_eomonth) as cumulative_try
		,SUM(total_amount_in_eur) over (partition by company,business_area,budat_year,account_number,source order by budat_eomonth) as cumulative_eur
		,SUM(total_amount_in_usd) over (partition by company,business_area,budat_year,account_number,source order by budat_eomonth) as cumulative_usd
	FROM TOTAL_AMOUNTS_BEFORE_CUMULATIVE dt
			LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb ON rb.gsber = dt.business_area 
														AND MONTH(dt.budat_eomonth) =  rb.budget_month
														AND YEAR(dt.budat_eomonth) =  rb.budget_year
)

	SELECT
		ct.company
		,ct.business_area
		,[type] = 'FX'
		,ct.account_number
		,ct.source
		,ct.budat_eomonth
		,budget_currency
		,cumulative_try = ct.cumulative_try * -1
		,cumulative_eur = ct.cumulative_eur * -1
		,cumulative_usd = ct.cumulative_usd * -1
		,cumulative_total = CASE 
								WHEN budget_currency = 'EUR' THEN cumulative_eur * -1
								WHEN budget_currency = 'USD' THEN cumulative_usd * -1
							ELSE cumulative_try * -1 END
		,cumulative_total_monthly = CASE 
										WHEN budget_currency = 'EUR' THEN cumulative_total_monthly_eur * -1
										WHEN budget_currency = 'USD' THEN cumulative_total_monthly_usd * -1
									ELSE cumulative_total_monthly_try * -1 END
		,order_rank = '9'
	FROM CUMULATIVE_TOTALS ct





