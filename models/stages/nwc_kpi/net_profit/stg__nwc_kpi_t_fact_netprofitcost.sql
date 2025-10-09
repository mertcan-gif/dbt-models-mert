{{
  config(
    materialized = 'table',tags = ['nwc_kpi','net_profit','net_profit_cost']
    )
}}
	

WITH TAHAKKUK_GIDER AS 
(
	SELECT 
		company,
		business_area,
		account_number = general_ledger_account,
		posting_date = EOMONTH(posting_date),
		document_currency,
		budget_currency = CASE 
								WHEN rb.pb IS NOT NULL THEN rb.pb
								WHEN (SELECT TOP 1 rb2.pb FROM aws_stage.sharepoint.raw__nwc_kpi_t_dim_rtibudgets rb2 where t.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) IS NULL THEN 'TRY'
								ELSE (SELECT TOP 1 rb2.pb FROM aws_stage.sharepoint.raw__nwc_kpi_t_dim_rtibudgets rb2 where t.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) 
							END,
		amount_in_tl = SUM(amount_in_tl),
		amount_in_eur = SUM(amount_in_eur),
		amount_in_usd = SUM(amount_in_usd),
		source = CASE WHEN is_adjusting_document = 'NO' THEN 'S4HANA' ELSE 'Adjustment' END
	FROM {{ ref('dm__nwc_kpi_t_fact_costrealization') }} t
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} TCURX ON document_currency = TCURX.CURRKEY 
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb ON rb.gsber = t.business_area 
															AND MONTH(t.posting_date) =  rb.budget_month
															AND YEAR(t.posting_date) =  rb.budget_year
	WHERE 1=1
		AND [type] IN (N'GIDER', N'GİDER')
		AND posting_date > '2022-12-31'
		AND (fiscal_period NOT IN ('13','14','15','16','00') OR fiscal_period IS NULL)
		AND document_type <> 'IA'
		AND document_type <> 'SA'
	GROUP BY
		company,
		business_area,
		general_ledger_account,
		EOMONTH(posting_date),
		document_currency,
		rb.pb,
		is_adjusting_document
),


DATE_TABLE as
(
	SELECT 
		EOMONTH(DATEADD(MONTH, number, '2023-01-01')) AS end_of_month
	FROM 
		master..spt_values
	WHERE 
		type = 'P' 
		AND DATEADD(MONTH, number, '2023-01-01') <= GETDATE()
),

DIMENSION_WITH_ALL_DATES AS (
	SELECT * 
	FROM (SELECT DISTINCT company,business_area,account_number,source,budget_currency FROM TAHAKKUK_GIDER) RAW_D
		CROSS JOIN DATE_TABLE
),

TOTAL_AMOUNTS_BEFORE_CUMULATIVE AS (
	SELECT
		dt.company
		,dt.business_area
		,dt.account_number
		,posting_date = dt.end_of_month
		,budat_year = LEFT(dt.end_of_month,4)
		,total_amount_try = SUM(COALESCE(amount_in_tl,0))
		,total_amount_eur = SUM(COALESCE(amount_in_eur,0))
		,total_amount_usd = SUM(COALESCE(amount_in_usd,0))
		,dt.budget_currency
		,dt.source
	FROM DIMENSION_WITH_ALL_DATES dt
		LEFT JOIN TAHAKKUK_GIDER tg ON dt.end_of_month = tg.posting_date
									AND dt.business_area = tg.business_area
									AND dt.company = tg.company
									AND dt.source = tg.source		
									AND dt.account_number = tg.account_number	  
	GROUP BY
		dt.company
		,dt.business_area
		,dt.account_number
		,dt.end_of_month
		,dt.budget_currency
		,dt.source
)

,CUMULATIVE_TOTALS AS (
	SELECT
		company
		,business_area
		,account_number
		,source
		,posting_date
		,budget_currency
		,budat_year
		,total_amount_try
		,total_amount_eur
		,total_amount_usd
		,SUM(total_amount_try) over (partition by company,business_area,budat_year,month(posting_date),account_number,source order by posting_date) as cumulative_total_monthly_try
		,SUM(total_amount_eur) over (partition by company,business_area,budat_year,month(posting_date),account_number,source order by posting_date) as cumulative_total_monthly_eur
		,SUM(total_amount_usd) over (partition by company,business_area,budat_year,month(posting_date),account_number,source order by posting_date) as cumulative_total_monthly_usd
		,SUM(total_amount_try) over (partition by company,business_area,budat_year,account_number,source order by posting_date) as cumulative_total_try
		,SUM(total_amount_eur) over (partition by company,business_area,budat_year,account_number,source order by posting_date) as cumulative_total_eur
		,SUM(total_amount_usd) over (partition by company,business_area,budat_year,account_number,source order by posting_date) as cumulative_total_usd
	FROM TOTAL_AMOUNTS_BEFORE_CUMULATIVE dt
)

	SELECT
	company
	,business_area
	,type = 'COST'
	,account_number
	,source
	,budat_eomonth = posting_date
	,budget_currency
	,cumulative_total_try = cumulative_total_try * -1 
	,cumulative_total_eur = cumulative_total_eur * -1
	,cumulative_total_usd = cumulative_total_usd * -1
	,cumulative_total = CASE 
							WHEN budget_currency = 'EUR' THEN cumulative_total_eur * -1
							WHEN budget_currency = 'USD' THEN cumulative_total_usd * -1
						ELSE cumulative_total_try * -1 END
	,cumulative_total_monthly = CASE 
									WHEN budget_currency = 'EUR' THEN cumulative_total_monthly_eur * -1
									WHEN budget_currency = 'USD' THEN cumulative_total_monthly_usd * -1
								ELSE cumulative_total_monthly_try * -1 END
		,order_rank = '2'
	FROM CUMULATIVE_TOTALS t

	

	


