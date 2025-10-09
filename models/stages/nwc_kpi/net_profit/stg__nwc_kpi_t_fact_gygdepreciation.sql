{{
  config(
    materialized = 'table',tags = ['nwc_kpi','net_profit','gyg_depr_net']
    )
}}
	

WITH RAW_DATA AS (
/** GYG DATA **/
	SELECT
		company = ra.bukrs ,
		business_area = ra.gsber , 
		account_number = ra.hkont,
		budat_eomonth = EOMONTH(CAST(h_budat AS date)),
		amount_in_try = balance_tl*-1, 
		amount_in_eur = balance_euro*-1,
		amount_in_usd = balance_dollar*-1,
		[type],
		source = 'Adjustment'
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtiaccrualadjustmentgyg') }} ra
		--RIGHT JOIN [aws_stage].[sharepoint].[raw__nwc_kpi_t_dim_rtibudgets] rb ON rb.gsber = ra.gsber
	WHERE 1=1
		AND [type] = 'GYG'


	UNION ALL


	SELECT 	
		company = ACDOCA.RBUKRS,
		business_area = CONCAT(ACDOCA.RBUKRS,'M'),
		account_number = ACDOCA.RACCT,
		budat_eomonth = EOMONTH(CAST(ACDOCA.BUDAT AS date)),
		amount_in_try = ACDOCA.HSL *-1, 
		amount_in_eur = ACDOCA.KSL *-1,
		amount_in_usd = ACDOCA.OSL *-1,
		[type] = 'GYG',
		source = 'S4HANA'
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} ACDOCA 
		--RIGHT JOIN [aws_stage].[sharepoint].[raw__nwc_kpi_t_dim_rtibudgets] rb ON rb.gsber  = ACDOCA.RBUSA
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON (ACDOCA.GJAHR = [BKPF].GJAHR)
			AND (ACDOCA.BELNR = [BKPF].BELNR)
			AND (ACDOCA.RBUKRS = [BKPF].BUKRS)
	WHERE 1=1
		AND LEFT(RIGHT(fiscyearper,6),2) NOT IN ('13','14','15','16','00')
		AND Left(RACCT, 3) = '770' 
		AND (ACDOCA.BLART = 'WA' OR ([BKPF].XREVERSING = 0 AND [BKPF].XREVERSED = 0))	
		AND ACDOCA.BLART <> 'SA'
		AND ACDOCA.BLART <> 'IA'


	UNION ALL


/** DEPRECIATION_DATA **/
	SELECT 	
		company = ACDOCA.RBUKRS,
		business_area = ACDOCA.RBUSA,
		account_number = ACDOCA.RACCT,
		budat_eomonth = EOMONTH(CAST(ACDOCA.BUDAT AS date)),
		amount_in_try = ACDOCA.HSL, 
		amount_in_eur = ACDOCA.KSL,
		amount_in_usd = ACDOCA.OSL,
		[type] = 'DEPRECIATION',
		source = 'S4HANA'
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} ACDOCA 
		--RIGHT JOIN [aws_stage].[sharepoint].[raw__nwc_kpi_t_dim_rtibudgets] rb ON rb.gsber  = ACDOCA.RBUSA
		LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} BKPF ON (ACDOCA.GJAHR = [BKPF].GJAHR)
			AND (ACDOCA.BELNR = [BKPF].BELNR)
			AND (ACDOCA.RBUKRS = [BKPF].BUKRS)
	WHERE 1=1
		AND LEFT(RIGHT(fiscyearper,6),2) NOT IN ('13','14','15','16','00')
		AND Left(RACCT, 1) = '7' 
		AND ACDOCA.BLART = 'AF'
		AND (ACDOCA.BLART = 'WA' OR ([BKPF].XREVERSING = 0 AND [BKPF].XREVERSED = 0))	
		AND ACDOCA.BLART <> 'SA'
		AND ACDOCA.BLART <> 'IA'
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
						  ,[type] = [type]
						  ,source = source
						  ,account_number = account_number FROM RAW_DATA) RAW_D
		CROSS JOIN (SELECT DISTINCT EOMONTH(Date) budat_eomonth FROM DATE_TABLE) DT
),

TOTAL_AMOUNTS_BEFORE_CUMULATIVE AS (
	SELECT
		dt.company
		,dt.business_area
		,dt.[type]
		,dt.account_number
		,currency = 'TRY'
		,budat_year = LEFT(dt.budat_eomonth,4)
		,budat_eomonth = dt.budat_eomonth
		,total_amount_in_try = SUM(COALESCE(amount_in_try,0))
		,total_amount_in_eur = SUM(COALESCE(amount_in_eur,0))
		,total_amount_in_usd = SUM(COALESCE(amount_in_usd,0))
		,dt.source
	FROM DIMENSION_WITH_ALL_DATES dt
		LEFT JOIN RAW_DATA rd ON dt.budat_eomonth = rd.budat_eomonth
							  AND dt.company = rd.company
							  AND dt.business_area = rd.business_area
							  AND dt.[type] = rd.[type]	
							  AND dt.source = rd.source		
							  AND dt.account_number = rd.account_number	  
	GROUP BY
		dt.company
		,dt.business_area
		,dt.budat_eomonth
		,dt.[type]
		,dt.source
		,dt.account_number
)

,CUMULATIVE_TOTALS AS (
	SELECT
		company
		,business_area
		,[type]
		,account_number
		,source
		,budat_eomonth
		,budat_year
		,budget_currency = CASE 
								WHEN rb.pb IS NOT NULL THEN rb.pb
								WHEN (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}  rb2 where dt.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) IS NULL THEN 'TRY'
								ELSE (SELECT TOP 1 rb2.pb FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}  rb2 where dt.business_area  = rb2.gsber and rb2.pb IS NOT NULL order by rb2.budget_year desc, rb2.budget_month desc) 
							END
		,total_amount_in_try
		,SUM(total_amount_in_try) over (partition by company,business_area,[type],budat_year,month(budat_eomonth),account_number order by budat_eomonth) as cumulative_total_monthly_try
		,SUM(total_amount_in_eur) over (partition by company,business_area,[type],budat_year,month(budat_eomonth),account_number order by budat_eomonth) as cumulative_total_monthly_eur
		,SUM(total_amount_in_usd) over (partition by company,business_area,[type],budat_year,month(budat_eomonth),account_number order by budat_eomonth) as cumulative_total_monthly_usd
		,SUM(total_amount_in_try) over (partition by company,business_area,[type],budat_year,source,account_number order by budat_eomonth) as cumulative_try
		,SUM(total_amount_in_eur) over (partition by company,business_area,[type],budat_year,source,account_number order by budat_eomonth) as cumulative_eur
		,SUM(total_amount_in_usd) over (partition by company,business_area,[type],budat_year,source,account_number order by budat_eomonth) as cumulative_usd
	FROM TOTAL_AMOUNTS_BEFORE_CUMULATIVE dt
			LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} rb ON rb.gsber = dt.business_area 
															AND MONTH(dt.budat_eomonth) =  rb.budget_month
															AND YEAR(dt.budat_eomonth) =  rb.budget_year
)

	SELECT
		ct.company
		,ct.business_area
		,ct.[type]
		,ct.account_number
		,ct.source
		,ct.budat_eomonth
		,budget_currency
		,ct.cumulative_try
		,ct.cumulative_eur
		,ct.cumulative_usd
		,cumulative_total = CASE 
								WHEN budget_currency = 'EUR' THEN cumulative_eur
								WHEN budget_currency = 'USD' THEN cumulative_usd
							ELSE cumulative_try END
		,cumulative_total_monthly = CASE 
										WHEN budget_currency = 'EUR' THEN cumulative_total_monthly_eur
										WHEN budget_currency = 'USD' THEN cumulative_total_monthly_usd
									ELSE cumulative_total_monthly_try END
		,order_rank = CASE 
							WHEN ct.[type] = 'DEPRECIATION' THEN '4' 
							WHEN ct.[type] = 'GYG' THEN '5'
						END
	FROM CUMULATIVE_TOTALS ct


	


