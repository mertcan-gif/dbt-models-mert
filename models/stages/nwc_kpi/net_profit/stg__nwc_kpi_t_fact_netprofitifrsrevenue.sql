{{
  config(
    materialized = 'table',tags = ['nwc_kpi','net_profit','net_profit_revenue']
    )
}}
	

WITH REVENUE_RAW AS (

	SELECT
		bukrs
		,gsber
		,account_number = ''
		,source = 'Due to Due From'
		,currency
		,[year] = LEFT(starting_date_of_month,4)
		,end_of_month = EOMONTH(starting_date_of_month)
		,gerceklesmesi_gereken_gelir
		,eur_value
		,try_value
	FROM {{ ref('dm__nwc_kpi_t_fact_duetoduefrom') }}

	UNION ALL

	SELECT
		bukrs = CASE 
					WHEN rbusa = 'R043' THEN 'RMI'
					ELSE 'REC'
				END
		,rbusa
		,account_number = ''
		,source = ''
		,currency
		,[year] = '2022'
		,end_of_month = '2022-12-31'
		,revenue_cumulative
		,eur_value = (SELECT eur_value FROM dwh_prod.dimensions.dm__dimensions_t_dim_dailys4currencies d
								WHERE date_value = CAST('2022-12-31' AS date) AND d.currency = r.currency )
		,try_value= (SELECT try_value FROM dwh_prod.dimensions.dm__dimensions_t_dim_dailys4currencies d
								WHERE date_value = CAST('2022-12-31' AS date) AND d.currency = r.currency )
	FROM aws_stage.sharepoint.raw__nwc_kpi_t_fact_rtigroup2022revenue r
),

monthly_revenue AS (
    SELECT
		bukrs
		,gsber
		,account_number
		,source
		,currency
		,[year] 
		,end_of_month 
		,gerceklesmesi_gereken_gelir
		,eur_value
		,try_value
        ,gerceklesmesi_gereken_gelir - ISNULL(LAG(gerceklesmesi_gereken_gelir) OVER (PARTITION BY bukrs, gsber ORDER BY end_of_month), 0) AS monthly_ifrs_revenue
    FROM 
        REVENUE_RAW
),

monthly_revenue_cumulative AS (
    SELECT 
		bukrs
		,gsber
		,account_number
		,source
		,currency
		,[year] 
		,end_of_month 
		,gerceklesmesi_gereken_gelir
		,eur_value
		,try_value
		,monthly_ifrs_revenue
        ,SUM(monthly_ifrs_revenue) OVER (PARTITION BY [year], bukrs, gsber ORDER BY end_of_month) AS yearly_cumulative_ifrs_revenue
    FROM 
        monthly_revenue
)
SELECT 
		company = bukrs
		,business_area = gsber
		,type = 'REVENUE'
		,account_number
		,source
		,budat_eomonth = end_of_month 
		,budget_currency = currency
		,cumulative_try = yearly_cumulative_ifrs_revenue * try_value
		,cumulative_eur = yearly_cumulative_ifrs_revenue * eur_value
		,cumulative_usd = 0
		,cumulative_total = yearly_cumulative_ifrs_revenue
		,cumulative_total_monthly = monthly_ifrs_revenue
		,order_rank = '1'
FROM 
    monthly_revenue_cumulative
	


