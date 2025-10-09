{{
  config(
    materialized = 'table',tags = ['nwc_kpi','monthlyreport']
    )
}}

	SELECT
		[month] = MONTH(date_value)
		,year_month = CONCAT(YEAR(date_value),'-',MONTH(date_value))
		,currency
		,try_value
		,eur_value
	FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }}
	WHERE 1=1
		AND DATEPART(d,DATEADD(d,1,date_value)) = 1 -- Last day of a month
		AND YEAR(date_value) >= DATEADD(YEAR,-1,YEAR(GETDATE()))