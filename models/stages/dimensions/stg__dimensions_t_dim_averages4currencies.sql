{{
  config(
    materialized = 'table',tags = ['nwc_kpi','dimensions','average_curr_dim','net_profit']
    )
}}
	

WITH DATE_TABLE as
(
	SELECT DISTINCT
		EOMONTH(DATEADD(MONTH, number, '2022-12-31')) AS end_of_month
	FROM 
		master..spt_values
	WHERE 
		type = 'P' 
		AND DATEADD(MONTH, number, '2023-01-01') <= EOMONTH(GETDATE())
)

,monthly_average_currencies AS (

	select 
		currdate = eomonth(date_value)
		,currency
		,avg_try_curr_monthly = avg(try_value)
		,avg_usd_curr_monthly = avg(usd_value)
		,avg_eur_curr_monthly = avg(eur_value)

	from {{ ref('dm__dimensions_t_dim_dailys4currencies') }}
	group by
		eomonth(date_value)
		,currency
)

,ytd_before_cumulative as (
	select 
		[eomonth] = EOMONTH(date_value) 
		,year = LEFT(date_value,4)
		,currency
		,eur_sum = case 
						when currency IN ('TRY','USD') then sum(1/eur_value)
						else sum(eur_value)
					end
		,usd_sum = sum(usd_value)
		,try_sum = sum(try_value)
	from {{ ref('dm__dimensions_t_dim_dailys4currencies') }}
	group by
		EOMONTH(date_value)
		,currency
		,LEFT(date_value,4)
),

ytd_days_count as (
	/**
		Yılın başından itibaren her end of month için kaç gün geçtiğini hesaplayan CTE'dir
	**/
	select 	
		[eomonth] 
		,SUM(eur_sum) over (partition by year,currency order by [eomonth]) as ytd_days
	from ytd_before_cumulative
	where currency = 'EUR'
)

select 	
	y.[eomonth] 
	,y.currency 
	/**
		Currency TRY iken karşılığında bulunan TRY/EUR değerlerinin toplayıp ortalamasının alınması ile, EUR/TRY kurunun ortalamasının
		alınması farklı sonuçlar veriyor. Aşağıdaki işlemde TRY/EUR'dan ilerlemek yerine EUR/TRY kurunun ortalaması alınıp hesaplamalar 
		buna göre yapılmıştır
	**/
	,avg_eur_curr_ytd = case 
							when y.currency = 'TRY' then 1/(SUM(eur_sum) over (partition by year,y.currency order by y.[eomonth]) / yc.ytd_days)
							when y.currency = 'USD' then 1/(SUM(eur_sum) over (partition by year,y.currency order by y.[eomonth]) / yc.ytd_days)
							else SUM(eur_sum) over (partition by year,y.currency order by y.[eomonth]) / yc.ytd_days 
						end
	,avg_usd_curr_ytd = SUM(usd_sum) over (partition by year,y.currency order by y.[eomonth]) / yc.ytd_days
	,avg_try_curr_ytd = SUM(try_sum) over (partition by year,y.currency order by y.[eomonth]) / yc.ytd_days
	--,yc.ytd_days
	,mc.avg_eur_curr_monthly
	,mc.avg_usd_curr_monthly
	,mc.avg_try_curr_monthly
from ytd_before_cumulative y
	left join ytd_days_count yc on yc.[eomonth] = y.[eomonth]
	left join monthly_average_currencies mc on mc.currdate = y.[eomonth]
											and mc.currency = y.currency

