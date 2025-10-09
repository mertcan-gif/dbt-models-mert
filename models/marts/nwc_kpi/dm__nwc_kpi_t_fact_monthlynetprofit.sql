
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','net_profit_draft','net_profit_draft']
    )
}}

with net_profit_unionized as (

	select 		
		company 
		,business_area
		,type = 'REVENUE'
		,account_number
		,source
		,end_of_month = budat_eomonth 
		,budget_currency
		,cumulative_try
		,cumulative_eur 
		,cumulative_usd 
		,cumulative_total
		,cumulative_total_monthly 
		,order_rank
	from {{ ref('stg__nwc_kpi_t_fact_netprofitifrsrevenue') }}


		union all


	select * from {{ ref('stg__nwc_kpi_t_fact_netprofitcost') }}


		union all


	select * from {{ ref('stg__nwc_kpi_t_fact_gygdepreciation') }}


		union all


	select * from {{ ref('stg__nwc_kpi_t_fact_netprofitvuk') }}
	where 1=1 
		and (type = 'Hedge' or type = 'Interest')


		union all


	select * from {{ ref('stg__nwc_kpi_t_fact_netprofitfx') }}


		union all


	select * from {{ ref('stg__nwc_kpi_t_fact_netprofitother') }}

)

select 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(n.company ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(n.business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,n.*
	,business_area_name = t.name1
	,account_name = s.txt50
	,ac.avg_try_curr_ytd
	,ac.avg_eur_curr_ytd
	,ac.avg_usd_curr_ytd
	,ac.avg_try_curr_monthly
	,ac.avg_eur_curr_monthly
	,ac.avg_usd_curr_monthly
from net_profit_unionized n
	left join {{ ref('vw__s4hana_v_sap_ug_skat') }} s on CAST(s.saknr AS nvarchar) = CAST(n.account_number AS NVARCHAR) and s.spras = 'T'
	left join {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc on n.company = kuc.RobiKisaKod
	left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t on t.werks = n.business_area
	left join {{ ref('stg__dimensions_t_dim_averages4currencies') }} ac on ac.currency = n.budget_currency
																				and ac.[eomonth] = n.end_of_month