{{
  config(
    materialized = 'table',tags = ['nwc_kpi','monthlyreport']
    )
}}

	select 
		rbukrs 
		,rbusa 
		,business_area_code 
		,ret.[year_month]
		,NWC_Mapping 
		,total_amount
		,currency = 'EUR'
		,s4c.try_value
		,s4c.eur_value
	from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_retnwcmonthlyreport') }} ret
		left join {{ ref('stg__nwc_kpi_t_dim_monthlycurrencies') }} s4c on FORMAT(CAST(ret.year_month + '-01' AS DATE), 'yyyy-MM') = FORMAT(CAST(s4c.year_month + '-01' AS DATE), 'yyyy-MM')
								and  s4c.currency = 'EUR'
