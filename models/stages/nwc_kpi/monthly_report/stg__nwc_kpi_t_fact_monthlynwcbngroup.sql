{{
  config(
    materialized = 'table',tags = ['nwc_kpi','monthlyreport']
    )
}}

	select 
		rbukrs
		,business_area = rbusa
		,business_area_code = CONCAT('BNM_',CAST(DENSE_RANK() OVER(ORDER BY rbusa) AS NVARCHAR))  -- Proje adı ve kodu ayrı olarak iletilmediğinden iki kolon için de rbusa kullanılmıştır
		,bn.[year_month]
		,nwc_mapping 
		,total_amount
		,currency = 'EUR'
		,s4c.try_value
		,s4c.eur_value
	from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_bnnwcmonthlyreport') }} bn
		left join {{ ref('stg__nwc_kpi_t_dim_monthlycurrencies') }} s4c on FORMAT(CAST(bn.year_month + '-01' AS DATE), 'yyyy-MM') = FORMAT(CAST(s4c.year_month + '-01' AS DATE), 'yyyy-MM')
								and  s4c.currency = 'EUR'