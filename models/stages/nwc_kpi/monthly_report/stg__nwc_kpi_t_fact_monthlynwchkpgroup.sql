{{
  config(
    materialized = 'table',tags = ['nwc_kpi','monthlyreport']
    )
}}

	select 
		rbukrs = CASE 
					WHEN rbukrs  = 'HTK' THEN 'NS_HKP02'
					ELSE rbukrs  
				END
		,business_area = rbusa 
		,business_area_code = rbusa   -- Proje adı ve kodu ayrı olarak iletilmediğinden iki kolon için de rbusa kullanılmıştır
		,hkp.[year_month]
		,nwc_mapping 
		,total_amount
		,currency = 'EUR'
		,s4c.try_value
		,s4c.eur_value
	from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_hkpnwcmonthlyreport') }} hkp
		left join {{ ref('stg__nwc_kpi_t_dim_monthlycurrencies') }} s4c on FORMAT(CAST(hkp.year_month + '-01' AS DATE), 'yyyy-MM') = FORMAT(CAST(s4c.year_month + '-01' AS DATE), 'yyyy-MM')
								and  s4c.currency = 'EUR'