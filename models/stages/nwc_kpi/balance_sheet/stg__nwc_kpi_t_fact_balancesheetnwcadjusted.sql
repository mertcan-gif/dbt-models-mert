{{
  config(
    materialized = 'table',tags = ['nwc_kpi','balancesheet']
    )
}}

	select 	
		s.rbukrs
		,s.name1
		,s.rbusa
		,[year_month] = CONCAT(s.[year],'-',s.[month])
		,s.nwc_mapping
		,[value] = COALESCE(CAST(s.[value] AS MONEY),0)
		,currency = cur.WAERS
		,s4c.try_value
		,s4c.eur_value
	from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_rtigroupnwc') }} s
		left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} cur on s.rbukrs = cur.BUKRS
		left join {{ ref('stg__nwc_kpi_t_dim_monthlycurrencies') }} s4c on CONCAT(s.[year],'-',s.[month]) = s4c.year_month
									and cur.WAERS = s4c.currency 

union all

	select 	
		s.rbukrs
		,s.name1
		,s.rbusa
		,[year_month] = CONCAT(s.[year],'-',s.[month])
		,s.nwc_mapping
		,[value] = COALESCE(s.[value],0)
		,currency = cur.WAERS
		,s4c.try_value
		,s4c.eur_value
	from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_fact_nwcadjustments') }} s
		left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} cur on s.rbukrs = cur.BUKRS
		left join {{ ref('stg__nwc_kpi_t_dim_monthlycurrencies') }} s4c on CONCAT(s.[year],'-',s.[month]) = s4c.year_month
									and cur.WAERS = s4c.currency 

							