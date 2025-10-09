
{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','deposit_cockpit_draft']
    )
}}

SELECT 	
  [rls_region] = (select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE dc.company = kuc.RobiKisaKod )
	,[rls_group] = 
		CONCAT(
			COALESCE((select top 1 KyribaGrup from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE dc.company = kuc.RobiKisaKod ),'')
			,'_'
			,COALESCE((select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE dc.company = kuc.RobiKisaKod ),'')
			)
	,[rls_company] =
		CONCAT(
			COALESCE(company,'')
			,'_'
			,COALESCE((select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE dc.company = kuc.RobiKisaKod ),'')
			)
	,[rls_businessarea] = CONCAT('_',(select top 1 RegionCode from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc WHERE dc.company = kuc.RobiKisaKod ))
  ,dc.company
  ,dc.currency
  ,dc.racct
  ,dc.txtRate AS rate
  ,dc.txtMarketRate as market_rate
  ,dc.txtBalance AS balance
  ,dc.txtBalanceEur AS balance_eur
  ,dc.calculated_balance
  ,dc.calculated_balance_eur
  ,dc.start_date
  ,dc.end_date
  ,dc.date_diff
  ,dc.date_interval
  ,dc.bank
  ,dc.eur_value
  ,dc.is_foreign
  ,dc.vade_durumu AS deposit_status
  ,dc.source
  ,dc.deposit_demand_group
  ,kuc.KyribaGrup AS [group]
  ,kuc.KyribaKisaKod AS [kyriba_company_code]
  ,txt_rate_normal = CASE
						WHEN CAST(txtRate as money) = '0.01' THEN NULL
						ELSE CAST(txtRate as money) * 0.01
					END
  ,txt_market_rate_normal = CASE
						WHEN CAST(txtMarketRate as money) = '0.01' THEN NULL
						ELSE CAST(txtMarketRate as money) * 0.01
					END

FROM {{ ref('stg__nwc_kpi_t_fact_depositcockpit') }} dc
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON dc.company = kuc.RobiKisaKod 
WHERE 1=1
	AND (kuc.Durum <> 'Pasif' OR kuc.Durum IS NULL)
	-- AND NOT (date_interval IS NULL AND vade_durumu = 'Vadeli')