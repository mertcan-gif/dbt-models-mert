{{
  config(
    materialized = 'table',tags = ['nwc_kpi','duetoduefrom']
    )
}}
	

WITH PIVOTTED_DATA AS (

	SELECT company, RBUSA, PostingDate, budget_currency,
		MAX(CASE WHEN AnaHesap = 'kümülatif_maliyet' THEN cumulative_total_hsl END) AS cumulative_cost_try,
		MAX(CASE WHEN AnaHesap = 'kümülatif_maliyet' THEN cumulative_total_ksl END) AS cumulative_cost_eur,
		MAX(CASE WHEN AnaHesap = 'kümülatif_maliyet' THEN cumulative_total_osl END) AS cumulative_cost_usd,
		MAX(CASE WHEN AnaHesap = 'kümülatif_gelir' THEN cumulative_total_hsl END) AS cumulative_revenue_try,
		MAX(CASE WHEN AnaHesap = 'kümülatif_gelir' THEN cumulative_total_ksl END) AS cumulative_revenue_eur,
		MAX(CASE WHEN AnaHesap = 'kümülatif_gelir' THEN cumulative_total_osl END) AS cumulative_revenue_usd
	FROM {{ ref('stg__nwc_kpi_t_fact_duetoduefromcumulativecalculation') }}
	GROUP BY company, RBUSA, PostingDate, budget_currency
)

,SUMMARIZED_DATA AS
(
	SELECT
		bv.bukrs
		,bv.gsber
		,starting_date_of_month = PostingDate
		,cumulative_cost_try = COALESCE(cumulative_cost_try,0)
		,cumulative_cost_eur = COALESCE(cumulative_cost_eur,0)
		,cumulative_cost_usd = COALESCE(cumulative_cost_usd,0)
		,cumulative_revenue_try = COALESCE(cumulative_revenue_try,0) * -1
		,cumulative_revenue_eur = COALESCE(cumulative_revenue_eur,0) * -1
		,cumulative_revenue_usd = COALESCE(cumulative_revenue_usd,0) * -1
		,butce_geliri = bv.revenue
		,butce_maliyet = CASE WHEN bv.fixed_budget = 1 THEN bv.revenue ELSE bv.cost END
		,butce_doviz_cinsi = bv.pb
	FROM PIVOTTED_DATA s4
		RIGHT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} bv ON s4.RBUSA = bv.gsber
																AND s4.company = bv.bukrs
																AND MONTH(s4.PostingDate) =  bv.budget_month
																AND YEAR(s4.PostingDate) =  bv.budget_year
	WHERE 1=1 
		AND RBUSA IS NOT NULL
		AND bv.revenue <> 0
)


SELECT 
	rls_region   = kuc.RegionCode 
	,rls_group   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,rls_company = CONCAT(COALESCE(bukrs collate database_default ,''),'_',COALESCE(kuc.RegionCode,''),'')
	,rls_businessarea = CONCAT(COALESCE(gsber,''),'_',COALESCE(kuc.RegionCode,''))
	,BUSINESS_AREA = ba.NAME1 
	,currency = butce_doviz_cinsi
	,SUMMARIZED_DATA.bukrs
	,gsber
	,starting_date_of_month = CAST(CONCAT(YEAR(starting_date_of_month),'-',MONTH(starting_date_of_month),'-','01') AS DATE)
	,kümülatif_maliyet = CASE
							WHEN butce_doviz_cinsi = 'EUR' THEN cumulative_cost_eur
							WHEN butce_doviz_cinsi = 'USD' THEN cumulative_cost_usd
							ELSE cumulative_cost_try
						END
	,kümülatif_gelir = CASE
							WHEN butce_doviz_cinsi = 'EUR' THEN cumulative_revenue_eur
							WHEN butce_doviz_cinsi = 'USD' THEN cumulative_revenue_usd
							ELSE cumulative_revenue_try
						END
	,butce_geliri
	,butce_maliyet
	,butce_doviz_cinsi
	,gerceklesme_oranı = CASE
							WHEN butce_doviz_cinsi = 'EUR' THEN  (cumulative_cost_eur)/(butce_maliyet)
							WHEN butce_doviz_cinsi = 'USD' THEN  (cumulative_cost_usd)/(butce_maliyet)
							ELSE (cumulative_cost_try)/(butce_maliyet)
						END
	,gerceklesmesi_gereken_gelir = CASE
										WHEN butce_doviz_cinsi = 'EUR' AND cumulative_cost_eur/butce_maliyet < 1  THEN  butce_geliri * (cumulative_cost_eur)/(butce_maliyet)
										WHEN butce_doviz_cinsi = 'EUR' AND cumulative_cost_eur/butce_maliyet >= 1  THEN  butce_geliri
										WHEN butce_doviz_cinsi = 'USD' AND cumulative_cost_usd/butce_maliyet < 1  THEN  butce_geliri * (cumulative_cost_usd)/(butce_maliyet)
										WHEN butce_doviz_cinsi = 'USD' AND cumulative_cost_usd/butce_maliyet >= 1  THEN  butce_geliri 
										WHEN cumulative_cost_try/butce_maliyet < 1 THEN butce_geliri * (cumulative_cost_try)/(butce_maliyet)
										WHEN cumulative_cost_try/butce_maliyet >= 1 THEN butce_geliri 
									END
	,due = CASE 	
				WHEN butce_doviz_cinsi = 'EUR' THEN  (butce_geliri * (cumulative_cost_eur)/(butce_maliyet)) - (cumulative_revenue_eur)
				WHEN butce_doviz_cinsi = 'USD' THEN  (butce_geliri * (cumulative_cost_usd)/(butce_maliyet)) - (cumulative_revenue_usd)
				ELSE  (butce_geliri * (cumulative_cost_try)/(butce_maliyet)) - (cumulative_revenue_try)
			END
	,due_type = CASE 
					WHEN butce_doviz_cinsi = 'EUR' AND (butce_geliri * (cumulative_cost_eur)/(butce_maliyet)) - (cumulative_revenue_eur) > 0 THEN 'Due from Clients Under Construction Contracts'
					WHEN butce_doviz_cinsi = 'USD' AND (butce_geliri * (cumulative_cost_usd)/(butce_maliyet)) - (cumulative_revenue_usd) > 0 THEN 'Due from Clients Under Construction Contracts'
					WHEN butce_doviz_cinsi = 'TRY' AND (butce_geliri * (cumulative_cost_try)/(butce_maliyet)) - (cumulative_revenue_try) > 0 THEN 'Due from Clients Under Construction Contracts'
					ELSE 'Due to Customers Under Construction Contracts'
				END
	,s4c.try_value
	,s4c.eur_value
	,kuc.KyribaGrup
	,kuc.KyribaKisaKod
FROM SUMMARIZED_DATA
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} ba ON SUMMARIZED_DATA.GSBER = ba.WERKS collate database_default
	LEFT JOIN {{ ref('stg__nwc_kpi_t_dim_s4currencies') }} s4c ON LEFT(SUMMARIZED_DATA.starting_date_of_month,7) = s4c.year_month
								AND SUMMARIZED_DATA.butce_doviz_cinsi = s4c.currency collate database_default
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON SUMMARIZED_DATA.BUKRS = kuc.RobiKisaKod 