{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
/*
cari tutar ve tatil sepeti tutarı için ayrı olarak kur dönüşümü için 2 tane join atılmasının sebebi 
cari fiyattaki kur bilgisi ile tatil sepetindeki kur bilgisinin farklı olduğu durumdan kaynaklıdır.
*/


SELECT 
	rls_region = cm.RegionCode
	,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
	,rls_company = cm.RobiKisaKod + '_' + cm.RegionCode
	,rls_businessarea = '_' + cm.RegionCode
	,TRIM([city]) AS city
	,TRIM([hotel]) AS hotel
	--,CAST([contract_price] AS money) AS contract_price
	--,TRIM([contract_price_currency]) AS contract_price_currency
	,CAST([contract_price] AS money) * curr_cp.try_value AS contract_price_try
	,CAST([contract_price] AS money) * curr_cp.usd_value  AS contract_price_usd
	,CAST([contract_price] AS money) * curr_cp.eur_value AS contract_price_eur
	--,CAST([tatilsepeti_price] AS money) as tatilsepeti_price
	--,TRIM([tatilsepeti_currency]) AS tatilsepeti_currency
	,CAST([tatilsepeti_price] AS money) * curr_ts.try_value AS tatilsepeti_price_try
	,CAST([tatilsepeti_price] AS money) * curr_ts.usd_value  AS tatilsepeti_price_usd
	,CAST([tatilsepeti_price] AS money) * curr_ts.eur_value AS tatilsepeti_price_eur
	,TRY_CAST([current_price_start_date] AS date) AS current_price_start_date
	,TRY_CAST([currrent_price_end_date] AS date) AS currrent_price_end_date
	,TRIM([detail]) AS detail
	,CAST(transaction_date AS date) AS transaction_date
FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_hotelprices') }} hp
LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cm ON 'RMH' = cm.RobiKisaKod
LEFT JOIN {{ ref('stg__dimensions_t_dim_dailys4currencies') }} curr_cp ON curr_cp.date_value = hp.transaction_date
																				AND curr_cp.currency = hp.contract_price_currency
LEFT JOIN {{ ref('stg__dimensions_t_dim_dailys4currencies') }} curr_ts ON curr_ts.date_value = hp.transaction_date
																				AND curr_ts.currency = hp.tatilsepeti_currency