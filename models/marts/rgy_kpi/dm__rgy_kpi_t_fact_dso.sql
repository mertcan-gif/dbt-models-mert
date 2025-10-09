{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH main_cte AS(
	SELECT 
		BukrsClr AS company,
		IsAlani AS business_area,
		MaliYil AS fiscal_year,
		CAST(TahsilatTarihi AS date) AS collection_date,
		SUM(CAST(Tahsilat AS float)) OVER(PARTITION BY MaliYil, BukrsClr ) AS collection_amount,
		SUM(CAST(AgirlikliToplam AS float)) OVER(PARTITION BY MaliYil, BukrsClr ) AS weighted_sum
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_dayssalesoutstanding') }}
)
SELECT 
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(business_area, '_', rls_region),
	main_cte.*,
	days_sales_outstanding = weighted_sum / collection_amount
FROM main_cte
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON main_cte.company = dim_comp.RobiKisaKod
