{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH currencies AS (
    SELECT 
		YEAR(date_value) AS [year],
        FORMAT(date_value, 'MM') AS [month],
        try_value,
		usd_value,
		eur_value
    FROM 
        {{ ref('stg__dimensions_t_dim_dailys4currencies') }} c
    WHERE 1=1
	AND currency = 'TRY'
    AND date_value = (
            SELECT MAX(date_value)
            FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }}
            WHERE FORMAT(date_value, 'yyyy-MM') = FORMAT(c.date_value, 'yyyy-MM')
        )
)

SELECT 
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(p.WERKS, '_', rls_region),
	[company],
	[portfolio_id],
	[portfolio],
	t001w.name1 AS business_area_description,
	noi.[month],
	noi.[year],
	budget_try = [budget],
	budget_eur = [budget] * currencies.eur_value,
	budget_usd = [budget] * currencies.usd_value,
	actual_try = [realized],
	actual_eur = [realized] * currencies.eur_value,
	actual_usd = [realized] * currencies.usd_value,
	[type],
	noi.stake
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_noibudgetactual') }} noi
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON noi.portfolio_id = p.ID
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON p.WERKS = t001w.bwkey
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON p.WERKS = t001k.bwkey
LEFT JOIN currencies ON noi.[year] = currencies.[year] 
		AND noi.[month] = currencies.[month]
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod

