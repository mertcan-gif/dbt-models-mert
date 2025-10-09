{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

WITH currencies AS (
    SELECT 
		YEAR(date_value) AS [year],
        MONTH(date_value) AS [month],
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
            WHERE FORMAT(date_value, 'yyyy-MM') = FORMAT(c.date_value, 'yyyy-MM'))
)

SELECT
	  rls_region,
	  rls_group,
	  rls_company,
	  rls_businessarea = CONCAT(p.WERKS, '_', rls_region),
	  [sirket] AS [company],
      [portfoy] AS portfolio_name,
	  t001w.name1 AS business_area_description,
      [bolge] AS region,
      [ay] AS [month],
      [yil] AS [year],
      [fiili] AS realized_try,
	  realized_eur = ad.fiili * currencies.eur_value,
	  realized_usd = ad.fiili * currencies.usd_value,
      [butce] AS budget_try
  FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_adbudgetactual') }} ad
  LEFT JOIN currencies ON ad.[yil] = currencies.[year] AND ad.[ay]  = currencies.[month]
  LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON ad.portfoy_id = p.id
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON p.WERKS = t001w.bwkey
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON ad.sirket = dim_comp.RobiKisaKod