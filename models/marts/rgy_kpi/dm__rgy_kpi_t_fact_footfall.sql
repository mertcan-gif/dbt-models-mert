{{
  config(
    materialized = 'table',tags = ['rgy_kpi']
    )
}}

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(p.werks, '_', rls_region),
	p.[name] as portfolio_name,
	[Count] as footfall,
	[date],
	CASE 
		WHEN DATENAME(WEEKDAY, [date]) IN ('Saturday', 'Sunday') THEN 'Weekend'
		ELSE 'Weekday'
	END AS day_information
FROM {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_fact_footfall') }} f
LEFT JOIN {{ source('stg_rgy_kpi', 'raw__rgy_kpi_t_dim_portfolio') }} p ON f.portfolioid = p.id
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON p.werks = t001k.bwkey
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON t001k.bukrs = dim_comp.RobiKisaKod
