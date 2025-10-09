{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT('_', rls_region),
	bukrs,
	budget.*
FROM {{ ref('stg__rmg_kpi_t_fact_equipmentbudget') }} budget
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfctr') }} fmfctr ON budget.financial_center_code = fmfctr.fictr
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON fmfctr.bukrs = dim_comp.RobiKisaKod