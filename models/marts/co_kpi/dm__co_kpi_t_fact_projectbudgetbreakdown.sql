{{
  config(
    materialized = 'table',tags = ['co_kpi']
    )
}}

SELECT
	dim_comp.rls_region
	,dim_comp.rls_group
	,dim_comp.rls_company
	,rls_businessarea = CONCAT(l.fm_fund , '_' , dim_comp.rls_region) 
	,[company] = bukrs
	,[business_area] = fm_fund
	,[level] = seviye
	,[period] = CONCAT(gjahr,'-',buku_version) 
	,[budget_code] = butce_kodu
	,[budget_description] = butce_tanimi
	,[budget_amount_in_eur] = top_butce
	,[budget_currency] = top_butce_waers
	,[project_amount] = proje_tutar
	,[project_currency] = proje_waers
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_log_v2') }}  l
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON l.bukrs = dim_comp.RobiKisaKod
WHERE 1=1
	AND seviye = '1'
