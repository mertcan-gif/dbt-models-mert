{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

/* Finansal P&L için aşağıdaki v0 versiyonuna bakınız */

select 
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea = CONCAT(acd.rbusa, '_', rls_region),
	rbukrs as company,
	rbusa as business_area,
	cast(acd.budat as date) as posting_date,
	acd.belnr as document_number,
	acd.buzei as line_item,
	acd.blart as document_type,
	acd.rcntr as cost_center,
	acd.fistl as financial_center,
    CASE    
        WHEN LEFT(acd.racct,2) IN ('60','61') THEN 'Revenue'
        WHEN LEFT(acd.racct,2) IN ('62') OR LEFT(acd.racct, 3) IN ('740') THEN 'Cost of Revenue'
        WHEN LEFT(acd.racct,3) IN ('770') THEN 'General Administrative Expenses'
        WHEN LEFT(acd.racct,3) IN ('642','646') THEN 'Financial Income'
        WHEN LEFT(acd.racct,3) IN ('656','780') THEN 'Financial Expense'
        WHEN LEFT(acd.racct,3) IN ('679') THEN 'Other Operating Income From Main Activities'
        WHEN LEFT(acd.racct,3) IN ('689') THEN 'Other Operating Expense From Main Activities'
		ELSE 'NOT CATEGORIZED'
    END AS financial_category,
	acd.racct as account_number,
	acc.[Ana Kategori] AS main_category,
	acc.[Hesap Kategorisi] AS account_group,
	left(racct,2) AS account_title,
	acc.[Hesap Kodu] AS account_code,
	acd.hsl as amount_in_try,
	acd.ksl as amount_in_eur,
	costcenter_category = COALESCE(fc.[cost_center_category],'NOT CATEGORIZED'),
	account_category = COALESCE(account_mapping.[account_category],'NOT CATEGORIZED'),
	account_main_category = COALESCE(account_mapping.[account_main_category],'NOT CATEGORIZED'),
	account_header = COALESCE(account_mapping.[account_header],'NOT CATEGORIZED'),
	is_adjustment = 0
from {{ ref('stg__s4hana_t_sap_acdoca') }} acd
	LEFT JOIN {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_dim_pnl_costcenter_category') }} fc on acd.rcntr = fc.cost_center_code
	LEFT JOIN {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_dim_pnl_main_account_category') }} account_mapping on acd.racct = cast(account_mapping.main_account as nvarchar)
	LEFT JOIN {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_dim_account_mapping') }} acc ON LEFT(acd.racct, 2) = acc.[Hesap Başlığı] 
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} comp ON acd.rbukrs = comp.RobiKisaKod 
WHERE 1=1
	AND rbukrs = 'RMG'
	--AND CAST(BUDAT AS date) >= '2025'

















/*
Finansal P&L versiyon 0
select 
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea = CONCAT(acd.rbusa, '_', rls_region),
	rbukrs as company,
	rbusa as business_area,
	cast(acd.budat as date) as posting_date,
	CASE	
		WHEN LEFT(acd.racct,2) IN ('60','61') THEN 'Revenue'
		WHEN LEFT(acd.racct,2) IN ('62') OR LEFT(acd.racct, 3) IN ('740') THEN 'Cost of Revenue'
		WHEN LEFT(acd.racct,3) IN ('770') THEN 'General Administrative Expenses'
		WHEN LEFT(acd.racct,3) IN ('642','646') THEN 'Financial Income'
		WHEN LEFT(acd.racct,3) IN ('656','780') THEN 'Financial Expense'
		WHEN LEFT(acd.racct,3) IN ('679') THEN 'Other Operating Income From Main Activities'
		WHEN LEFT(acd.racct,3) IN ('689') THEN 'Other Operating Expense From Main Activities'
	END AS financial_category,
	acd.racct as account_number,
	acd.hsl as amount_in_try,
	acd.ksl as amount_in_eur,
	fc.financial_center,
	cmmt.commitment_item
from {{ ref('stg__s4hana_t_sap_acdoca') }} acd
	LEFT JOIN {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_dim_financialcenter') }} fc on acd.fistl = fc.financial_center
	LEFT JOIN {{ source('stg_rmg_kpi', 'raw__rmg_kpi_t_dim_commitmentitem') }} cmmt on acd.fipex = cast(cmmt.commitment_item as nvarchar)
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} comp ON acd.rbukrs = comp.RobiKisaKod
WHERE 1=1
	AND rbukrs = 'RMG'
	AND LEFT(acd.racct,1) IN ('6','7')
	AND CAST(BUDAT AS date) >= '2025'

*/