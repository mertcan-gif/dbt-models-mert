{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
SELECT 
	sc.rls_region
	,sc.rls_group
	,sc.rls_company
	,sc.rls_businessarea
	,sc.[group]
	,sc.company
	,sc.business_area
	,t001w.name1 AS business_area_name
	,sc.vendor_code
	,sc.vendor_name
	,sc.fiscal_year
	,sc.document_no
	,sc.document_line_item
	,sc.general_ledger_account
	,sc.document_date
	,sc.eur_value
	,sc.usd_value
	,sc.try_value
	,hr.total_count AS headcount
	,sc.expense_category
	,sc.expense_sub_category
FROM {{ ref('stg__rmh_kpi_t_fact_servicecosts') }}   sc  
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }}  t001w ON sc.business_area = t001w.werks
LEFT JOIN {{ ref('stg__rmh_kpi_t_fact_companyheadcount') }} hr ON hr.year_month = FORMAT(sc.document_date, 'yyyy-MM') 
														AND hr.company = sc.company

