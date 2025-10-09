{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
SELECT 
	[rls_region]
	,[rls_group]
	,[rls_company]
	,[rls_businessarea]
	,[company]
	,[businessarea]
	,[businessarea_name]
	,[deduction_id]
	,[material_no]
	,[material_name]
	,[quantity_type]
	,[service_quantity]
	,[service_amount]
	,[currency]
	,[document_date]
FROM {{ ref('stg__to_kpi_t_fact_servicepayment') }}