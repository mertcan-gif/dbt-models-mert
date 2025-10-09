{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}

SELECT 
	dim.[region] AS rls_region
	,[rls_group] = CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) 
	,[rls_company] = CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) 
	,[rls_businessarea] = CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) 
	,dim.[project_id]	
	,pp.[business_area]
	,pp.[project_name]
	,[number_of_contracts]
	,[definite_progress_payment_completed]
	,[definite_progress_payment_not_completed]
	,[firm_contract_amount_try]
	,[firm_contract_amount_usd]
	,[firm_contract_amount_eur]
	,[firm_contract_amount_gbp]
	,[initial_approved_progress_payment_try]
	,[initial_approved_progress_payment_usd]
	,[initial_approved_progress_payment_eur]
	,[initial_approved_progress_payment_gbp]
	,[contract_progress_percentage_try] = CASE
											  WHEN [firm_contract_amount_try] = 0 OR [firm_contract_amount_try] IS NULL THEN 0
											  ELSE [initial_approved_progress_payment_try] / [firm_contract_amount_try]
										  END
	,[contract_progress_percentage_usd] = CASE
											  WHEN [firm_contract_amount_usd] = 0 OR [firm_contract_amount_usd] IS NULL THEN 0
											  ELSE [initial_approved_progress_payment_usd] / [firm_contract_amount_usd]
										  END
	,[contract_progress_percentage_eur] = CASE
											  WHEN [firm_contract_amount_eur] = 0 OR [firm_contract_amount_eur] IS NULL THEN 0
											  ELSE [initial_approved_progress_payment_eur] / [firm_contract_amount_eur]
										  END
	,[order_rank]
  FROM {{source('stg_to_kpi','raw__to_kpi_t_dim_progresspayment')}} pp  
  	LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON dim.business_area = pp.business_area  
-- WHERE project_id <> 'TO_9999' OR project_id IS NULL





