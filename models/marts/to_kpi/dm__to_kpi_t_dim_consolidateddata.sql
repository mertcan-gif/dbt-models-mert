{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
SELECT 
	dim.region AS rls_region
	,rls_group = CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) 
	,rls_company = CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) 
	,rls_businessarea = CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) 
	,dim.project_id
	,dim.company
	,cd.[sap_business_area]
	,cd.[status]
	,cd.[category]
	,cd.[project_name]
	,cd.[project_code]
	,cd.[gba_project_based]
	,cd.[gba_contract_based]
	,cd.[planning_code]
	,cd.[planning_code_mh]
	,cd.[contract_start_date]
	,cd.[contract_end_date]
	,cd.[days_passed]
	,cd.[total_duration]
	,[time_progress] = CASE 
							WHEN COALESCE(cd.[total_duration],0) = 0 THEN 0 
							WHEN (cd.[days_passed] * 1.00 / cd.[total_duration]) > 1 THEN 1.00
							ELSE cd.[days_passed] * 1.00 / cd.[total_duration]
						END
	,[physical_progress] = CASE
								WHEN COALESCE(cd.total_mh,0) = 0 THEN 0
								ELSE cd.[earned_mh] / cd.[total_mh]
							END
	,cd.[total_mh]
	,cd.[earned_mh]
	,cd.[planned_mh]
	,[actual_mh] = CASE
						WHEN COALESCE(cd.[cpi],0) = 0 THEN 0 
						ELSE cd.[earned_mh] / cd.[cpi]
					END
	,cd.[cpi]
	,[spi] = CASE 
				WHEN COALESCE(cd.[planned_mh],0) = 0 THEN 0 
				ELSE cd.[earned_mh] / cd.[planned_mh]
			END
	,cd.[contract_value]
	,cd.[currency]
	,cd.[issued_invoice_advance_payment_tax_excluded]
	,cd.[first_collection_term]
	,cd.[last_collection_term]
	,cd.[poc_this_year]
	,cd.[poc_next_year]
	,cd.[poc_two_years_later]
	,cd.[poc_three_years_later]
	,cd.[realized_last_year]
	,cd.[usd_tl_currency_rate]
	,cd.[eur_tl_currency_rate]
	,cd.[cost_tax_excluded]
	,cd.[income_tax_excluded]
	,cd.[income_tax_excluded_paid]
	,[order_rank] = cd.[rank]
FROM {{source('stg_to_kpi','raw__to_kpi_t_dim_consolidateddata')}} cd 
	LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON dim.business_area = cd.sap_business_area  
-- WHERE (project_id <> 'TO_9999' OR project_id IS NULL)
WHERE 1=1
	AND category IS NOT NULL




