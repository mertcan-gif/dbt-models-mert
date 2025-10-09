{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
SELECT
	dim.region AS rls_region
	,CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) AS rls_group
	,CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) AS rls_company 
	,CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) AS rls_businessarea
	,dim.project_id
	,cd.[business_area]
    ,[tracking_number]
    ,[subcontractor]
    ,[project]
    ,[contract_number]
    ,[subject]
    ,[claim_ybf_topic]
    ,[type]
    ,[subcontractor_claim_try]
    ,[subcontractor_claim_usd]
    ,[subcontractor_claim_eur]
    ,[construction_site_expenses_try]
    ,[construction_site_expenses_usd]
    ,[construction_site_expenses_eur]
    ,[head_office_expenses_try]
    ,[head_office_expenses_usd]
    ,[head_office_expenses_eur]
    ,[confirmed_amount_try]
    ,[confirmed_amount_usd]
    ,[confirmed_amount_eur]
    ,cd.[status]
    ,[action]
    ,[year]
    ,[approved_total_amount_try]
    ,[claim_total_amount_try]
    ,[construction_site_total_amount_try]
    ,[head_office_total_amount_try]
    ,[claim_mto_scope]
    ,[claim_year]
    ,[order_rank]
FROM {{source('stg_to_kpi','raw__to_kpi_t_fact_claimdata')}} cd 
	LEFT JOIN {{source('stg_dimensions','raw__dwh_t_dim_project')}} dim ON dim.business_area = cd.business_area  


