
{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}

select
    rls_key=CONCAT(t.rls_businessarea, '-', t.rls_company, '-', t.rls_group)
    ,t.rls_region 
    ,t.rls_group 
    ,t.rls_company 
    ,t.rls_businessarea 
    ,k.Tanim as company
    ,t.project_name as proje
      ,[cowid] as cowid
      ,[Company Code] as company_code
      ,[Document Category] as document_category
      ,[Document Type] as document_type
      ,[Payment Terms] as payment_terms
      ,[Purchasing Group] as purchasing_group
      ,[Purchasing Organization] as purchasing_organisation
      ,[Contract Group / Contract Unit] as contract_group_or_unit
      ,[Insurer] as insurer
      ,[Risk Address (Location)] as risk_address_location
      ,[Smart Code] as smart_code
      ,[Contract Type] as contract_type
      ,[Document Kind] as document_kind
      ,cast([Contract Amount 2] as float) as contract_amount_2
      ,[Contract Amount 2 Currency] as contract_amount_2_currency
      ,cast([Performance Bond Amount 1] as float) as performance_bond_amount_1
      ,[Performance Bond Amount 1 Currency] as performance_bond_amount_1_currency
      ,cast([Performance Bond Amount 2] as float) performance_bond_amount_2
      ,[Performance Bond Amount 2 Currency] performance_bond_amount_2_currency
      ,cast([Advance Payment Amount 1] as float) as advance_payment_amount_1
      ,[Advance Bond Amount 1 Currency] as advance_payment_amount_currency
      ,cast([Advance Payment Amount 2] as float) as advance_payment_amount_2
      ,[Advance Bond Amount 2 Currency] as advance_payment_amount_2_currency
      ,cast([Personnel Limit Number] as float) as personel_limit_number
      ,[Performance Bond Period (Month)] as performance_bond_period_month
      ,[Amendment - Phase] as amendment_phase
      ,cast([Amendment - Count] as float) as amendment_count
      ,[Duration of Work (Days)] as duration_of_work_in_days
      ,[Advance Payment Bond Period (Month)] as advance_payment_period_month
      ,cast([Performance Bond Rate (%)] as float) as period_bond_rate_percent
      ,cast([Advance Payment Deduction Rate (%)] as float) as advance_payment_deduction_rate_percent
      ,cast([Advance Payment Rate (%)] as float) as advance_payment_rate_percent
      ,[Unit Price Validity Date] as unit_price_validity_date
from {{ ref('stg__scm_kpi_t_fact_aribacontractdetails') }} m
	LEFT JOIN {{ ref('dm__dimensions_t_dim_projects') }} t on t.business_area = m.Plant
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} k on k.RobiKisaKod = m.[Company Code]