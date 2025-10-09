{{
  config(
    materialized = 'table',tags = ['nwc_kpi','balance_reconciliation']
    )
}}

WITH FINAL_OUTPUT AS (
  SELECT 
    [group]
    ,[company]
    ,counterparty_group
    ,counterparty_company
    ,general_ledger_account
    ,general_ledger_description
    ,vendor_code
    ,customer_code
    ,document_number
    ,clearing_document_number
    ,document_line_item
    ,fiscal_year
    ,posting_date
    ,document_date
    ,main_account
    ,document_currency
    ,company_currency
    ,amount_in_company_currency
    ,amount_in_eur
    ,amount_in_document_currency
    ,item_text
    ,kyriba_group
    ,kyriba_company_code
    ,main_opposite_flag = 'Main' 
  FROM {{ ref('stg__nwc_kpi_t_fact_intercompanyreconciliationdetails') }} b 

  UNION ALL

  SELECT 
    counterparty_group
    ,counterparty_company
    ,[group]
    ,[company]
    ,general_ledger_account
    ,general_ledger_description
    ,vendor_code
    ,customer_code
    ,document_number
    ,clearing_document_number
    ,document_line_item
    ,fiscal_year
    ,posting_date
    ,document_date
    ,main_account
    ,document_currency
    ,company_currency
    ,amount_in_company_currency
    ,amount_in_eur
    ,amount_in_document_currency
    ,item_text
    ,kyriba_group
    ,kyriba_company_code
    ,main_opposite_flag = 'Opposite' 
  FROM {{ ref('stg__nwc_kpi_t_fact_intercompanyreconciliationdetails') }} b 

)

SELECT
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(COMPANY collate database_default ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT('_',kuc.RegionCode)
	,F.*
FROM FINAL_OUTPUT F
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON F.company = kuc.RobiKisaKod
  LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc2 ON F.counterparty_company = kuc2.RobiKisaKod
WHERE (kuc.IsNWC = 1 AND kuc.IsNWC = 1)

