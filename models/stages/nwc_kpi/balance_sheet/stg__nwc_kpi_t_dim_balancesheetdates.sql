{{
  config(
    materialized = 'table',tags = ['nwc_kpi','balancesheet']
    )
}}

	
WITH DATE_TABLE as
(
	SELECT DISTINCT
		EOMONTH(DATEADD(MONTH, number, '2022-12-31')) AS end_of_month
	FROM 
		master..spt_values
	WHERE 
		type = 'P' 
		AND DATEADD(MONTH, number, '2023-01-01') <= GETDATE()
)
,
DIMENSION_WITH_ALL_DATES AS (
	SELECT DISTINCT  
		date_dim.posting_year_month 
		,date_dim.company
		,date_dim.business_area
		,date_dim.customer_vendor_code
		,date_dim.bs_mapping
		,date_dim.general_ledger_account
		,date_dim.[source]
	FROM (SELECT DISTINCT  
				posting_year_month = dt.end_of_month 
				,business_area 
				,company
				,customer_vendor_code
				,bs_mapping
				,general_ledger_account
				,[source]
				from {{ ref('stg__nwc_kpi_t_fact_basebalancesheetreport') }}
				CROSS JOIN DATE_TABLE dt) date_dim
			LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_basebalancesheetreport') }} rb ON rb.business_area = date_dim.business_area
															AND rb.company = date_dim.company
															AND date_dim.customer_vendor_code = rb.customer_vendor_code
															AND date_dim.posting_year_month =  rb.posting_year_month
															AND date_dim.bs_mapping = rb.bs_mapping
															AND date_dim.general_ledger_account = rb.general_ledger_account
															AND date_dim.[source] = rb.[source]
)

SELECT * FROM DIMENSION_WITH_ALL_DATES

