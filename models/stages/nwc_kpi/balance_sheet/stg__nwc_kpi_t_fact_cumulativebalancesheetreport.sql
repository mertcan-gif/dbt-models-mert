{{
  config(
    materialized = 'table',tags = ['nwc_kpi','balancesheet']
    )
}}

with RAW_DATA_from_DIM AS (

	SELECT
		mgd.company
		,mgd.business_area
		,mgd.customer_vendor_code
		,mgd.posting_year_month
		,mgd.bs_mapping
		,mgd.general_ledger_account
		,mgd.[source]
		,SUM(total_amount_in_company_currency) over (partition by mgd.company, mgd.business_area, mgd.customer_vendor_code, mgd.bs_mapping, mgd.general_ledger_account, mgd.[source] order by mgd.posting_year_month) as cumulative_total_in_company_currency
	FROM {{ ref('stg__nwc_kpi_t_dim_balancesheetdates') }} mgd
			LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_basebalancesheetreport') }} aa ON aa.business_area = mgd.business_area
									AND aa.company = mgd.company
									AND aa.customer_vendor_code = mgd.customer_vendor_code
									AND aa.posting_year_month = mgd.posting_year_month
									AND aa.bs_mapping = mgd.bs_mapping
									AND aa.general_ledger_account = mgd.general_ledger_account
									AND aa.[source] = mgd.[source]
)


select 
	company
	,business_area
	,business_area_description = t001w.name1
	,posting_year_month
	,bs_mapping
	,general_ledger_account
	,customer_vendor_code
	,running_total = cumulative_total_in_company_currency
	,currency
	,s4c.try_value
	,s4c.eur_value
	,[source]
from RAW_DATA_from_DIM s
	LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }}  t001 ON s.company = t001.BUKRS
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON s.business_area = t001w.werks
	LEFT JOIN {{ ref('stg__nwc_kpi_t_dim_monthlycurrencies') }} s4c ON s.posting_year_month= EOMONTH(CAST(CONCAT(s4c.year_month,'-01') AS DATE))
								AND t001.WAERS = s4c.currency
	
 
