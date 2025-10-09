{{
  config(
    materialized = 'table',tags = ['nwc_kpi','monthlyreport','datedimtest']
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
		,date_dim.rbukrs
		,date_dim.business_area_code
		,date_dim.nwc_mapping
	FROM (SELECT DISTINCT  
				posting_year_month = dt.end_of_month 
				,business_area_code 
				,rbukrs
				,nwc_mapping
				from {{ ref('stg__nwc_kpi_t_fact_basemonthlyreport') }}
				CROSS JOIN DATE_TABLE dt) date_dim
			LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_basemonthlyreport') }} rb ON rb.business_area_code = date_dim.business_area_code 
															AND rb.rbukrs = date_dim.rbukrs
															AND date_dim.posting_year_month =  rb.posting_year_month
															AND date_dim.nwc_mapping = rb.nwc_mapping
)

SELECT * FROM DIMENSION_WITH_ALL_DATES

