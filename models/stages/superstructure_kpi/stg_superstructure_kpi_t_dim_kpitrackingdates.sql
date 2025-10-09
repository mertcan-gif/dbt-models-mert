{{
  config(
    materialized = 'table',tags = ['superstructure_kpi']
    )
}}	


WITH DATE_TABLE as
(
	SELECT DISTINCT
		EOMONTH(DATEADD(MONTH, number, '2020-12-31')) AS end_of_month
	FROM 
		master..spt_values
	WHERE 
		type = 'P' 
		AND DATEADD(MONTH, number, '2021-01-01') <= GETDATE()
),

DIMENSION_WITH_ALL_DATES AS (
	SELECT DISTINCT  
		date_dim.posting_year_month 
		,date_dim.business_area
		,date_dim.business_area_description
		,date_dim.type
	FROM (SELECT DISTINCT  
				posting_year_month = dt.end_of_month 
				,business_area
                ,business_area_description
				,type
				from {{ ref('stg_superstructure_kpi_t_fact_kpitrackingbase') }}
				CROSS JOIN DATE_TABLE dt) date_dim
			LEFT JOIN {{ ref('stg_superstructure_kpi_t_fact_kpitrackingbase') }} rb ON rb.business_area = date_dim.business_area 
															AND date_dim.posting_year_month =  rb.posting_date
															AND date_dim.type = rb.type
)


SELECT * FROM DIMENSION_WITH_ALL_DATES









