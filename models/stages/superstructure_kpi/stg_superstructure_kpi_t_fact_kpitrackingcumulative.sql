{{
  config(
    materialized = 'table',tags = ['superstructure_kpi']
    )
}}	


with RAW_DATA_FROM_DIM AS (

	SELECT
		mgd.business_area
		,mgd.business_area_description
		,mgd.posting_year_month
		,mgd.type
		,SUM(amount_in_eur) over (partition by mgd.business_area, mgd.type order by mgd.posting_year_month) as cumulative_amount_in_eur
		,amount_in_eur
	FROM {{ ref('stg_superstructure_kpi_t_dim_kpitrackingdates') }} mgd
			LEFT JOIN {{ ref('stg_superstructure_kpi_t_fact_kpitrackingbase') }} aa ON aa.business_area = mgd.business_area
									AND aa.posting_date = mgd.posting_year_month
									AND aa.type = mgd.type
)

select 
	business_area
	,business_area_description
	,posting_year_month
	,type
	,cumulative_amount_in_eur
	,amount_in_eur
from RAW_DATA_FROM_DIM s
