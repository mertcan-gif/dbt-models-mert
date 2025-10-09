{{
  config(
    materialized = 'table',tags = ['nwc_kpi','monthlyreport']
    )
}}

with RAW_DATA_from_DIM AS (

	SELECT
		mgd.rbukrs
		,mgd.business_area_code
		,mgd.posting_year_month
		,mgd.nwc_mapping
		,SUM(sum_hsl) over (partition by mgd.rbukrs, mgd.business_area_code, mgd.nwc_mapping order by mgd.posting_year_month) as cumulative_total_hsl
	FROM {{ ref('stg__nwc_kpi_t_dim_monthlyreportdates') }} mgd
			LEFT JOIN {{ ref('stg__nwc_kpi_t_fact_basemonthlyreport') }} aa ON aa.business_area_code = mgd.business_area_code
									AND aa.rbukrs = mgd.rbukrs
									AND aa.posting_year_month = mgd.posting_year_month
									AND aa.nwc_mapping = mgd.nwc_mapping
)


select 
	rbukrs
	,business_area = t001w.name1
	,business_area_code
	,posting_year_month
	,nwc_mapping
	,running_total = cumulative_total_hsl
	,currency
	,s4c.try_value
	,s4c.eur_value
from RAW_DATA_from_DIM s
	LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }}  t001 ON s.RBUKRS = t001.BUKRS
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON s.business_area_code = t001w.werks
	LEFT JOIN {{ ref('stg__nwc_kpi_t_dim_monthlycurrencies') }} s4c ON s.posting_year_month= EOMONTH(CAST(CONCAT(s4c.year_month,'-01') AS DATE))
								AND t001.WAERS = s4c.currency
	
 
