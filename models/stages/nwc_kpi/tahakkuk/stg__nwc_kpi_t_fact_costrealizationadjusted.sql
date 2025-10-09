
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','tahakkuk']
    )
}}

SELECT 
	[rls_region]   = kuc.RegionCode 
	,[rls_group]   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,[rls_company] = CONCAT(COALESCE(company  ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,[rls_businessarea] = CONCAT(COALESCE(business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,Final.*
FROM (
	SELECT *
	FROM {{ ref('stg__nwc_kpi_t_fact_costrealizationrearranged') }}
	UNION ALL
	SELECT *
		,vendor_code = ''
		,document_header_text = ''
		,functional_area = ''
		,functional_area_text = ''
		,funds_center = ''
		,material_number = ''
		,material_description = ''
		,contract_number = ''
		,sas_short_text = ''
		,purchasing_document = ''
		,sas_amount = ''
		,unit = ''
		,net_price = ''
		,warehouse_document_number = ''
		,warehouse_amount = ''
		,warehouse_unit = ''
		,warehouse_material_description = ''
	FROM {{ ref('stg__nwc_kpi_v_fact_costrealizationadjustments') }}
	
) Final
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON Final.company = kuc.RobiKisaKod 
