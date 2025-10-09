{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','projectprogress_draft']
    )
}}

SELECT 
	dim.region AS rls_region
	,rls_group = CONCAT(COALESCE(dim.[group],''),'_',COALESCE(dim.region,'')) 
	,rls_company = CONCAT(COALESCE(dim.company,''),'_',COALESCE(dim.region,'')) 
	,rls_businessarea = CONCAT(COALESCE(dim.business_area,''),'_',COALESCE(dim.region,'')) 
	,dim.company
	,dim.project_id
	,cd.[sap_business_area]
	,cd.[project_name]
	,[time_progress] = CASE 
							WHEN COALESCE(cd.[total_duration],0) = 0 THEN 0 
							WHEN (cd.[days_passed] * 1.00 / cd.[total_duration]) > 1 THEN 1.00
							ELSE cd.[days_passed] * 1.00 / cd.[total_duration]
						END
	,[physical_progress] = CASE
								WHEN COALESCE(cd.total_mh,0) = 0 THEN 0
								ELSE cd.[earned_mh] / cd.[total_mh]
							END
	,kuc.KyribaGrup AS [group]
	,kuc.KyribaKisaKod AS kyriba_company_code
FROM {{ source('stg_to_kpi', 'raw__to_kpi_t_dim_consolidateddata') }} cd
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} dim ON dim.business_area = cd.sap_business_area
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON dim.company = kuc.RobiKisaKod
WHERE (project_id <> 'TO_9999' OR project_id IS NULL)
	AND category IS NOT NULL
	AND dim.company IS NOT NULL