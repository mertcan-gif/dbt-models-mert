{{
  config(
    materialized = 'table',tags = ['rmore','test_rmore']

    )
}}
	SELECT 
		rls_region = NULL
		,rls_group = NULL
		,rls_company = NULL
		,rls_businessarea = NULL
		,*
	FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_job_applications') }}

