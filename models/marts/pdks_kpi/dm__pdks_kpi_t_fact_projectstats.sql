{{
  config(
    materialized = 'table',tags = ['pdks_kpi']
    )
}}

	SELECT
		  (SELECT [region] 
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
			WHERE p.project_id = ps.project_id) AS rls_region	
		  ,(SELECT CONCAT(COALESCE([group],''),'_',COALESCE([region],'')) 
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
			WHERE p.project_id = ps.project_id) AS rls_group
		  ,(SELECT CONCAT(COALESCE(company,''),'_',COALESCE([region],''))
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
			WHERE p.project_id = ps.project_id) AS rls_company
		  ,(SELECT CONCAT(COALESCE(business_area,''),'_',COALESCE([region],'')) 
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
			WHERE p.project_id = ps.project_id) AS rls_businessarea
		  ,(SELECT project_name 
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
			WHERE p.project_id = ps.project_id) AS project_name
		  ,[calculation_date]
		  ,ps.project_id
		  ,ps.[average_time_hour]
		  ,ps.[total_time_hour]
		  ,ps.[uniq_personal_count]
	FROM {{ source('stg_pdks_kpi', 'raw__pdks_kpi_t_fact_projectstats') }} ps


UNION ALL 

	SELECT * FROM aws_stage.pdks_kpi.stg__pdks_kpi_t_fact_projectstats_rmoresample