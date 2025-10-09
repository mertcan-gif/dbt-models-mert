{{
  config(
    materialized = 'table',tags = ['pdks_kpi']
    )
}}

SELECT 
	   [rls_region] = 
			(SELECT
				[region]
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
				WHERE project_id =
				((SELECT project_id 
				FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
				WHERE p.project_id = pt.project_id)))
	   ,[rls_group] = 
			(SELECT
				CONCAT(COALESCE([group],''),'_',COALESCE([region],'')) 
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
				WHERE project_id =
				((SELECT project_id 
				FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
				WHERE p.project_id = pt.project_id)))
	  ,[rls_company] = 
			(SELECT
				CONCAT(COALESCE([company],''),'_',COALESCE([region],''))
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
				WHERE project_id =
				((SELECT project_id 
				FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
				WHERE p.project_id = pt.project_id)))
	  ,[rls_businessarea] = 
			(SELECT
				CONCAT(COALESCE([business_area],''),'_',COALESCE([region],''))
			FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
				WHERE project_id =
				((SELECT project_id 
				FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
				WHERE p.project_id = pt.project_id)))
	  ,(SELECT [project_name] 
		FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }} p
		WHERE p.project_id = pt.project_id) AS [project_name]
	  ,[terminal_name]
      ,[calculation_date]
      ,[uniq_personal_count]
      ,project_id
  FROM {{ source('stg_pdks_kpi', 'raw__pdks_kpi_t_fact_projectterminaluniqnumpeople') }} pt


UNION ALL

	SELECT * FROM aws_stage.pdks_kpi.stg__pdks_kpi_t_fact_projectterminalstats_rmoresample