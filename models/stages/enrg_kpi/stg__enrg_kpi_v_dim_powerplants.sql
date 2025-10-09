{{
  config(
    materialized = 'view',tags = ['enrg_kpi']
    )
}}	
SELECT
	 [rls_region] = 'TUR'
	,[rls_group] = CONCAT([group],'_TUR')
	,[rls_company] = CONCAT([company],'_TUR')
	,[rls_business_area]  = CONCAT([werks],'_TUR')
  ,[rls_businessarea]  = CONCAT([werks],'_TUR') 
  ,pp.*
FROM {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }} pp