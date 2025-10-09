
{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}

WITH final_data AS (

      SELECT
         dim_comp.[rls_region]
         ,dim_comp.[rls_group]
         ,dim_comp.[rls_company]
         ,dim_comp.[rls_businessarea]
         ,[year]
         ,[month]
         ,[company]
	 	--,[cost_center] = NULL
         ,[financial_center_code]
         ,[norm]
         ,[actual]
         ,[version] = 'V2'
     FROM {{ ref('stg__gyg_kpi_t_fact_hrpermanentstaff') }} ps
     LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp 
             ON dim_comp.RobiKisaKod = ps.company
     WHERE dim_comp.kyriba_ust_group = N'RÖNESANS'

	 UNION ALL
	SELECT
		  rls_region
		  ,rls_group
		  ,rls_company
		  ,rls_businessarea
		  ,[year]
		  ,[month]
		  ,[company]
		  --,[cost_center]
		  ,[financial_center_code]
		  ,[norm]
		  ,[actual]
		  ,[version] = 'V1'
	  FROM {{ source('stg_sharepoint','raw__gyg_kpi_t_fact_hrpermanentstaff') }} hr
	  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON hr.[company] = dim_comp.RobiKisaKod
	  WHERE dim_comp.kyriba_ust_group = N'RÖNESANS'
)


SELECT * FROM final_data
