{{
  config(
    materialized = 'table',tags = ['hr_kpi', 'rmore']
    )
}}

/* 
Date: 20250912
Creator: Kaan Keskin - Adem Numan Kaya
Report Owner: Adem Numan Kaya
Explanation: Uzerinde zimmet olan personeller listelenmistir. 
*/


SELECT
	rls.[rls_businessarea]
	,rls.[rls_company]
	,rls.[rls_group]
	,rls.[rls_region]
	,rls.[name_surname]
  	,eai.[externalcode] AS "external_code"
  	,eai.[parent_external_code] AS "parent_external_code"
  	,eai.[assigned_item_type] AS "assigned_item_type"
  	,CAST(eai.[last_modified_date] AS DATE) AS "last_modified_date"
  	,eai.[assigned_item_note] AS "assigned_item_note"
  	,CAST(eai.[created_date] AS DATE) AS "created_date"
  	,eai.[assigned_item_serial_no] AS "assigned_item_serial_no"
  	,eai.[quantity] AS "quantity"
  	,CAST(eai.[end_date] AS DATE) AS "end_date"
  	,CAST(eai.[expiration_date] AS DATE) AS "expiration_date"
  	,CAST(eai.[start_date] AS DATE) AS "start_date"
  	,CAST(eai.[effective_date] AS DATE) AS "effective_date"
  	,eai.[last_modified_by] AS "last_modified_by"
  	,eai.[assigned_item_equipment_no] AS "assigned_item_equipment_no"
  	,eai.[assigned_item_material_no] AS "assigned_item_material_no"
  	,eai.[user_id] AS "user_id"
  	,eai.[asset_no] AS "asset_no"
  	,eai.[status] AS "status"
  	,eai.[assigned_item_name_tr] AS "assigned_item_name_tr"
  	,eai.[assigned_item_statu] AS "assigned_item_statu"
  	,CAST(eai.[db_upload_timestamp] AS DATE) AS "db_upload_timestamp"
	,emp.employee_type
	,emp.employee_city_tr
	,emp.manager_user_id
	,emp.employee_status_tr
	,emp.employee_status_en
	,job_start_date = CAST(emp.job_start_date AS DATE) 
	,job_end_date = CAST(emp.job_end_date AS DATE)
	,is_problamatic_flag = CASE WHEN emp.employee_status_en='Terminated' then 1 else 0 end
	,date_assign_diff_to_start = DATEDIFF(DAY, CAST(emp.job_start_date AS DATE), CAST(eai.[effective_date] AS DATE))
  FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employee_assign_items') }} as eai
  LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} AS rls on rls.[user_id]=eai.[user_id]
  LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} as emp on emp.[user_id]=eai.[user_id]
  WHERE rls_businessarea IS NOT NULL