{{
  config(
    materialized = 'table',tags = ['sf_new_api','hrall']
    )
}}

WITH raw_cte AS (
    SELECT
        user_id,
        manager_user_id
	FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} hr
    WHERE employee_status_en = N'ACTIVE'
),
EmployeeHierarchy AS (
    SELECT 
        user_id AS u2,
        user_id,
        manager_user_id,
        1 AS manager_rank
    FROM raw_cte

    UNION ALL

    SELECT 
        eh.u2,
        r.user_id,
        r.manager_user_id,
        eh.manager_rank + 1
    FROM raw_cte r
    INNER JOIN EmployeeHierarchy eh ON eh.user_id = r.manager_user_id
),
final_hierarchy AS (
    SELECT

        [u2] as topmanager_user_id_main
        ,hrc.[user_id] as [user_id_main]
        ,hrc.[manager_user_id] as manager_user_id_main
        ,[manager_rank]
        ,emp_manager.full_name AS manager_full_name
        ,emp.full_name AS personnel_full_name
        ,emp_manager.*
        ,CURRENT_TIMESTAMP AS db_upload_timestamp
    FROM EmployeeHierarchy hrc
        left join {{ ref('dm__hr_kpi_t_dim_employees') }} emp_manager on  hrc.u2 = emp_manager.user_id
        left join {{ ref('dm__hr_kpi_t_dim_employees') }} emp on  hrc.user_id = emp.user_id
)

SELECT * 
FROM final_hierarchy



