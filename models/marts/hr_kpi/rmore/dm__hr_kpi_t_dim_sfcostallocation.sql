{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

SELECT 
     rls_region = 'NAN'
    ,rls_group = 'GR_0000_NAN'
    ,rls_company = 'CO_0000_NAN'
    ,rls_businessarea = 'BA_0000_NAN'
    ,employees.[email_address]
    ,employees.full_name
    ,employees.[user_id] as sf_user_id
    ,employees.[global_id]
    ,employees.[employee_status]
    ,employees.[employee_type_name_en]
    ,employees.[position]
    ,employees.[payroll_company]
    ,employees.[payroll_company_code] 
    ,employees.[cost_center_code] 
    ,employees.[cost_center_name]
    ,employees.actual_location_name_tr
    ,employees.a_level
    ,employees.b_level
    ,employees.c_level
    ,employees.d_level
    ,employees.e_level
    ,CURRENT_TIMESTAMP  as "reporting_date"
FROM {{ ref('dm__hr_kpi_t_dim_employees') }} as employees
WHERE 1=1

    