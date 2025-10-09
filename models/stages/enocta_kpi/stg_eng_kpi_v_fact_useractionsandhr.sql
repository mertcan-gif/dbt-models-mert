
{{
  config(
    materialized = 'table',tags = ['enc_kpi']
    )
}}	
SELECT
  ua.*,
  hr.name,
  hr.surname,
  hr.full_name,
  hr.employee_id,
  hr.gender,
  hr.a_level_group,
  hr.b_level_company,
  hr.c_level_region,
  hr.d_level_department,
  hr.e_level_unit,
  hr.collar_type,
  hr.dwh_data_group,
  hr.grouped_title,
  hr.age,
  hr.date_of_birth,
  hr.dwh_education_status,
  hr.dwh_ronesans_total_seniority,
  hr.dwh_ronesans_seniority,
  hr.employee_status
  from {{ source('stg_enc_kpi', 'raw_enocta_kpi_t_fact_useractions') }}  ua
LEFT JOIN {{ ref('dm__hr_kpi_t_dim_hrall') }} as hr on hr.global_id = ua.USER_CODE and language = 'TR'