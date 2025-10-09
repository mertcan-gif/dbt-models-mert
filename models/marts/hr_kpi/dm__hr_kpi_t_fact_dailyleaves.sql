{{
  config(
    materialized = 'table',tags = ['hr_kpi','hr_activepersonnel']
    )
}}

select 
   rls.rls_region
  ,rls.rls_group
  ,rls.rls_company
  ,rls.rls_businessarea
  ,daily_leaves.user_id
  ,daily_leaves.sf_system
  ,daily_leaves.sap_id
  ,daily_leaves.leave_code
  ,daily_leaves.leave_type
  ,daily_leaves.time_account_external_code
  ,daily_leaves.calendar_entry_code
  ,daily_leaves.booking_date
  ,daily_leaves.quantity
  ,daily_leaves.tur
  ,daily_leaves.created_date_time
  ,daily_leaves.comment
  ,emp.employee_status
  ,emp.hay_kademe
  ,emp.ronesans_rank_personal
  ,emp.cost_center_code
  ,emp.cost_center_name
  ,emp.payroll_company
  ,emp.payroll_company_code
  ,emp.manager_user_id
  ,emp.business_area
  ,emp.email_address
  ,emp.total_team_size
  ,emp.team_member_size
  ,emp.date_of_birth
  ,emp.ronesans_rank
  ,emp.global_id
  ,emp.name
  ,emp.surname
  ,emp.full_name
  ,emp.a_level
  ,emp.b_level
  ,emp.c_level
  ,emp.d_level
  ,emp.e_level
  ,emp.position
  ,emp.position_group
  ,emp.workplace
  ,emp.job_start_date
  ,emp.job_end_date
  ,emp.gender
  ,emp.manager_name_surname
  ,emp.actual_location_code
  ,emp.actual_location_name_tr
  ,emp.employee_type_name_en
  ,emp.real_termination_reason_tr
  ,emp.real_termination_reason_en
  ,emp.actual_working_country
  ,emp.job_family
  ,is_future = case when cast(daily_leaves.booking_date as date) > cast(getdate() as date) then 'Yes' else 'No' end
from {{ ref('stg__hr_kpi_t_fact_daily_leaves') }} daily_leaves
	left join {{ ref('stg__hr_kpi_t_dim_sf_rls') }} rls on rls.user_id = daily_leaves.user_id
  left join {{ ref('dm__hr_kpi_t_dim_employees') }} emp on emp.user_id = daily_leaves.user_id
where transaction_type = 'normal_kayit' and daily_leaves.user_id IS NOT NULL and emp.employee_status = N'AKTİF'