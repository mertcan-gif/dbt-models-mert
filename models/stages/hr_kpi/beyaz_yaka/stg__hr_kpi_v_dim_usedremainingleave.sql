{{
  config(
    materialized = 'view',tags = ['hr_kpi']
    )
}}

WITH used as (
	select 
		emp_time.[user_id],
		SUM(CAST(emp_time.quantity as float)) as used_quantity,
		SUM(CASE WHEN emp_time.start_date >= emp.job_start_date  then CAST(emp_time.quantity as float) else 0 end) as used_quantity_final
	from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employee_time') }} emp_time
		left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }}  emp on emp.[user_id] = emp_time.[user_id]
	where cast(emp_time.[start_date] as date) <= GETDATE()
	group by 
		emp_time.[user_id]
)
,
remaining as 
(
	select 
		ta.user_id
		,SUM(cast(booking_amount as float)) remaining_quantity
	from {{ source('stg_sf_odata','raw__hr_kpi_t_sf_timeaccount') }} ta
		RIGHT join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_timeaccountdetails') }} tad ON tad.time_account_external_code = ta.external_code
	where 1=1
		and account_type_name = N'Annual Leave TR'
		and booking_date <= GETDATE()
		and account_closed = 0
	group by ta.user_id

)

select 
	emp.[global_id] AS 'merge_id',
    emp.[global_id] AS 'Rem_Global_Id',
	emp.[user_id] AS 'user_id',
	emp.[name] as 'Rem_Ad',
	emp.[surname] as 'Rem_Soyad',
	coalesce(remaining.remaining_quantity,0) + coalesce(used.used_quantity_final,0) AS 'DWH_AnnualLeave',
	--coalesce(remaining.remaining_quantity,0) as 'DWH_remaning',
	emp.[global_id] AS 'global_id_used',
	emp.[name] as 'USED_Ad',
	emp.[surname] as 'USED_Soyad',
	coalesce(used.used_quantity_final,0) * -1 AS 'DWH_UsedLeave'
	--coalesce(used.used_quantity,0) AS 'user_leave_v1',
from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }}  as emp 
	left join used  ON emp.[user_id] = used.[user_id]
	left join remaining  ON emp.[user_id] = remaining.[user_id]
where 1=1
	and emp.employee_status_tr = N'AKTİF'


