{{
  config(
    materialized = 'table',tags = ['hr_kpi','leave']
    )
}}


WITH used as (
	select 
		emp_time.[user_id],
		SUM(CAST(emp_time.quantity as float)) as used_quantity,
		SUM(CASE WHEN emp_time.start_date >= emp.job_start_date  then CAST(emp_time.quantity as float) else 0 end) as used_quantity_final
	from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employee_time') }} emp_time
		left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp on emp.[user_id] = emp_time.[user_id]
	where cast(emp_time.[start_date] as date) <= GETDATE()
	group by 
		emp_time.[user_id]
		),
remaining as 
	(
	select 
		ta.user_id
		,SUM(cast(booking_amount as float)) remaining_quantity
	from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_timeaccount') }} ta
		RIGHT join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_timeaccountdetails') }} tad ON tad.time_account_external_code = ta.external_code
	where 1=1
		and account_type_name = N'Annual Leave TR'
		and booking_date <= GETDATE()
		and account_closed = 0
	group by ta.user_id
	),
accrual AS 
	(
	select * 
	from (
		select user_id, 
				CAST (booking_amount AS float) AS 'accrual_leave',
		row_number() over(partition by user_id order by CAST(accrual_period_id AS int) DESC) AS 'rn'
		from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_timeaccount') }} ta
			RIGHT join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_timeaccountdetails') }} tad ON tad.time_account_external_code = ta.external_code
		where 1=1
			and account_type_name = N'Annual Leave TR'
			and booking_date <= GETDATE()
			and account_closed = 0
			AND booking_type='ACCRUAL'
		) AS subquery
	where subquery.rn=1
	)
 
,final_cte as (
	select
		rls_region = CASE 
						WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr) = 'TR' THEN 'TUR'
						ELSE 'RUS' 
					 END
		,rls_group = UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = level_a.name_tr))
		,rls_company = UPPER(level_b.name_en)
		,rls_businessarea = UPPER(emp.business_area)
		,emp.[user_id] as 'user_id'
		,emp.[sap_id] as 'sap_id'
		,concat(emp.name, ' ', emp.surname) as 'name_surname'
		,level_a.name_tr as 'a_level_name'
		,level_b.name_tr as 'b_level_name'
		,level_c.name_tr as 'c_level_name'
		,level_d.name_tr as 'd_level_name'
		,level_e.name_tr as 'e_level_name'
		--,coalesce(remaining.remaining_quantity,0) + coalesce(used.used_quantity_final,0) AS 'annual_leave',
		,coalesce(accrual.accrual_leave,0) AS 'accrual_leave'
		,coalesce(remaining.remaining_quantity,0) as 'remaning_leave'
		,coalesce(used.used_quantity_final,0) * -1 AS 'used_leave'
		,coalesce(used.used_quantity,0) * -1 AS 'used_leave_all_time'
	from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }}  as emp 
		LEFT JOIN used ON emp.[user_id] = used.[user_id]
		LEFT JOIN remaining ON emp.[user_id] = remaining.[user_id]
		LEFT JOIN accrual ON emp.[user_id]=accrual.[user_id]
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }} level_a ON level_a.code = emp.a_level_code
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_b') }} level_b ON level_b.code = emp.b_level_code
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_c') }} level_c ON level_c.code = emp.c_level_code
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_d') }} level_d ON level_d.code = emp.d_level_code
		LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_e') }} level_e ON level_e.code = emp.e_level_code 
	where 1=1
		and emp.employee_status_tr = N'AKTİF'
)
 
select 
	rls_region 
	,rls_group = CONCAT(COALESCE([rls_group],''),'_',COALESCE([rls_region],''))
	,rls_company = CONCAT(COALESCE([rls_company],''),'_',COALESCE([rls_region],''))
	,rls_businessarea = CONCAT(COALESCE([rls_businessarea],''),'_',COALESCE([rls_region],''))
	,[user_id]
	,sap_id
	,name_surname
	,a_level_name
	,b_level_name
	,c_level_name
	,d_level_name
	,e_level_name
	,accrual_leave
	,remaning_leave
	,used_leave
	,used_leave_all_time
from final_cte