{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

with raw_cte as (
SELECT
	rls.rls_region
	,rls.rls_group
	,rls.rls_company
	,rls.rls_businessarea 
	,eht.sf_system
	,eht.[snapshot_date]
	,eht.[user_id]
	,eht.[employee_status_en]
	,CASE 
		WHEN eht.[hay_kademe_personal] = 'None' THEN NULL
		ELSE eht.[hay_kademe_personal]
		END AS [hay_kademe_personal] 
	,CASE 
		WHEN eht.[ronesans_rank_personal] = 'None' THEN NULL
		WHEN eht.[ronesans_rank_personal] = '' THEN NULL
		ELSE eht.[ronesans_rank_personal]
	END AS [ronesans_rank_personal]
	,eht.[global_id]
	,eht.[sap_id]
	,eht.[name]
	,eht.[surname]
	,full_name = CONCAT(eht.[name], ' ', eht.[surname])
	,a_level= eht.[a_level_tr]
	,b_level= eht.[b_level_tr]
	,c_level= eht.[c_level_tr]
	,d_level= eht.[d_level_tr]
	,e_level= eht.[e_level_tr]
	,CASE 
		WHEN eht.[position] = 'None' THEN NULL
		ELSE eht.[position]
		END AS [position] 
	,CASE 
		WHEN eht.[business_function] = 'None' THEN NULL
		ELSE eht.[business_function]
		END AS [business_function] 
	,eht.[cost_center_code]
	,CASE 
		WHEN eht.[cost_center_name] = 'None' THEN NULL
		ELSE eht.[cost_center_name]
		END AS [cost_center_name] 
	,eht.[payroll_company_code]
	,eht.[payroll_company]
	,eht.[manager_user_id]
	,manager_full_name = CONCAT(mng.[name], ' ', mng.[surname])
	,CASE 
		WHEN eht.[workplace_en] = 'None' THEN NULL
		ELSE eht.[workplace_en]
		END AS [workplace_en]
	,eht.[job_start_date]
	,CASE 
		WHEN eht.[business_area] = 'None' THEN NULL
		ELSE eht.[business_area]
		END AS [business_area] 
	,CASE 
		WHEN eht.[email_address] = 'None' THEN NULL
		ELSE eht.[email_address]
		END AS [email_address] 
	,eht.[real_termination_reason_en]
	,eht.[event_reason]
	,is_updated = case 
					when snapshot_date = eht.[start_date] then 'Updated' 
					else 'Regular' 
					end
	,next_alevel = LEAD(eht.a_level) OVER(PARTITION BY eht.user_id ORDER BY eht.snapshot_date)
	,next_workplace = LEAD(eht.workplace_en) OVER(PARTITION BY eht.user_id ORDER BY eht.snapshot_date)
	,next_business_area = LEAD(eht.business_area) OVER(PARTITION BY eht.user_id ORDER BY eht.snapshot_date)
	,next_cost_center_name = LEAD(eht.cost_center_name) OVER(PARTITION BY eht.user_id ORDER BY eht.snapshot_date)
	,next_ronesansrankpersonal = LEAD(eht.ronesans_rank_personal) OVER(PARTITION BY eht.user_id ORDER BY eht.snapshot_date)
	,next_manager_user_id = LEAD(eht.manager_user_id) OVER(PARTITION BY eht.user_id ORDER BY eht.snapshot_date)
	,eht.employee_type_tr
	,eht.employee_type_en
	,next_employee_type_en = LEAD(eht.employee_type_en) OVER(PARTITION BY eht.user_id ORDER BY eht.snapshot_date)
FROM {{ ref('stg__hr_kpi_t_sf_employee_historia_transformed') }} eht
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_employees_union_raw') }} as mng on mng.user_id=eht.manager_user_id
	LEFT JOIN {{ ref('stg__hr_kpi_t_dim_sf_rls') }} rls on rls.user_id = eht.user_id
WHERE 1=1
)

select 
	rls_key=CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
	,raw_cte.*
	,is_levela_change = CASE WHEN a_level <> next_alevel THEN 1 ELSE 0 END
	,is_workplace_change = CASE WHEN workplace_en <> next_workplace THEN 1 ELSE 0 END
	,is_business_area_change = CASE WHEN business_area <> next_business_area THEN 1 ELSE 0 END
	,is_cost_center_change = CASE WHEN cost_center_name <> next_cost_center_name THEN 1 ELSE 0 END
	,is_ronesans_rank_change = CASE WHEN ronesans_rank_personal <> next_ronesansrankpersonal THEN 1 ELSE 0 END
	,is_manager_change = CASE WHEN manager_user_id <> next_manager_user_id THEN 1 ELSE 0 END
	,is_last_day = CASE 
		WHEN next_alevel IS NULL AND snapshot_date < CAST(GETDATE() AS DATE) THEN 1 
		ELSE 0 
	END
	,is_employee_type_change = CASE WHEN employee_type_en <> next_employee_type_en THEN 1 ELSE 0 END
	,_date.is_end_of_month
	,_date.is_start_of_month
	,_date.year_month
	from raw_cte
	left join {{ source('stg_dimensions', 'raw__dwh_t_dim_datesshrp') }} _date on _date.[date] = raw_cte.snapshot_date
where 1=1 
-- and rls_region <> 'RUS'
and snapshot_date>='2022-01-01'
and snapshot_date < getdate()

