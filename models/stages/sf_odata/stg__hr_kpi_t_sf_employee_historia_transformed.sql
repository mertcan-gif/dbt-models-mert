{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}
 
  with dedup as (
    select distinct 
        emp.sf_system,
        emp.[seq_number],
        CAST(emp.[start_date] AS date) as [start_date],
        case 
            when CAST(emp.[end_date] as date) = '9999-12-31' and emp.employee_status_en = 'Active' then CAST(GETDATE() as date)
            when emp.employee_status_en = 'Dismissed' then CAST(emp.[start_date] as date)
            else CAST(emp.[end_date] as date)
        end AS [end_date],
        emp.[user_id],
        emp.[employee_status_en],
        emp.[hay_kademe],
        emp.[hay_kademe_personal],
        emp.[ronesans_rank_personal],
        emp.[ronesans_rank_en],
        emp.[global_id],
        emp.[sap_id],
        emp.[name],
        emp.[surname],
        emp.[a_level_code],
        emp.[b_level_code],
        emp.[c_level_code],
        emp.[d_level_code],
        emp.[e_level_code],
        a.name_en AS a_level,
        b.name_en AS b_level,
        c.name_en AS c_level,
        d.name_en AS d_level,
        e.name_en AS e_level,
        a.name_tr AS a_level_tr,
        b.name_tr AS b_level_tr,
        c.name_tr AS c_level_tr,
        d.name_tr AS d_level_tr,
        e.name_tr AS e_level_tr,
        emp.[position],
        emp.[business_function],
        emp.[cost_center_code],
        emp.[cost_center_name],
        emp.[payroll_company_code],
        emp.[payroll_company],
        emp.[manager_user_id],
        emp.[workplace_en],
        emp.[job_start_date],
        emp.[job_end_date],
        emp.[business_area],
        emp.[email_address],
        emp.real_termination_reason_en,
        emp.employee_type_en,
        emp.employee_status_tr,
        emp.ronesans_rank_tr,
        emp.workplace_tr,
        emp.date_of_birth,
        emp.employee_type_tr,
        emp.gender,
        emp.total_team_size,
        emp.team_member_size,
        emp.actual_working_country,
        emp.physical_location,
        emp.physical_location_city,
        emp.real_termination_reason_tr,
        emp.event_reason,
        ROW_NUMBER() OVER (PARTITION BY emp.user_id, CAST(emp.[start_date] as date) order by CAST(emp.seq_number AS FLOAT) DESC) AS dedup_row_number
        from {{ ref('stg__hr_kpi_t_sf_employee_historia_unioned') }} emp
                LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levela_union') }} a ON a.code = emp.[a_level_code]
                LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levelb_union') }} b ON b.code = emp.[b_level_code]
                LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levelc_union') }} c ON c.code = emp.[c_level_code]
                LEFT JOIN {{ ref('stg__hr_kpi_t_dim_leveld_union') }} d ON d.code = emp.[d_level_code]
                LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levele_union') }} e ON e.code = emp.[e_level_code]
            --where emp.[user_id] not in (select distinct user_id from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees_historia') }}
              --                           where employee_status_en=N'Reported No Show'
                --                         )

  ),

  DateRange as (
  select * from dedup where dedup_row_number=1
  )

  ,historical_data AS (
  select  
      sf_system,
      dr.[user_id],
      dr.[start_date],
      dr.[end_date],
      dr.[employee_status_en],
      dr.[hay_kademe],
      dr.[hay_kademe_personal],
      dr.[ronesans_rank_personal],
      dr.[ronesans_rank_en],
      dr.[global_id],
      dr.[sap_id],
      dr.[name],
      dr.[surname],
      dr.[a_level],
      dr.[b_level],
      dr.[c_level],
      dr.[d_level],
      dr.[e_level],
      dr.[a_level_tr],
      dr.[b_level_tr],
      dr.[c_level_tr],
      dr.[d_level_tr],
      dr.[e_level_tr],
      dr.[position],
      dr.[business_function],
      dr.[cost_center_code],
      dr.[cost_center_name],
      dr.[payroll_company_code],
      dr.[payroll_company],
      dr.[manager_user_id],
      dr.[workplace_en],
      dr.[job_start_date],
      dr.[job_end_date],
      dr.[business_area],
      dr.[email_address],
      dr.real_termination_reason_en,
      dr.employee_type_en,
      dr.employee_status_tr,
      dr.ronesans_rank_tr,
      dr.workplace_tr,
      dr.date_of_birth,
      dr.employee_type_tr,
      dr.gender,
      dr.total_team_size,
      dr.team_member_size,
      dr.actual_working_country,
      dr.physical_location,
      dr.physical_location_city,
      dr.real_termination_reason_tr,
      dr.[event_reason],
      DATEADD(DAY, n.number, dr.start_date) AS snapshot_date
  from DateRange dr
  CROSS APPLY (
      select TOP (case when DATEDIFF(DAY, dr.start_date, dr.end_date) >= 0 
                      then DATEDIFF(DAY, dr.start_date, dr.end_date) + 1 
                      else 0 end) 
            ROW_NUMBER() OVER (order by (select NULL)) - 1 AS number
      from master.dbo.spt_values
  ) n
  where 1=1
    and dr.user_id NOT IN ('GLB354126') -- bu kişinin kaydı bozuk
  )

,final as 
    (
  SELECT 
      *
      ,rn=ROW_NUMBER() OVER (PARTITION BY sap_id, snapshot_date order by employee_status_en ASC) 
    FROM historical_data
    )
  
  SELECT 
       [sf_system]
      ,[user_id] = CASE WHEN sap_id = '' THEN user_id ELSE sap_id END --yeni sistemde user_idler sf_idler ile aynı yapıldı
      ,[start_date]
      ,[end_date]
      ,[employee_status_en]
      ,[hay_kademe]
      ,[hay_kademe_personal]
      ,[ronesans_rank_personal]
      ,[ronesans_rank_en]
      ,[global_id]
      ,[sap_id]
      ,[name]
      ,[surname]
      ,[a_level]
      ,[b_level]
      ,[c_level]
      ,[d_level]
      ,[e_level]
      ,[a_level_tr]
      ,[b_level_tr]
      ,[c_level_tr]
      ,[d_level_tr]
      ,[e_level_tr]
      ,[position]
      ,[business_function]
      ,[cost_center_code]
      ,[cost_center_name]
      ,[payroll_company_code]
      ,[payroll_company]
      ,[manager_user_id]
      ,[workplace_en]
      ,[job_start_date]
      ,[job_end_date]
      ,[business_area]
      ,[email_address]
      ,[real_termination_reason_en]
      ,[employee_type_en]
      ,[employee_status_tr]
      ,[ronesans_rank_tr]
      ,[workplace_tr]
      ,[date_of_birth]
      ,[employee_type_tr]
      ,[gender]
      ,[total_team_size]
      ,[team_member_size]
      ,[actual_working_country]
      ,[physical_location]
      ,[physical_location_city]
      ,[real_termination_reason_tr]
      ,[event_reason]
      ,[snapshot_date]
  FROM final
  WHERE rn = 1

