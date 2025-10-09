{{
  config(
    materialized = 'table',tags = ['sf_new_api']
    )
}}
WITH hr_beyaz_yaka AS (
    SELECT
        rls_region = CASE 
                        WHEN (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level.name_en) = 'TR' THEN 'TUR'
                        ELSE 'NAN' 
                    END,
        rls_group = UPPER((SELECT TOP 1 group_rls FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level.name_en)),
        rls_company = UPPER(b_level.name_en),
        rls_businessarea = work_area_code,
        [dwh_data_group] = 'WHITE COLLAR',
        [employee_status] = CASE WHEN emp.employee_status_en = 'Active' THEN 'A' ELSE 'T' END,
        [sf_id_number] = emp.user_id,
        [employee_id]  = emp.person_id, --Kullanılmıyor
        [adines_number] = '', --Kullanılmıyor
        [global_id] = emp.global_id,
        emp.[sap_id],
        username = UPPER(emp.[username]),
        emp.[name],
        emp.[surname],
        [full_name] = CONCAT(emp.[name], ' ', emp.[surname]),
        emp.[gender],
        marital_status = UPPER(emp.[marital_status_name_en]),
        emp.[nationality],
        [event_reason] = events.name_en,
        [country] = emp.actual_working_country,
        emp.[payroll_company],
        [cost_center] = UPPER(emp.cost_center_name),
        [employee_group]= UPPER(emp.employee_group_en),
        [role_code] = emp.job_code,
        [role] = emp.job_code_name,
        [ronesans_job_level] = emp.job_level,
        [supervisor_name] = CONCAT(emp.[manager_name], ' ', emp.[manager_surname]),
        CAST(emp.[date_of_birth] AS DATE) date_of_birth,
        [age] =
            DATEDIFF(YEAR, emp.date_of_birth, GETDATE()) -
                CASE
                    WHEN DATEADD(YEAR, DATEDIFF(YEAR, CAST(emp.date_of_birth AS DATE), GETDATE()), CAST(emp.date_of_birth AS DATE)) > GETDATE()
                    THEN 1
                    ELSE 0
                END,
        [language] = 'EN',
        [a_level_group] = a_level.name_en,
        [b_level_company] = b_level.name_en,
        [c_level_region] = c_level.name_en,
        [d_level_department] = d_level.name_en,
        [e_level_unit] = e_level.name_en,
        [custom_region] = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level.name_tr), --dim_group eklendi
        emp.[actual_working_country], -- country ile aynı kolon sonra kontrol edilmelidir
        [actual_working_city] = UPPER(emp.position_city),
        [collar_type] = 'WHITE COLLAR',
	    [dwh_leave] = COALESCE(izin.annual_leave_amount, 0) + COALESCE(izin.used_leave_amount , 0),
	    [dwh_annual_leave] = izin.annual_leave_amount,
        [dwh_used_leave] = izin.used_leave_amount,
        [dwh_workplace] = CASE
                                WHEN  emp.workplace_en  = N'Central' THEN N'Head Office'
                                WHEN emp.workplace_en = N'Corprate' THEN N'Facilities'
                                WHEN emp.workplace_en = N'Site' THEN N'Site'
                                ELSE  emp.workplace_en  END,
        [dwh_origin_code] = (SELECT TOP 1 custom_region FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp WHERE grp.[group] = a_level.name_tr), -- dim_groupla yapacağız şimdilik TUR yazılmıştır.
        [dwh_type] = 'SF',
        [dwh_employee_type] = emp.employee_type_name_en,
        [dwh_education_status] = egt.egitim_seviyesi_en , -- TBD.
        [dwh_date_of_recruitment] = CASE WHEN emp.[roneans_last_start_date] IS NOT NULL THEN CAST(emp.[roneans_last_start_date] AS DATE) END,
        [dwh_date_of_termination] = 
									CASE 
										WHEN emp.job_end_date = '1753-01-01 00:00:00.000' 
										THEN NULL ELSE emp.job_end_date
									END,
        ronesans_rank = [ronesans_rank_personal_tr],
        [grouped_title] =  UPPER(ug.Gruplanmis_Unvan_ENG), --TBD,
        dwh_termination_reason =
                        CASE
                            WHEN emp.employee_status_en = N'Active' THEN N'ACTIVE'
                            WHEN left(real_termination_reason_tr,6) = N'İstifa'  THEN  N'VOL-RESIGNATION'
                            WHEN left(real_termination_reason_tr,5) = N'İşten'  THEN N'INVOL-TERMINATION OF EMPLOYMENT'
                        ELSE 'Other'
                        END,
        [dwh_cause_details] = UPPER(emp.employee_status_en),
        [dwh_ronesans_last_seniority]=
                                CASE
                                        WHEN [employee_status_en] = N'Active' THEN DATEDIFF(DAY, job_start_date, GETDATE())
                                        ELSE DATEDIFF(DAY,job_start_date,job_end_date)
                                    END
        ,dwh_ronesans_seniority = 0
        ,dwh_non_ronesans_seniority = 0
        ,dwh_ronesans_total_seniority = 0
        ,dwh_actual_termination_reason = emp.real_termination_reason_en
        ,[dwh_worksite] = emp.work_area_code
        ,[payroll_sub_unit] = emp.pay_group_code
        ,[dwh_worksite_description] = UPPER(emp.work_area_en)
        ,[dwh_workplace_merged] = 
            CASE
                WHEN e_level.name_en <> '' THEN e_level.name_en
                WHEN d_level.name_en <> '' THEN d_level.name_en
                WHEN c_level.name_en <> '' THEN c_level.name_en
                WHEN b_level.name_en <> '' THEN b_level.name_en
                WHEN a_level.name_en <> '' THEN a_level.name_en    
            END
        ,emp.[payroll_company_code]
        ,calisan_tipi = emp.employee_type_name_en
        ,job_end_date
    FROM {{ ref('stg__hr_kpi_t_dim_employees_union_raw') }} emp
        LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_titlegroup') }} AS ug on emp.job_level = ug.Ronesans_Job_Level
        LEFT JOIN {{ ref('stg_hr_kpi_t_sf_new_used_remaining_leave') }} AS izin on emp.[global_id] = izin.global_id_used
        LEFT JOIN {{ ref('stg__hr_kpi_v_sf_new_maxeducation') }} AS egt on emp.user_id = egt.person_id
        LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_eventreasons') }} events ON emp.eventreason_code = events.code
        LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levela_union') }} a_level ON a_level.code = emp.[a_level_code]
        LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levelb_union') }} b_level ON b_level.code = emp.[b_level_code]
        LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levelc_union') }} c_level ON c_level.code = emp.[c_level_code]
        LEFT JOIN {{ ref('stg__hr_kpi_t_dim_leveld_union') }} d_level ON d_level.code = emp.[d_level_code]
        LEFT JOIN {{ ref('stg__hr_kpi_t_dim_levele_union') }} e_level ON e_level.code = emp.[e_level_code]
)

SELECT
    hr.[rls_region]
    ,rls_group = CONCAT(COALESCE([rls_group],''),'_',COALESCE([rls_region],''))
	,rls_company = CONCAT(COALESCE([rls_company],''),'_',COALESCE([rls_region],''))
	,rls_businessarea = CONCAT(COALESCE([rls_businessarea],''),'_',COALESCE([rls_region],''))
    ,hr.[dwh_data_group]
    ,hr.[employee_status]
    ,hr.[sf_id_number]
    ,hr.[employee_id]
    ,hr.[adines_number]
    ,hr.[global_id]
    ,hr.[sap_id]
    ,hr.[username]
    ,hr.[name]
    ,hr.[surname]
    ,hr.[full_name]
    ,hr.[gender]
    ,hr.[marital_status]
    ,hr.[nationality]
    ,hr.[event_reason]
    ,hr.[country]
    ,hr.[payroll_company]
    ,hr.[cost_center]
    ,hr.[employee_group]
    ,hr.[role_code]
    ,hr.[role]
    ,hr.[ronesans_job_level]
    ,hr.[supervisor_name]
    ,hr.[date_of_birth]
    ,hr.[age]
    ,hr.[language]
    ,hr.[a_level_group]
    ,hr.[b_level_company]
    ,hr.[c_level_region]
    ,hr.[d_level_department]
    ,hr.[e_level_unit]
    ,hr.[custom_region]
    ,hr.[actual_working_country]
    ,hr.[actual_working_city]
    ,hr.[collar_type]
    ,hr.[dwh_leave]
    ,hr.[dwh_annual_leave]
    ,hr.[dwh_used_leave]
    ,hr.[dwh_workplace]
    ,hr.[dwh_origin_code]
    ,hr.[dwh_type]
    ,hr.[dwh_employee_type]
    ,hr.[dwh_education_status]
    ,hr.[dwh_date_of_recruitment]
    ,hr.[dwh_date_of_termination]
    ,hr.[ronesans_rank]
    ,hr.[grouped_title]
    ,hr.[dwh_termination_reason]
    ,hr.[dwh_cause_details]
    ,hr.[dwh_ronesans_last_seniority]
    ,hr.[dwh_ronesans_seniority]
    ,hr.[dwh_non_ronesans_seniority]
    ,hr.[dwh_ronesans_total_seniority]
    ,hr.[dwh_actual_termination_reason]
    ,hr.[dwh_worksite]
    ,hr.[payroll_sub_unit]
    ,hr.[dwh_worksite_description]
    ,hr.[dwh_workplace_merged]
    ,hr.[payroll_company_code]
    ,hr.[calisan_tipi]
    ,photo_base64 = CASE WHEN LEN(pp.photobase64) < '28000' THEN pp.photobase64    ELSE NULL END
FROM hr_beyaz_yaka hr
    LEFT JOIN {{ ref('stg_hr_kpi_t_sf_new_photos') }} pp ON pp.user_id = hr.employee_id
WHERE 1=1
    AND custom_region <> 'RU'
    AND (calisan_tipi <> 'Oryantasyon' AND calisan_tipi IS NOT NULL)
    AND a_level_group <> 'OTHER'
    AND 1 = CASE
            WHEN employee_status = 'T' 
                AND	CONVERT(DATETIME, job_end_date, 104) >= '2022-01-01' 
            THEN 1
            WHEN employee_status = 'A' THEN 1
            ELSE 0
        END
    AND sf_id_number NOT IN (SELECT * FROM {{ ref('stg_hr_kpi_t_sf_new_concurrentusers') }})
    AND calisan_tipi NOT IN (N'Stajyer', 'Oryantasyon')
    --gorev tanimi filtresi eklenebilir