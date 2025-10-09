{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}
/*
  Yeni Rpeople sistemine eski personellerin (Aktif olmayan) verileri gömülmediği için eski sistemden alınmıştır. Ancak şöyle bir durum fark edilmiştir. 
Eski sistem = Coach
Yeni Sistem = Rpeople

    -- Eski personellerin bazıları a seviyesi "Veri Aktarımı" olacak şekilde Çıkarılmış olarak yeni sisteme gömülmüştür, bunların verilerini içeriye almıyoruz filtre atıyoruz
    -- Eski personellerin baızları ise verileri doldurularak içeri alınmış Aktif olmasa da bu bilgileri coachtan değl Rpeople'dan alıyoruz.
    -- Eski sistem ile yeni sistemin userIdleri aynı değildir, yeni sistemde userId'ler sap_id ile ezilmiştir, bu sebeple Coach'tan da sap_id'ler alınmıştır.
    -- Eski sistemde sap_id'ler tekrar edebilmektedir, bu sebeple aynı sap_id tekrar ediyor ise, job_end_date'i en yeni olan transaction baz alınmıştır.
*/
WITH raw_cte as (
  SELECT 
      -- Eski Sistemle Ortak Alanlar
      sf_system = 'Rpeople'
      ,sf_system_hierarcy = 99999
      ,[seq_number]  ,[start_date]  ,[user_id]  ,[global_id]  ,[sap_id]  ,[employee_status_tr]  ,[employee_status_en]  ,[ronesans_rank_tr]  ,[ronesans_rank_en]  ,[ronesans_rank_personal_tr]  ,[ronesans_rank_personal_en]  ,[name]  ,[surname]  ,[a_level_code]  ,[b_level_code]  ,[c_level_code]  ,[d_level_code]  ,[e_level_code]  ,[position]  ,[cost_center_code]  ,[cost_center_name]  ,[payroll_company_code]  ,[payroll_company]  ,[manager_user_id]  ,[workplace_tr]  ,[workplace_en]  ,[job_start_date]  ,[job_end_date]  ,[end_date]  ,[business_area]  ,[email_address]  ,[date_of_birth]  ,[gender]  ,[total_team_size]  ,[team_member_size]  ,[actual_working_country]  ,[position_group]  ,[real_termination_reason_tr]  ,[real_termination_reason_en]  ,[employee_type_name_tr]  ,[employee_type_name_en]  ,[hay_kademe]  ,[employee_city_tr]  ,[actual_location_name_tr]  ,[meal_allowance_en]  ,[meal_allowance_tr] ,[seniority_base_date] ,[db_upload_timestamp]  
      -- Yeni Sistemle Beraber Aldığımız Alanlar
        ,[person_id]  ,[manager_global_id]  ,[manager_sap_id]  ,[manager_name]  ,[manager_surname]  ,[second_manager_user_id]  ,[second_manager_name]  ,[second_manager_surname]  ,[second_manager_global_id]  ,[second_manager_sap_id]  ,[country_code]  ,[country]  ,[employee_area_code]  ,[employee_area_en]  ,[unit_code]  ,[unit]  ,[employee_group_code]  ,[employee_group_tr]  ,[employee_group_en]  ,[employee_sub_group_code]  ,[employee_sub_group_tr]  ,[employee_sub_group_en]  ,[initial_hire_date]    ,[employee_national_id]  ,[job_code]  ,[job_description]  ,[employee_sub_area_code]  ,[employee_sub_area]  ,[position_code]  ,[functional_manager_employee_id]  ,[functional_manager_global_id]  ,[functional_manager_sap_id]  ,[job_level]  ,[functional_manager_name]  ,[functional_manager_surname]  ,[business_area_code]  ,[hr_responsible_global_id]  ,[hr_responsible_sf_id]  ,[hr_responsible_name]  ,[hr_responsible_surname]  ,[work_area_code]  ,[work_area_tr]  ,[work_area_en]  ,[roneans_last_start_date]  ,[country_of_birth]  ,[domain_user]  ,[marital_status]  ,[marital_status_name_tr]  ,[marital_status_name_en]  ,[nationality]  ,[preferred_language_tr]  ,[preferred_language_en]  ,[second_nationality]  ,[username]  ,[eventreason_code]  ,[fte]  ,[employee_type]  ,[report_organization_tr]  ,[job_code_name]  ,[pay_group_code]  ,[pay_group_name]  ,[work_area_type_code]  ,[employee_city_en]  ,[personnel_sub_area_country_code]  ,[personnel_sub_area]  ,[actual_location_code]  ,[actual_location_name_en]  ,[job_family]  ,[job_function]  ,[peer_grup]  ,[kf_job_subfunction_code]  ,[position_city]
  FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp
  WHERE a_level_code <> '10005415' AND employee_status_tr <> N'Gelmeme Etkinliği Raporlandı'

  UNION ALL

  SELECT 
    sf_system = 'Coach'
    ,sf_system_hierarcy = ROW_NUMBER() over(partition by sap_id order by job_end_date asc)  
    /* aynı sap id ile iki üç farklı kayıt olabildiğinden ötürü job_end_date'e göre son termination date'i baz alıyorum */
    ,[seq_number] as [seq_number]
    ,[start_date] as [start_date]
    ,[user_id] = CASE WHEN (sap_id = '' or sap_id is null) THEN user_id else sap_id end
    ,[global_id] as [global_id]
    ,[sap_id] as [sap_id]
    ,CASE
        WHEN [employee_status_tr] = N'ÇIKARILMIŞ' THEN N'Terminated'
        ELSE [employee_status_tr]
    END AS [employee_status_tr]
    ,[employee_status_en] as [employee_status_en]
    ,[ronesans_rank_tr] as [ronesans_rank_tr]
    ,[ronesans_rank_en] as [ronesans_rank_en]
    ,[ronesans_rank_personal] as [ronesans_rank_personal_tr]
    ,[ronesans_rank_personal] as [ronesans_rank_personal_en]
    ,[name] as [name]
    ,[surname] as [surname]
    ,[a_level_code] as [a_level_code]
    ,[b_level_code] as [b_level_code]
    ,[c_level_code] as [c_level_code]
    ,[d_level_code] as [d_level_code]
    ,[e_level_code] as [e_level_code]
    ,[position] as [position]
    ,[cost_center_code] as [cost_center_code]
    ,[cost_center_name] as [cost_center_name]
    ,[payroll_company_code] as [payroll_company_code]
    ,[payroll_company] as [payroll_company]
    ,[manager_user_id] as [manager_user_id]
    ,[workplace_tr] as [workplace_tr]
    ,[workplace_en] as [workplace_en]
    ,[job_start_date] as [job_start_date]
    ,[job_end_date] as [job_end_date]
    ,[end_date] as [end_date]
    ,[business_area] as [business_area]
    ,[email_address] as [email_address]
    ,[date_of_birth] as [date_of_birth]
    ,[gender] as [gender]
    ,[total_team_size] as [total_team_size]
    ,[team_member_size] as [team_member_size]
    ,[actual_working_country] as [actual_working_country]
    ,[business_function] as [position_group]
    ,[real_termination_reason_tr] as [real_termination_reason_tr]
    ,[real_termination_reason_en] as [real_termination_reason_en]
    ,[employee_type_tr] as [employee_type_name_tr]
    ,[employee_type_en] as [employee_type_name_en]
    ,[hay_kademe] as [hay_kademe]
    ,[physical_location_city] as [employee_city_tr]
    ,[physical_location] as [actual_location_name_tr]
    ,[meal_allowance_en] as [meal_allowance_en]
    ,[meal_allowance_tr] as [meal_allowance_tr]
    ,seniority_date as seniority_base_date
    ,[db_upload_timestamp] as [db_upload_timestamp]
    ,NULL AS [person_id]
    ,NULL AS [manager_global_id]
    ,NULL AS [manager_sap_id]
    ,NULL AS [manager_name]
    ,NULL AS [manager_surname]
    ,NULL AS [second_manager_user_id]
    ,NULL AS [second_manager_name]
    ,NULL AS [second_manager_surname]
    ,NULL AS [second_manager_global_id]
    ,NULL AS [second_manager_sap_id]
    ,NULL AS [country_code]
    ,NULL AS [country]
    ,NULL AS [employee_area_code]
    ,NULL AS [employee_area_en]
    ,NULL AS [unit_code]
    ,NULL AS [unit]
    ,NULL AS [employee_group_code]
    ,NULL AS [employee_group_tr]
    ,NULL AS [employee_group_en]
    ,NULL AS [employee_sub_group_code]
    ,NULL AS [employee_sub_group_tr]
    ,NULL AS [employee_sub_group_en]
    ,NULL AS [initial_hire_date]
    ,NULL AS [employee_national_id]
    ,NULL AS [job_code]
    ,NULL AS [job_description]
    ,NULL AS [employee_sub_area_code]
    ,NULL AS [employee_sub_area]
    ,NULL AS [position_code]
    ,NULL AS [functional_manager_employee_id]
    ,NULL AS [functional_manager_global_id]
    ,NULL AS [functional_manager_sap_id]
    ,NULL AS [job_level]
    ,NULL AS [functional_manager_name]
    ,NULL AS [functional_manager_surname]
    ,NULL AS [business_area_code]
    ,NULL AS [hr_responsible_global_id]
    ,NULL AS [hr_responsible_sf_id]
    ,NULL AS [hr_responsible_name]
    ,NULL AS [hr_responsible_surname]
    ,NULL AS [work_area_code]
    ,NULL AS [work_area_tr]
    ,NULL AS [work_area_en]
    ,NULL AS [roneans_last_start_date]
    ,NULL AS [country_of_birth]
    ,NULL AS [domain_user]
    ,NULL AS [marital_status]
    ,NULL AS [marital_status_name_tr]
    ,NULL AS [marital_status_name_en]
    ,NULL AS [nationality]
    ,NULL AS [preferred_language_tr]
    ,NULL AS [preferred_language_en]
    ,NULL AS [second_nationality]
    ,NULL AS [username]
    ,NULL AS [eventreason_code]
    ,NULL AS [fte]
    ,NULL AS [employee_type]
    ,NULL AS [report_organization_tr]
    ,NULL AS [job_code_name]
    ,NULL AS [pay_group_code]
    ,NULL AS [pay_group_name]
    ,NULL AS [work_area_type_code]
    ,NULL AS [employee_city_en]
    ,NULL AS [personnel_sub_area_country_code]
    ,NULL AS [personnel_sub_area]
    ,NULL AS [actual_location_code]
    ,NULL AS [actual_location_name_en]
    ,NULL AS [job_family]
    ,NULL AS [job_function]
    ,NULL AS [peer_grup]
    ,NULL AS [kf_job_subfunction_code]
    ,NULL AS [position_city]
  FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_employees') }} emp
  WHERE employee_status_tr = N'ÇIKARILMIŞ'
)


select *
from (
  select 
    *,
    row_number() over(partition by user_id order by sf_system_hierarcy desc ) as rn
  from raw_cte ) final
where rn = 1
