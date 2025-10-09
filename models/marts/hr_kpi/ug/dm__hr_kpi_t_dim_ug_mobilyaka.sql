{{
  config(
    materialized = 'table',tags = ['hr_kpi','hr_ug','personnel_locations'],grants = {'select': ['s4hana_ug_user']}
    )
}}


SELECT
  [user_id],
  emp.[name] as  first_name,
  emp.[surname] as last_name,
  emp.[email_address],
  profile_image = null, -- sonradan doldurulacak
  emp.[position] as title,
  phone_number = phn.phone_number, --sf'ten alınacak
  password = null, -- sonradan doldurulacak
  emp.[actual_location_name_tr] as [location],
  emp.[a_level], --group bilgisi konulmuştur
  is_phone_restricted = 1, --default restrict edilmiştir.
  is_mail_restricted = 1,
  emp.[date_of_birth],
  emp.[job_start_date],
  emp.employee_status,
  emp.actual_working_country,
  emp.workplace,
  [department] =
      CASE
          WHEN e_level <> '' THEN e_level
          WHEN d_level <> '' THEN d_level
          WHEN c_level <> '' THEN c_level
          WHEN b_level <> '' THEN b_level
          WHEN a_level <> '' THEN a_level    
      END
FROM {{ ref('dm__hr_kpi_t_dim_employees') }} emp
  left join {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_phone_numbers') }} phn 
      on emp.[user_id] = phn.[person_id_external]
      and phn.is_primary = 1
      and phn.country_code = '9696'
where 1=1
      and emp.employee_status = N'AKTİF'
  -- and a_level = N'RÖNESANS HOLDİNG A.Ş.'