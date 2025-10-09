{{
  config(
    materialized = 'view',tags = ['hr_kpi','hr_ug','personnel_locations'],grants = {'select': ['s4hana_ug_user']}
    )
}}

SELECT [user_id]
      ,[photo]
      ,[photo_id]
      ,[width]
      ,[height]
      ,[photo_type]
      ,[photo_name]
      ,[last_modified_datetime]
  FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employee_photo') }}
where photo_type <> '14'