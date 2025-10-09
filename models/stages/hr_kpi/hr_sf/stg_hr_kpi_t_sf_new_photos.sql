{{
  config(
    materialized = 'table',tags = ['sf_new_api']
    )
}}
select 
	user_id,
	CASE WHEN photo = '' then 'No Photo' else photo end as photobase64
from (
	select user_id,
        photo,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY LEN(photo) DESC) row_ranker
	from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employee_photo') }}
	    WHERE 
        user_id IN (select user_id from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employee_photo') }}) 
    and user_id not in 
        (
                '47005234',
                '47048328',
                '47064027',
                '47064751',
                '47064905',
                '47066735',
                '47066780',
                'GLB312436',
                'GLB41976',
                'GLB42033',
                'GLB460829',
                'GLB8595'
            ) 
    and LEN(photo)<=30000
	AND height>width
	) raw
where row_ranker = 1
 
UNION ALL
 
SELECT 
	user_id,
	CASE WHEN photo = '' then 'No Photo' else photo end as photobase64
from (
	select user_id,
        photo,
        ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY LEN(photo) DESC) row_ranker
	from {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employee_photo') }} 
	    WHERE 
        user_id IN (
            '47005234',
            '47048328',
            '47064027',
            '47064751',
            '47064905',
            '47066735',
            '47066780',
            'GLB312436',
            'GLB41976',
            'GLB42033',
            'GLB460829',
            'GLB8595'
        ) 
        and LEN(photo)<=30000
	) raw
where row_ranker = 1