{{
  config(
    materialized = 'view',tags = ['eff_kpi']
    )
}}
WITH SICIL_RLS_MATCHING AS (
	SELECT *
	FROM (
		SELECT 
			rls_region
			,rls_group
			,rls_company
			,rls_businessarea
			,global_id
			,employee_id
			,employee_status
			,dwh_worksite AS business_area
			,payroll_company_code
			,ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY event_reason,age DESC) AS RN	
		FROM  {{ ref('dm__hr_kpi_t_dim_hrall') }}
		WHERE language = 'EN'
	) RAW_DATA
	WHERE RAW_DATA.RN = 1
)

,REHIRE_TERMINATION_UNION AS (
SELECT
	[user_id]
	,[name_surname]
	,[event]
	,[event_reason]
	,[real_termination_reason] = ''
	,[event_date] = [rehire_date]
	,[event_type] = 'Rehire'
	,[first_date_at_ronesans]
	,[a_level_code]
	,[a_level_label]
	,[b_level_code]
	,[b_level_label]
	,[e_level_code]
	,[e_level_label]
	,[position_code]
	,[position_title]
	,[db_upload_timestamp]
FROM  {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_rehire') }}
	WHERE 1=1
		AND event_reason <> N'Emeklilik Sonrası'
		AND event_reason <> N'Norm Kadro'

UNION ALL

SELECT
	[user_id]
	,[name_surname]
	,[event]
	,[event_reason]
	,[real_termination_reason]
	,[termination_date]
	,[event_type] = 'Termination'
	,[first_date_at_ronesans]
	,[a_level_code]
	,[a_level_label]
	,[b_level_code]
	,[b_level_label]
	,[e_level_code]
	,[e_level_label]
	,[position_code]
	,[position_title]
	,[db_upload_timestamp]
FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_terminations') }}
)

,FINAL_UNION AS (
SELECT
     srm.rls_region
    ,srm.rls_group
    ,srm.rls_company
    ,srm.rls_businessarea
	,srm.employee_status
	,srm.global_id
	,business_area
	,payroll_company_code
	,[user_id]
	,[name_surname]
	,[event]
	,[event_reason]
	,[real_termination_reason]
	,[event_date] 
	,[event_type]
	,[first_date_at_ronesans]
	,[a_level_code]
	,[a_level_label]
	,[b_level_code]
	,[b_level_label]
	,[e_level_code]
	,[e_level_label]
	,[position_code]
	,[position_title]
	,[db_upload_timestamp]	
FROM REHIRE_TERMINATION_UNION rtu
	LEFT JOIN SICIL_RLS_MATCHING srm ON srm.employee_id = rtu.[user_id]
WHERE [user_id] IN (SELECT DISTINCT [user_id]
						FROM (
								SELECT *
									,ROW_NUMBER() OVER(PARTITION BY [user_id] ORDER BY [event_date] DESC) ROWNUM
								FROM REHIRE_TERMINATION_UNION 
								) RN 
						WHERE ROWNUM > 1)

	UNION ALL 				

/** Termination'u yansımamış fakat Rehire olan çalışanlar mevcut olduğundan, bu kişileri veride kaybetmemek adına Rehire sonradan DISTINCT
çekilecek şekilde tekrardan union edilir **/

SELECT
     srm.rls_region
    ,srm.rls_group
    ,srm.rls_company
    ,srm.rls_businessarea
	,srm.employee_status
	,srm.global_id
	,business_area
	,payroll_company_code
	,[user_id]
	,[name_surname]
	,[event]
	,[event_reason]
	,[real_termination_reason] = ''
	,[event_date] = [rehire_date]
	,[event_type] = 'Rehire'
	,[first_date_at_ronesans]
	,[a_level_code]
	,[a_level_label]
	,[b_level_code]
	,[b_level_label]
	,[e_level_code]
	,[e_level_label]
	,[position_code]
	,[position_title]
	,[db_upload_timestamp]
FROM  {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_rehire') }} rh
	LEFT JOIN SICIL_RLS_MATCHING srm ON srm.employee_id = rh.[user_id]
	WHERE 1=1
		{# AND event_reason <> N'Emeklilik Sonrası'
		AND event_reason <> N'Norm Kadro' #}

)

,final_cte as (
SELECT DISTINCT 
	rls_region
    ,rls_group
    ,rls_company
    ,rls_businessarea
	,employee_status
	,global_id
	,business_area
	,payroll_company_code
	,[user_id]
	,[name_surname]
	,[event]
	,[event_reason]
	,[real_termination_reason]
	,[event_date] 
	,[event_type]
	,[first_date_at_ronesans]
	,[a_level_code]
	,[a_level_label]
	,[b_level_code]
	,[b_level_label]
	,[e_level_code]
	,[e_level_label]
	,[position_code]
	,[position_title]
	,[db_upload_timestamp]	
FROM FINAL_UNION
)

,ranked_last_cte as (
select DISTINCT
	 t.*
	,ROW_NUMBER() over(partition by name_surname order by cast(event_date as date) asc) as rn_1
	,ROW_NUMBER() over(partition by name_surname order by cast(event_date as date) desc) as rn_2
from final_cte  t
)
select
*,
is_first_date = case when rn_1 = '1' then 'True' else 'False' end,
is_last_date = case when rn_2 = '1' then 'True' else 'False' end
from ranked_last_cte
where 1=1