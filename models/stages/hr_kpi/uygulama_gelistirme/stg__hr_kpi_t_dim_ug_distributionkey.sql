{{
  config(
    materialized = 'table',tags = ['hr_kpi','hr_ug','distrubiton_key'],grants = {'select': ['s4hana_ug_user']}
    )
}}

SELECT DISTINCT *
 
FROM (
select 
         first_name =  case when sac.middle_name IS NULL then sac.first_name else CONCAT(sac.first_name,' ',sac.middle_name) end
		,sac.last_name
		,[full_name] = CONCAT(CASE WHEN sac.middle_name IS NULL THEN UPPER(COALESCE(sac.first_name_lat,sac.first_name)) ELSE UPPER(CONCAT(COALESCE(sac.first_name_lat,sac.first_name),' ',COALESCE(sac.middle_name_lat,sac.middle_name))) END,' ',UPPER(COALESCE(sac.last_name_lat,sac.last_name)))
		,[user_id] as sf_id_number
		,[sac].person_id as employee_id
		,sap_id
		,[payroll_company] = UPPER(sac.bordro_sirketi_en)
		,[payroll_company_code] = sac.bordro_sirketi_kodu
		,costcenter_code
		,[eposta_adresi]  
		,snapshot_date
from {{ source('snapshots_hr_kpi', 'DWH_Stage_SF_SACRapor_Snapshots') }} sac
	LEFT JOIN  {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_group') }} grp ON grp.[group] = sac.[grup/baskanlik_en] 
where custom_region <> 'RU'
	and sac.[employee_status] = '663908'
	AND (sac.snapshot_date = CAST(GETDATE() AS DATE) 
		 or RIGHT(sac.snapshot_date,2) = '01')
	AND sac.snapshot_date >= '2024-05-01'
		) a 