{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}
WITH RAW_DATA AS (
	select 
		sf.rls_region,
		sf.rls_group,
		sf.rls_company,
		sf.rls_businessarea,
		eposta_adresi,
		sf.[payroll_company_code] as [company],
		sf.KyribaGrup as [group],
		CONCAT(CASE 
					WHEN sac.middle_name IS NULL 
					THEN UPPER(COALESCE(sac.first_name_lat,sac.first_name)) 
					ELSE UPPER(CONCAT(COALESCE(sac.first_name_lat,sac.first_name),' ',COALESCE(sac.middle_name_lat,sac.middle_name))) 
					END,' ',UPPER(COALESCE(sac.last_name_lat,sac.last_name))
				) as full_name,
		grouped_title,
		sf.role as title,
		sf.country,
		ROW_NUMBER() OVER(PARTITION BY eposta_adresi ORDER BY dwh_date_of_recruitment DESC) DUPS,
		dwh_date_of_recruitment
	from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }}  sac
		left join (
			SELECT KyribaGrup,hr.*
			FROM {{ ref("dm__hr_kpi_t_dim_hrall") }} hr
				LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dc ON hr.[payroll_company_code] = dc.RobiKisaKod
			WHERE [language] = 'en' 
			) sf ON sf.global_id = cast(sac.global_id as nvarchar)
	where eposta_adresi IS NOT NULL 
),
final_data_real as ( 
	select 
		[rls_region],
		[rls_group],
		[rls_company],
		[rls_businessarea],
		LOWER(TRIM([eposta_adresi])) as email,
		[company],
		[group],
		[full_name],
		[grouped_title],
		[title],
		[country],
		priority = 1
	from RAW_DATA
	WHERE DUPS = 1
)
,
final_data_non_users_added AS (
	-- fact tablolarında olan user tablosunda olmayan userları buradan alıyoruz. look: stg__metadata_kpi_t_dim_allusersfromfacts
	select *
	from final_data_real
	UNION
	select 
		[rls_region] = NULL,
		[rls_group] = NULL,
		[rls_company] = NULL,
		[rls_businessarea] = NULL,
		LOWER(REPLACE(REPLACE(REPLACE(REPLACE(email, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), CHAR(160), '')) as email,
		[company] = NULL,
		[group] = NULL,
		[full_name] = NULL,
		[grouped_title] = NULL,
		[title] = NULL,
		[country] = NULL,
		priority = 2
	from {{ ref('stg__metadata_kpi_t_dim_allusersfromfacts') }}
)
, final_data_non_users_added_2 AS (
	select 
		*,
		ROW_NUMBER() OVER(PARTITION BY email ORDER BY [priority] asc) dups_deleter
	from final_data_non_users_added
)
SELECT 
		[rls_region],
		[rls_group],
		[rls_company],
		[rls_businessarea],
		email,
		COALESCE([company],'EXTERNAL') as company,
		COALESCE([group],'EXTERNAL') as [group],
		coalesce([full_name],'EXTERNAL') as full_name,
		coalesce([grouped_title],'EXTERNAL') as grouped_title,
		coalesce([title],'EXTERNAL') as [title],
		coalesce([country],'EXTERNAL') as [country]
FROM final_data_non_users_added_2
WHERE dups_deleter = 1