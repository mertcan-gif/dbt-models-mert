{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

SELECT 
	rls_region = 'NAN'
	,rls_group = 'GR_0000_NAN'
	,rls_company = 'CO_0000_NAN'
	,rls_businessarea = 'BA_0000_NAN'
	,[id_code]
	,[username]
	,[user_type]
	,[user_group]
	,[first_name]
	,[last_name]
	,[full_name]
	,[company]
	,[mail]
	,[cost_center]
	,[date]
	--,[snapshot_date]
FROM (
	SELECT 
		*
		,ROW_NUMBER() OVER (PARTITION BY id_code, FORMAT(date, 'yyyy-MM') ORDER BY snapshot_date desc) rn
	FROM {{ ref('stg__incremental_kpi_t_fact_activesapuserlistsnapshots') }}
	) source 
WHERE rn = 1 

UNION

SELECT 
	rls_region = 'NAN'
	,rls_group = 'GR_0000_NAN'
	,rls_company = 'CO_0000_NAN'
	,rls_businessarea = 'BA_0000_NAN'
	,[id_code]
	,[username]
	,[user_type]
	,[user_group]
	,[first_name]
	,[last_name]
	,[full_name]
	,[company]
	,[mail]
	,[cost_center]
	,[date]
	--,[snapshot_date] = CAST(GETDATE() AS date)
FROM {{ ref('stg__hr_kpi_t_fact_activesapuserlist') }}
WHERE date = CAST(GETDATE() AS DATE)