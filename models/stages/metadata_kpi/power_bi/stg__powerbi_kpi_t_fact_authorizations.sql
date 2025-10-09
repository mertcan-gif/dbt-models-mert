{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

WITH dim_report_user_mapping AS (
	/* Principle Type: User */
	SELECT
		email_address,
		display_name,
		report_id,
		report_user_access_right,
		reporting_date
	FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_reportusers') }}
	WHERE 1=1	
		AND principal_type = 'User'
	UNION ALL
	/* Principle Type: Group*/
	SELECT
		dfm.email,
		dru.display_name,
		dru.report_id,
		dru.report_user_access_right,
		dru.reporting_date
	FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_reportusers') }} dru
		LEFT JOIN {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_reportusersfunctionalmatrix') }} dfm ON dru.report_id = dfm.report_id AND dru.reporting_date = dfm.reporting_date
	WHERE 1=1
		AND principal_type = 'Group'
		AND display_name = 'DWH_RLS_USERS'
)
SELECT 
	f.email_address,
	f.display_name,
	f.report_id,
	rep.name AS report_name,
	f.report_user_access_right,
	f.reporting_date,
	u.rls_profile,
	'Power BI' as segment
FROM dim_report_user_mapping f
    LEFT JOIN {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_userprofilemapping') }} u ON u.email = f.email_address and u.reporting_date = f.reporting_date
	LEFT JOIN {{ ref('dm__metadata_kpi_t_dim_reports') }} rep ON rep.id = f.report_id