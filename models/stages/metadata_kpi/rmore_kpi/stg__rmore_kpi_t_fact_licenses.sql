{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

SELECT 
	email_address = [EmailAddress]
	,'RMORE' AS license_group
	,'RMORE' AS license_type
	,'RMORE' AS segment
	,snapshot_date = CAST([snapshot_date] as date)
FROM {{ source('stg_metadata_kpi','raw__rmore_t_dim_rmorelicenses') }}
WHERE [EmailAddress] NOT LIKE '%test%'