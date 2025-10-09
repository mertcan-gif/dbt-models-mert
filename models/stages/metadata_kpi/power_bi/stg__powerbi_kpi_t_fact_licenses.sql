{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

SELECT 
  [EmailAddress] as [email_address]
  ,'POWER BI' as segment
  ,snapshot_date
FROM  {{ source('stg_powerbi_kpi', 'raw__powerbi_t_dim_powerbilicenses') }}
where 1=1
