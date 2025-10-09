{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

SELECT 
  [rls_region]
  ,[rls_group]
  ,[rls_company]
  ,[rls_businessarea]
  ,[email_address]
  ,[name_surname]
  ,[license_group]
  ,[license_type]
  ,[segment]
  ,[snapshot_date]
FROM {{ ref('dm__metadata_kpi_t_fact_licenses') }}
WHERE snapshot_date=DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
  AND rls_region <> 'RUS'