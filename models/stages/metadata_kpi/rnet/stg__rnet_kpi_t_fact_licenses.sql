{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

SELECT 
    EMAIL collate SQL_Latin1_General_CP1_CI_AS  AS user_id,
    N'RNet' collate SQL_Latin1_General_CP1_CI_AS AS license_group,
    CASE O.TYPE
        WHEN 0 THEN N'Normal'
        WHEN 1 THEN N'Özel'
        WHEN 2 THEN N'Sistem'
    END AS license_type,
    EMPLOYEMENTSTART AS license_start_date,
    EMPLOYEMENTEND AS license_end_date,
    'NON-ERP' as segment
FROM EBA.EBA.dbo.OSUSERS AS O WITH (NOLOCK)
WHERE (STATUS = 1) 