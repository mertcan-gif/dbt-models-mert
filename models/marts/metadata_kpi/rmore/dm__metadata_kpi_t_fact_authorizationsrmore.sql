{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

--RNET datası getdate ile geldiği için aynı tablo farklı filtrelerle unionlanmistir.
SELECT 
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea,
    email_address,
    name_surname,
    report_id,
    report_name,
    report_user_access_right,
    CASE 
        WHEN CAST(reporting_date AS DATE) = CAST(GETDATE() AS DATE) 
        THEN DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
        ELSE CAST(reporting_date AS DATE)
    END AS reporting_date,
    segment
FROM {{ ref('dm__metadata_kpi_t_fact_authorizations') }}
WHERE rls_region <> 'RUS' 
  AND (
       (CAST(reporting_date AS DATE) = DATEADD(DAY, -1, CAST(GETDATE() AS DATE)) and segment = 'Power BI') 
       OR 
       (CAST(reporting_date AS DATE) = CAST(GETDATE() AS DATE))
	)
