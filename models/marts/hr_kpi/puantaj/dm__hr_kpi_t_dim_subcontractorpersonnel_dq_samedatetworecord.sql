{{
  config(
    materialized = 'table',tags = ['hr_kpi_puantaj']
    )
}}
WITH ValidDates AS (
    SELECT 
        [sap_id],
        [rls_region],
        [rls_company],
        [rls_businessarea],
        [rls_group],
        [full_name],
        [start_date],
        CASE 
            WHEN transaction_distribution = N'İşten ayrılma' THEN [start_date]
            WHEN [end_date] = '9999-12-31' AND transaction_distribution <> N'İşten ayrılma' THEN CAST(GETDATE() AS DATE)
            ELSE [end_date]
        END AS end_date,
        [transaction_distribution],
        [position],
        [statu],
        [direct_indirect],
        [blue_white_collar],
        [project],
        [year_of_seniority],
        [gender],
        [production_class],
        [age],
        [leaving_work],
        [nationality],
        [team_code],
        [team_based],
        [country],
        [transportation],
        [accommodation],
        [education],
        [company_class],
        [main_discipline],
        [sub_subcontractor],
        [subcontractor],
        [employee_group],
        [personnel_subfield],
		[reason_for_termination_code],
      	[leaving_reason],
        [task_type],
        [location],
        [business_area],
        [db_upload_timestamp],
        [group],
        [company],
        [name]
    FROM {{ ref('dm__hr_kpi_t_dim_subcontractorpersonnel') }}
)
, ExpandedDates AS (
    SELECT 
        v.[sap_id],
        v.[rls_region],
        v.[rls_company],
        v.[rls_businessarea],
        v.[rls_group],
        v.[full_name],
        DATEADD(DAY, n, v.start_date) AS transaction_date,
        v.start_date,  -- Include start_date in the output
        v.end_date,
        v.transaction_distribution,
        v.position,
        v.statu,
        v.direct_indirect,
        v.blue_white_collar,
        v.project,
        v.year_of_seniority,
        v.gender,
        v.production_class,
        v.age,
        v.leaving_work,
        v.nationality,
        v.team_code,
        v.team_based,
        v.country,
        v.transportation,
        v.accommodation,
        v.education,
        v.company_class,
        v.main_discipline,
        v.sub_subcontractor,
        v.subcontractor,
        v.employee_group,
        v.personnel_subfield,
		v.[reason_for_termination_code],
      	v.[leaving_reason],
        v.task_type,
        v.location,
        v.business_area,
        v.db_upload_timestamp,
        v.[group],
        v.[company],
        v.[name]
    FROM ValidDates v
    CROSS APPLY (
        SELECT TOP (CASE WHEN DATEDIFF(DAY, v.start_date, v.end_date) >= 0 THEN DATEDIFF(DAY, v.start_date, v.end_date) + 1 ELSE 1 END)
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM master.dbo.spt_values
    ) AS Numbers
),

deduplication as 
(
SELECT 
    [rls_region],
    [rls_company],
    [rls_businessarea],
    [rls_group],
	rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group),
    [sap_id],
    CAST([transaction_date] AS DATE) AS [transaction_date], 
    [start_date],  
	[full_name],
    [end_date],
    [transaction_distribution],
    [position],
    [statu],
    [direct_indirect],
    [blue_white_collar],
    [project],
    [year_of_seniority],
    [gender],
    [production_class],
    [age],
    [leaving_work],
    [nationality],
    [team_code],
    [team_based],
    [country],
    [transportation],
    [accommodation],
    [education],
    [company_class],
    [main_discipline],
    [sub_subcontractor],
    [subcontractor],
    [employee_group],
    [personnel_subfield],
	[reason_for_termination_code],
    [leaving_reason],
    [task_type],
    [location],
    [business_area],
    [db_upload_timestamp],
    [group],
    [company],
    [name],
	[join_key]=CONCAT(transaction_date, '_', sap_id)
FROM ExpandedDates
),

final as 
(
    select *,
    deduplication_key=ROW_NUMBER() OVER(partition by [join_key] ORDER BY transaction_date)
    from deduplication
        where 1=1 
        and sap_id not in ('01064845')
        and transaction_date >= '2024-01-01'
        and personnel_subfield IN (select 
                                    distinct btrtl 
                                    from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_uypaat') }}) )

/* Blart personel alt alanini gosteriyor. Alt alani olmayan personeller rapora yansitilmayacak. */

,failed_records_dedup as 
(
    select * 
    from final 
    where 1=1
        and deduplication_key>1
        and transaction_date<=cast(getdate() as date)
)

select 
rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
,*
from {{ ref('dm__hr_kpi_t_dim_subcontractorpersonnel') }}
where sap_id in (
    select distinct sap_id from failed_records_dedup
)