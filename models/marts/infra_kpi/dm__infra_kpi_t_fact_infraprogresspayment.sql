{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}
WITH _raw AS (
  SELECT
    rls_region = cm.RegionCode
    ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
    ,rls_company = cm.KyribaKisaKod + '_' + cm.RegionCode
    ,rls_businessarea = TRIM(ipp.[businessarea]) + '_' + cm.RegionCode
    ,cm.KyribaGrup as [group]
    ,[company]
    ,ipp.[businessarea]
    ,[project]
    ,CAST([active_subcontractor] AS INT) [active_subcontractor]
    ,CAST([finalized_subcontractors_count] AS INT) [finalized_subcontractors_count]
    ,CAST([total_progress_payment] AS INT) [total_progress_payment_count]
    ,CAST([reporting_period_progress_payment] AS INT) [reporting_period_progress_payment_count]
    ,CAST([contract_tl] AS money) [contract_tl]
    ,CAST([contract_usd] AS money) [contract_usd]
    ,CAST([contract_eur] AS money) [contract_eur]
    ,CAST([contract_gbp] AS money) [contract_gbp]
    ,CAST([last_month_progress_payment_tl] AS money) [last_month_progress_payment_tl]
    ,CAST([last_month_progress_payment_usd] AS money) [last_month_progress_payment_usd]
    ,CAST([last_month_progress_payment_eur] AS money) [last_month_progress_payment_eur]
    ,CAST([last_month_progress_payment_gbp] AS money) [last_month_progress_payment_gbp]
    ,CAST([reporting_period_progress_payment_tl] AS money) [reporting_period_progress_payment_tl]
    ,CAST([reporting_period_progress_payment_usd] AS money) [reporting_period_progress_payment_usd]
    ,CAST([reporting_period_progress_payment_eur] AS money) [reporting_period_progress_payment_eur]
    ,CAST([reporting_period_progress_payment_gbp] AS money) [reporting_period_progress_payment_gbp]
    ,CAST([date] as date) date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_infraprogresspayment') }} ipp
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(ipp.company) = cm.KyribaKisaKod
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(ipp.project) = t.werks
)
SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
