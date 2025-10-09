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
    ,rls_businessarea = TRIM(business_area) + '_' + cm.RegionCode
    ,cm.KyribaGrup as [group]
    ,[company]
    ,TRIM(business_area) as [business_area]
    ,t.name1 as businessarea_name
    ,[budget_period]
    ,CAST([price_difference_coefficient] as float) AS price_difference_coefficient
    ,CAST([end_of_work_expense_eur] as money) * try_value as end_of_work_expense_try
    ,CAST([end_of_work_expense_eur] as money) * usd_value as end_of_work_expense_usd
    ,CAST([end_of_work_expense_eur] as money) AS end_of_work_expense_eur
    ,CAST([end_of_work_revenue_eur] as money) * try_value as end_of_work_revenue_try
    ,CAST([end_of_work_revenue_eur] as money) * usd_value as end_of_work_revenue_usd
    ,CAST([end_of_work_revenue_eur] as money) AS end_of_work_revenue_eur 
    ,CAST([profit_eur] AS money) * try_value AS profit_try
    ,CAST([profit_eur] AS money) * usd_value AS profit_usd
    ,CAST([profit_eur] AS money) AS profit_eur
    ,CAST([profit_percentage] AS FLOAT) AS profit_percentage
    ,CAST([content_cost_eur] AS money) * try_value AS content_cost_try
    ,CAST([content_cost_eur] AS money) * usd_value AS content_cost_usd
    ,CAST([content_cost_eur] AS money) AS content_cost_eur
    ,CAST([date] as DATE) AS date
    ,CAST([Data Control Date] AS DATE) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_budgetsummary') }} bs
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(bs.company) = cm.KyribaKisaKod
    LEFT JOIN {{ ref('dm__dimensions_t_dim_dailys4currencies') }} curr on curr.date_value = CAST(bs.[date] as date)
                                              and curr.currency = 'EUR'
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(bs.business_area) = t.werks
)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
