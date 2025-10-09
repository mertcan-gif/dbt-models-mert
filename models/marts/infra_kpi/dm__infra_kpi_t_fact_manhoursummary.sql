{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

SELECT 
	[company]
  ,[business_area]
  ,CAST([spent_man_hour] AS float) as spent_man_hour
  ,CAST([date] AS date) as date
  ,CAST(data_control_date AS date) AS data_control_date
FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_manhoursummary') }}
