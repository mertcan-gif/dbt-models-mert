{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

SELECT 
   lg.[id]
  ,CAST([creation_time] AS DATETIME) AS creation_time
  ,lg.[user_id]
  ,lg.[workspace_id]
  ,lg.[report_id]
  ,lg.[report_type]
  ,lg.[consumption_method]
  ,rp.[name] AS report_name
  FROM {{ source('stg_powerbi_kpi', 'raw__powerbi_t_fact_viewreportlogs') }} lg
  LEFT JOIN {{ ref('stg__powerbi_kpi_t_dim_reports') }} rp ON lg.report_id = rp.id
  WHERE item_name <> 'nan'
