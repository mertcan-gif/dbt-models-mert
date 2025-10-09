{{
  config(
    materialized = 'table',tags = ['metadata_kpi','rmore_kpi']
    )
}}
  
SELECT 
    [id]
    ,CAST([created_at] AS DATETIME) AS creation_time
    ,[user_id] = username
    ,[workspace_id] = 
      CASE 
        WHEN event_name	LIKE 'detail%'	THEN CONCAT('RMORE - ',report_name)
        ELSE CONCAT('RMORE - ',item_name)
      END
    ,[report_id] = 
        CASE 
            WHEN event_name	LIKE 'detail%'	THEN CONCAT('RMORE - ',report_name,' - ',item_name)
        ELSE CONCAT('RMORE - ',report_name)
      END
    ,[report_type] = 'RMORE'
    ,[consumption_method] = ''
    ,1 AS transaction_amount
    ,REPLACE(report_name, 'r_applications', 'R Applications') AS report_name
FROM {{ source('stg_metadata_kpi', 'raw__rmore_kpi_t_fact_viewreportlogs') }}
WHERE 1=1 AND
  (event_name =  'page_view')
