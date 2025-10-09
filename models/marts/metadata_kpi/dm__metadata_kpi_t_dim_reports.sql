{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}

SELECT 
    [id],
    [report_type],
    [name],
    [created_date_time],
    [modified_date_time],
    [modified_by],
    [created_by],
    [workspace_id],
    [sub_segment],
    [segment]
 FROM {{ ref('stg__powerbi_kpi_t_dim_reports') }}
 
 UNION ALL
 
SELECT 
    [id],
    'RNET' as [report_type],
    UPPER(report_name) as [name],
    '1900-01-01' as [created_date_time],
    '1900-01-01' as [modified_date_time],
    'RNET' as[modified_by],
    'RNET' as[created_by],
    'RNET' as[workspace_id],
    [sub_segment],
    [segment]
 FROM {{ ref('stg__rnet_kpi_t_dim_reports') }}

 UNION ALL 

 SELECT *
 FROM {{ ref('stg__s4_kpi_t_dim_reports') }}
 UNION ALL 

 SELECT *
 FROM {{ ref('stg__rmore_kpi_t_dim_reports') }}