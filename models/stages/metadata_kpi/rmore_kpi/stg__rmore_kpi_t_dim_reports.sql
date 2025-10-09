{{ config( materialized = 'table', tags = ['metadata_kpi','rmore_kpi'] ) }}

WITH raw_data as (
  SELECT DISTINCT
    [id] = dim_reports.report_id,
    [report_type] = 'RMORE',
    [name] = dim_reports.report_id,
    [created_date_time] = '1900-01-01',
    [modified_date_time] = '1900-01-01',
    [modified_by] = 'RMORE',
    [created_by] = 'RMORE',
    [workspace_id] = dim_reports.workspace_id,
    sub_segment = dim_reports.workspace_id,
    segment = 'RMORE'
  FROM {{ ref('stg__rmore_kpi_t_fact_viewreportlogs') }} dim_reports
)

, dups as (
  SELECT
  *,
  ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) AS dups_flag
  FROM raw_data
)


Select 
    [id]
    ,[report_type] 
    ,[name]
    ,[created_date_time]
    ,[modified_date_time]
    ,[modified_by]
    ,[created_by]
    ,[workspace_id]
    ,sub_segment
    ,segment
from dups
where dups_flag = 1