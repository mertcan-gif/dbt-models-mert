{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}

WITH log_data as (
	SELECT        
		cast([LOG].ID as nvarchar(max)) AS log_id, 
		[LOG].DATE AS creation_time, 
		LOWER(REPLACE(REPLACE(REPLACE(REPLACE([LOG].EMAIL, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), CHAR(160), '')) AS user_id, 
		SUBSTRING([LOG].DETAILS, 
		CHARINDEX(CHAR(32), [LOG].DETAILS), 
		CHARINDEX(CHAR(13), [LOG].DETAILS) - CHARINDEX(CHAR(32), [LOG].DETAILS)) AS report_id
	FROM {{ source('stg_metadata_kpi', 'stg__rnet_kpi_t_fact_rawlogs') }} [LOG]
)
select 
	log_id collate SQL_Latin1_General_CP1_CI_AS as log_id,
	creation_time,
	user_id collate SQL_Latin1_General_CP1_CI_AS as user_id,
	TRIM(report_id) collate SQL_Latin1_General_CP1_CI_AS as report_id,
	rp.report_name
from log_data
LEFT JOIN {{ ref('stg__rnet_kpi_t_dim_reports') }} rp ON TRIM(log_data.report_id) = rp.id COLLATE SQL_Latin1_General_CP1_CI_AS