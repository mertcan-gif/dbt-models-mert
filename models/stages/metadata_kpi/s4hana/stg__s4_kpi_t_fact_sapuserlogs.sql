{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}

WITH raw_data AS (
	SELECT 
		[SapId]
		,[TransactionCode]
		,[TransactionDate]
		,[TransactionCount]
		,ROW_NUMBER() OVER (PARTITION BY [SapID], [TransactionCode], [TransactionDate] ORDER BY	CAST(db_upload_timestamp AS date) DESC) rn
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_sapuserlog') }}
	)
SELECT 
	[SapId]
	,[TransactionCode]
	,[TransactionDate]
	,[TransactionCount]
FROM raw_data
where rn = '1'
