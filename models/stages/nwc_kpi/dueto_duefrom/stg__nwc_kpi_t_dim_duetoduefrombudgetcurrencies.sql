{{
  config(
    materialized = 'table',tags = ['nwc_kpi','duetoduefrom']
    )
}}
	

SELECT
	gsber
	,pb
FROM (
	SELECT 
		gsber
		,pb
		,ROW_NUMBER() OVER(PARTITION BY gsber ORDER BY budget_year,budget_month DESC) AS RN
	FROM {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }}
) D
WHERE RN = 1
