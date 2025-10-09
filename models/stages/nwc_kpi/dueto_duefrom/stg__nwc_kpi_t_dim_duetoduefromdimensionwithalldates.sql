{{
  config(
    materialized = 'table',tags = ['nwc_kpi','duetoduefrom']
    )
}}
	
WITH DATE_TABLE as
(
	SELECT DISTINCT
		EOMONTH(DATEADD(MONTH, number, '2022-12-31')) AS end_of_month
	FROM 
		master..spt_values
	WHERE 
		type = 'P' 
		AND DATEADD(MONTH, number, '2023-01-01') <= GETDATE()
)
,

DIMENSION_WITH_ALL_DATES AS (
	SELECT DISTINCT  
		date_dim.end_of_month 
		,date_dim.bukrs
		,date_dim.gsber
		,budget_currency = CASE WHEN rb.pb IS NULL THEN 'TRY' ELSE rb.pb END
	FROM (SELECT DISTINCT  
				end_of_month 
				,gsber 
				,bukrs
				from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtibudgets') }} 
				CROSS JOIN DATE_TABLE dt) date_dim
			LEFT JOIN {{ ref('stg__nwc_kpi_t_dim_duetoduefrombudgetcurrencies') }}  rb ON rb.gsber = date_dim.gsber 

)

SELECT *, AnaHesap = 'kümülatif_maliyet' FROM DIMENSION_WITH_ALL_DATES
UNION ALL 
SELECT *, AnaHesap = 'kümülatif_gelir' FROM DIMENSION_WITH_ALL_DATES
