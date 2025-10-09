{{
  config(
    materialized = 'table',tags = ['fms_kpi', 'shell']
    )
}}

WITH shell_raw_data AS (
SELECT
	UPPER([Customer_name]) AS fleet_name
	,CASE
		WHEN ISNUMERIC(SUBSTRING([Plate_cd], 1, 3)) = 1 AND SUBSTRING([Plate_cd], 1, 1) = '0'
		THEN STUFF([Plate_cd], 1, 1, '')
		ELSE [Plate_cd]
	END AS license_plate
	,[Retail_outlet_name] AS station_name
	,CAST([Transaction_date] AS datetime) AS sales_date  
	,[Unit_price] AS unit_price
	,[Volume] AS volume
	,SUBSTRING([Fuel_name], 1, CHARINDEX(' ', [Fuel_name]) - 1) AS product_name
	,[Sales_total_amount] AS total_amount
	,UPPER([Rtl_otlt_province]) AS city
	,ROW_NUMBER () OVER (PARTITION BY Plate_cd, Transaction_date ORDER BY db_upload_timestamp DESC) rn
FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_shellfuelexpenses') }})

SELECT
	fleet_name
	,license_plate
	,station_name
	,sales_date
	,unit_price
	,volume
	,product_name
	,total_amount
	,city
FROM shell_raw_data
WHERE rn=1
