{{
  config(
    materialized = 'table',tags = ['fms_kpi', 'opet']
    )
}}

WITH raw_data as (
SELECT
    CONVERT(datetime, SWITCHOFFSET([SaleEnd], '+03:00')) AS sales_date
    ,[StationName] AS station_name
    ,[FleetName] AS fleet_name
    ,CASE
      WHEN ISNUMERIC(SUBSTRING([LicensePlateNr], 1, 3)) = 1 AND SUBSTRING([LicensePlateNr], 1, 1) = '0'
      THEN STUFF([LicensePlateNr], 1, 1, '')
      ELSE [LicensePlateNr]
    END AS license_plate
    ,[ProductName] AS product_name
    ,[Total] AS total_amount
    ,[Volume] AS volume
    ,[UnitPrice] AS unit_price
    ,[CityName] AS city
    ,[RID]
    ,[db_upload_timestamp]
    ,ROW_NUMBER() OVER (PARTITION BY [LicensePlateNr], RID ORDER BY db_upload_timestamp desc) rn
  FROM {{ source('stg_fms_kpi', 'raw__fms_kpi_t_fact_opetfuelexpenses') }} )
 
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
FROM raw_data
WHERE rn=1