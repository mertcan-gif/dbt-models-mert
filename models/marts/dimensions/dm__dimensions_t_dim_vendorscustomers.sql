{{
  config(
    materialized = 'table',tags = ['rmore','dimensions']
    )
}}

WITH vendors_and_customers as (
  SELECT
    vendor_customer_code = lifnr
    ,vendor_customer_name = name1
    ,db_upload_timestamp
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }}  

  UNION ALL

  SELECT
    vendor_customer_code = kunnr
    ,vendor_customer_name = name1
    ,db_upload_timestamp
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} 

)

,row_numbers as (
  SELECT
    vendor_customer_code
    ,vendor_customer_name
    ,ROW_NUMBER() OVER(PARTITION BY vendor_customer_code ORDER BY db_upload_timestamp DESC) RN
    from vendors_and_customers
	)

SELECT 
  vendor_customer_code
  ,vendor_customer_name
FROM row_numbers
WHERE RN = 1
