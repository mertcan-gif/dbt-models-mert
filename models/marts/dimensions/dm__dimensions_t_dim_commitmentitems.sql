{{
  config(
    materialized = 'table',tags = ['rmore','dimensions']
    )
}}

select 
	commitment_item_code = fipex
	,commitment_item_description = UPPER(mctxt)
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} 
where 1=1
	AND spras = 'TR'
	AND NOT (fipex = 'DUMMY' AND fikrs = 'RTOR')