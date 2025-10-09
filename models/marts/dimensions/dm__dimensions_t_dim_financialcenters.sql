{{
  config(
    materialized = 'table',tags = ['rmore','dimensions']
    )
}}

select 
	financial_center_code = fictr
	,financial_center_description = UPPER(mctxt)
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfctrt') }}
where NOT (fictr = 'DUMMY' AND fikrs = 'RTOR')