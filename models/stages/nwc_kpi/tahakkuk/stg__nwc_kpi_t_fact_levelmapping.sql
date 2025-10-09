
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','tahakkuk']
    )
}}

select 
	level_1 = LEFT(fipex,2) +'000000',
	level_2 = LEFT(fipex,4) +'0000',
	level_3 = LEFT(fipex,6) +'00',
	level_4 = fipex
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} fmcit
where LEN(fipex) = 8 AND spras = 'TR' 
