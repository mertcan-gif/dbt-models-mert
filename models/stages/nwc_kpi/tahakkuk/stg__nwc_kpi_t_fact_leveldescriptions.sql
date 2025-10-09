
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','tahakkuk']
    )
}}


select 
	code = fictr,
	case 
	  when len(fictr) = 8 and right(fictr,6)='000000' then 'level_1'
	  when len(fictr) = 8 and right(fictr,4)='0000' then 'level_2'
	  when len(fictr) = 8 and right(fictr,2)='00' then 'level_3'
	else 'other' end
	as level_of_category,
	description = bezeich
 from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfctrt') }}  
where spras = 'TR' and fictr <> 'DUMMY' 

UNION ALL

select
    code = fipex,
	'level_4' as level_of_category,
    description = text1
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }} fmcit
where spras = 'TR' and fipex <> 'DUMMY' 