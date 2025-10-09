{{
  config(
    materialized = 'view',tags = ['uygulama_gelistirme'],grants = {'select': ['s4hana_ug_user']}
    )
}}

select *
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }}
