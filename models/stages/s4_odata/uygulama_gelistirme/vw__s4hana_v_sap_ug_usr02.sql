{{
  config(
    materialized = 'view',tags = ['uygulama_gelistirme'],grants = {'select': ['s4hana_ug_user']}
    )
}}

SELECT *
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_usr02') }}
