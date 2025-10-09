{{
  config(
    materialized = 'table',tags = ['hr_kpi','hr_activepersonnel'],grants = {'select': ['UserAppLicense']})
}}

SELECT 
* 
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zhr_000_t_dwhlog') }}
