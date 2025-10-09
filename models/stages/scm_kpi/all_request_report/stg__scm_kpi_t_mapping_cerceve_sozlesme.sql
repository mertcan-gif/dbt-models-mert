{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}

    SELECT
        cowid,
        STRING_AGG(ebeln, ',') as ebeln
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zar_001_t_baslik') }}
    WHERE ebeln <> ''
    GROUP BY cowid