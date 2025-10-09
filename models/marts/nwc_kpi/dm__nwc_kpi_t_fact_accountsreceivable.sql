
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','arap','araptest']
    )
}}

SELECT *
FROM {{ ref('stg__nwc_kpi_t_fact_accountsreceivable') }}
