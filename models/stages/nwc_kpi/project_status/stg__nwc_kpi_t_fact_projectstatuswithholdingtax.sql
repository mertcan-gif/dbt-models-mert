
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','projectstatus']
    )
}}

  SELECT 
    RBUSA,
    SUM(HSL) AS withholding_tax_try,
    SUM(OSL) AS withholding_tax_usd,
    SUM(KSL) AS withholding_tax_eur
  FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
  WHERE 1=1
    AND RACCT LIKE '295%'
  GROUP BY 
    RBUSA