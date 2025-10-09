
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','projectstatus']
    )
}}

SELECT 
  RBUSA = business_area,
  company,
  SUM([amount_in_tl]) cash_flow_try,
  SUM([amount_in_usd]) cash_flow_usd,
  SUM([amount_in_eur]) cash_flow_eur
FROM {{ ref('dm__nwc_kpi_t_fact_cashflow') }}
WHERE 1=1
  AND [type] <> N'KAR'
  AND [type] <> N'TEMETTÜ'
  AND [type] <> N'FAİZ NPV'
  AND [type] <> N'FAİZ REEL'
  AND [type] <> N'KDV DÜZELTME'
GROUP BY business_area,company