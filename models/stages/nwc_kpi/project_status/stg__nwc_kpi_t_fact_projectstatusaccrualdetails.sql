
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','projectstatus']
    )
}}

SELECT 
  company,
  business_area,
  business_area_description,
  SUM(amount_in_tl) revenue_realization_try,
  SUM(amount_in_usd) revenue_realization_usd,
  SUM(amount_in_eur) revenue_realization_eur
FROM {{ ref('dm__nwc_kpi_t_fact_costrealization') }} 
WHERE [type] <> 'KAR' --20.11.2023 - Burak Aydın Bey tahakkuk'taki filtrenin 'MERKEZ' olması gerektiğini iletti.
  AND [type] <> 'Faiz'
  AND [type] <> N'FAİZ'
  AND [type] <> N'GELIR'
  AND [type] <> N'GELİR'
  AND [type] <> 'GYG'
  AND [type] <> 'Hedge'
  AND [type] <> 'HURDA'
  AND [type] IS NOT NULL
  AND fiscal_period NOT IN ('13','14','15','16')
--WHERE [type] = 'MERKEZ'
GROUP BY company,business_area,business_area_description