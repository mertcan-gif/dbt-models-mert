{{
  config(
    materialized = 'table',tags = ['budget_kpi']
    )
}}
SELECT
    YEAR(CAST(budat AS DATE)) AS year_int
    ,MONTH(CAST(budat AS DATE)) as month_int
    ,masraf_yeri_mali_merkez_map.target1 as financial_center_code
    ,fipex as commitment_item_code
    ,amount_try = SUM(CAST(HSL AS MONEY))
    ,amount_usd = SUM(CAST(OSL AS MONEY))
    ,amount_eur = SUM(CAST(KSL AS MONEY))
from {{ ref('stg__s4hana_t_sap_acdoca') }}
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfmoarep1000012') }} masraf_yeri_mali_merkez_map 
        ON acdoca.rcntr BETWEEN masraf_yeri_mali_merkez_map.sour1_from AND masraf_yeri_mali_merkez_map.sour1_to
GROUP BY
    YEAR(CAST(budat AS DATE))
    ,MONTH(CAST(budat AS DATE))
    ,fistl 
    ,fipex

