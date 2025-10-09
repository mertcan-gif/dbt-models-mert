{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}
 
WITH project_company_mapping AS (
  SELECT
    name1
    ,WERKS
    ,w.BWKEY
    ,bukrs
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
  )
 
SELECT
  rls_region
  ,rls_group
  ,rls_company
  ,rls_businessarea = CONCAT(m.werks , '_' , c.rls_region)
  ,f.*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM {{ ref('stg__infra_kpi_t_fact_stock') }} f
  LEFT JOIN project_company_mapping m ON f.business_area = m.werks
  LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} c ON c.RobiKisaKod = m.bukrs
--WHERE rls_region IS NOT NULL
