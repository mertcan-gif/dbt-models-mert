{{
  config(
    materialized = 'table',tags = ['budget_kpi','rmore'] 
    )
}}

    select 
        _map.*
        ,fipex1 = level_1.fipex
        ,fipex2 = COALESCE(level_2.fipex, level_1.fipex)
        ,fipex3 = COALESCE(level_3.fipex, level_2.fipex, level_1.fipex)
        ,fipex4 = COALESCE(level_4.fipex, level_3.fipex, level_2.fipex, level_1.fipex)
    from {{ ref('stg__fi_kpi_t_dim_budgetmapping') }} _map
        left JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmci') }} level_1 ON _map.commitment_item_code = level_1.fipex
        left JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmci') }} level_2 ON level_1.fipup = level_2.fipex and level_1.fipup <>''
        left JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmci') }} level_3 ON level_2.fipup = level_3.fipex and level_2.fipup <>''
        left JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmci') }} level_4 ON level_3.fipup = level_4.fipex and level_3.fipup <>''
