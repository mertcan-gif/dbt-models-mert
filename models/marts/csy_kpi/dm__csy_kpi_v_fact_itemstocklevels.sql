{{
  config(
    materialized = 'table',tags = ['csy_kpi']
    )
}}
WITH final_cte as (
SELECT
    i.company,
    i.business_area,
    i.item_code,
    i.item_name,
    SUM(i.quantity) as quantity,
    i.warehouse_name
FROM {{ ref('stg__csy_kpi_v_fact_itemstocklevels') }} i
GROUP BY i.company,i.business_area,warehouse_name,item_code,item_name
)
SELECT
	rls_region,
	[rls_group]   = CONCAT(COALESCE(comp.KyribaGrup,''),'_',COALESCE(comp.RegionCode,'')) collate database_default,
	[rls_company] = CONCAT(COALESCE(f.company  ,''),'_'	,COALESCE(comp.RegionCode,''),'') collate database_default, 
	[rls_businessarea] = f.business_area,
    f.*,
    CASE 
        WHEN f.item_code in  (SELECT MATNR FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_marc') }} )
        THEN 'SARF' ELSE 'STOKLU' 
    END AS sarf_yada_stoklu_malzeme,
    t1.vtext as category,
    t1.prodh as category_code

FROM final_cte f
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies')}} comp on comp.[KyribaKisaKod]  = f.company  collate database_default
	LEFT JOIN {{ source("stg_s4_odata", "raw__s4hana_t_sap_mara") }} mra on mra.matnr = f.item_code
    LEFT JOIN ( SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t179t') }} WHERE spras = 'T' ) t1 on mra.prdha = t1.prodh
