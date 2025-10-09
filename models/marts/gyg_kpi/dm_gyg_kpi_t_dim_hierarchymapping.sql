{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}

with final as (
SELECT
    [rls_region] = dim_comp.RegionCode
      ,[rls_group] = dim_comp.KyribaGrup + '_' + dim_comp.RegionCode
      ,[rls_company] = hiyerarsi.bukrs + '_' + dim_comp.RegionCode
      ,[rls_businessarea] = CONCAT('_', dim_comp.RegionCode)
      ,kyriba_grup = dim_comp.KyribaGrup
      ,hiyerarsi.bukrs as company_code
      ,hiyerarsi.zfi_cati as holding_company_cati
      ,hiyerarsi.zfi_grup as bussines_group_grup
      ,hiyerarsi.zfi_sektor as sector_sektor
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfi_sk_hiyerarsi') }} AS hiyerarsi
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} AS dim_comp 
    ON hiyerarsi.bukrs = dim_comp.RobiKisaKod
    where rls_region is not null 
)

SELECT
	rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
	,* 
FROM final