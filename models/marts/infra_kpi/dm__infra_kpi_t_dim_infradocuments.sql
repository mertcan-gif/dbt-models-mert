{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}

WITH _raw AS (
  SELECT
  	 rls_region = cm.RegionCode
    ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
    ,rls_company = cm.KyribaKisaKod + '_' + cm.RegionCode
    ,rls_businessarea = TRIM(id.[İş Alanı]) + '_' + cm.RegionCode
    ,cm.KyribaGrup as [group]
    ,TRIM(id.[İş Alanı]) as business_area
    ,t.name1 AS business_area_descrition
    ,TRIM([Kategori]) as document_category
    ,TRIM([Doküman Adı]) as document_name
    ,TRIM([Link]) as document_link
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_infradocuments') }} id
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on 'REC' = cm.KyribaKisaKod
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t ON TRIM(id.[İş Alanı]) = t.werks
)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw
