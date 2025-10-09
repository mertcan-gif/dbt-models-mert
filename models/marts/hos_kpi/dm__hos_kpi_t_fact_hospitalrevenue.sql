{{
  config(
    materialized = 'table',tags = ['hos_kpi']
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
,company = c.RobiKisaKod
,businessarea = m.werks
,[IvBegda] AS ıvbegda
,[IvEndda] AS ıvendda
,[Iwerk] AS plant
,CONVERT(DATETIME, [Begda], 104) AS begda
,CONVERT(DATETIME, [Endda], 104) AS [endda]
,CAST([Gelir] AS float) AS revenue
,[Glrpb] AS currency
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hospitalrevenue') }} h 
LEFT JOIN project_company_mapping m ON h.[Iwerk] = m.werks
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} c ON c.RobiKisaKod = m.bukrs
