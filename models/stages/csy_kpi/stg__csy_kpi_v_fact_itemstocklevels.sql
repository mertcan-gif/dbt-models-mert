{{
  config(
    materialized = 'table',tags = ['csy_kpi']
    )
}}	
WITH makt as (
SELECT 
	 REPLACE(LTRIM(REPLACE(matnr, '0', ' ')), ' ', '0') AS matnr
    ,[spras]
    ,[maktx]
    ,[maktg]
    ,[zzlongtx]
    ,[db_upload_timestamp]
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }} m)
SELECT
  s.bukrs as company,
  s.werks as business_area,
	s.matnr as item_code,
	m.maktx as item_name,
	CAST(s.stockqty AS FLOAT) as quantity,
	t0.lgobe as warehouse_name
FROM 
	{{ source('stg_s4_odata', 'raw__s4hana_t_sap_matdoc') }}  as s
	LEFT JOIN makt m on m.matnr = s.matnr and m.spras= 'T'
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001l') }} as t0 on concat(t0.werks,'_',t0.lgort) = CONCAT(s.werks,'_',s.lgort)
WHERE 1=1
	  AND s.matnr is not null