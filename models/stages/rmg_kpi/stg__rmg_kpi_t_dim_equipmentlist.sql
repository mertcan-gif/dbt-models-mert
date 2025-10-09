{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

SELECT
	t001k.bukrs AS company,
    iloa.gsber AS business_area,
	t001w.name1 AS business_area_description,
	equipment_group = 'Ana Kategori',
    equi.objnr AS object_number,
	t370k.eartx AS object_type,
	equi.equnr AS equipment,
	zzplakano AS plate,
	zzitorhsshrfv AS registered_owner,
	zzmarka AS brand,
	zzmodel AS model,
	baujj AS model_year,
	equi.answt AS purchase_price
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equi') }} equi
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equz') }} equz on equi.equnr = equz.equnr
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t370k_t') }} t370k on equi.eqart = t370k.eqart
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa ON equz.iloan = iloa.iloan
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON iloa.gsber = t001w.werks
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k ON equz.iwerk = t001k.bwkey
WHERE 1=1
AND equz.iwerk = 'RMGM'
AND equz.datbi = '99991231'
AND equi.eqtyp = 'T'