{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
/*
	Verilen hizmetlerin miktar ve tutar bilgileri bulunmaktadır.
*/
WITH service_payment AS (
	SELECT
		rls_region = cm.RegionCode
		,rls_group = CONCAT(cm.KyribaGrup, '_', cm.RegionCode)
		,rls_company = CONCAT(cm.RobiKisaKod, '_', cm.RegionCode)
		,rls_businessarea = CONCAT(ksnesl.pspid, '_', cm.RegionCode)
		,company = ekpo.bukrs
		,businessarea = ksnesl.pspid
		,businessarea_name = t001w.name1
		,deduction_id = ksnesl.kesinti_id
		,material_no = ksnesl.matnr
		,material_name = makt.zzlongtx
		,quantity_type = ekpo.meins
		,service_quantity = cast(ekpo.menge as float)
		,service_amount = cast(ekpo.netwr as float)
		,currency = ekko.waers
		,document_date = CAST(ekko.bedat AS date)
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_001_t_ksnesl') }} ksnesl
		INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_proj') }} proj ON proj.pspid = ksnesl.pspid
		INNER JOIN (SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }} where spras = 'T') makt ON makt.matnr = ksnesl.matnr
		INNER JOIN (SELECT * FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }}
					WHERE 1=1
						AND loekz <> 'L' 
						AND idnlf IN ('MKSOZ', 'MKZYL', 'MKFYT', 'MKFFT', 'MKILV')) ekpo ON ekpo.matnr = RIGHT(ksnesl.matnr,8)
		INNER JOIN (SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekkn') }} where loekz <> '1') ekkn ON ekkn.ebeln = ekpo.ebeln
																												AND ekkn.ebelp = ekpo.ebelp
																												AND ekpo.werks = ksnesl.pspid
		INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }} ekko ON ekko.ebeln = ekpo.ebeln
		LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm ON cm.RobiKisaKod = ekpo.bukrs
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = ksnesl.pspid
)
SELECT 
	sp.*
FROM service_payment sp
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_001_t_ksnesl') }} ksnesl on sp.businessarea = ksnesl.pspid
																				and sp.deduction_id = ksnesl.kesinti_id
																				and sp.document_date >= CAST(ksnesl.gec_bas_trh as date)
																				and sp.document_date <= CAST(ksnesl.gec_bit_trh as date)
																				and material_no = ksnesl.matnr
WHERE ksnesl.pspid IS NOT NULL