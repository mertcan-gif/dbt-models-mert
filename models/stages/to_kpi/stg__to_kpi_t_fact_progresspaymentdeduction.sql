{{
  config(
    materialized = 'table',tags = ['to_kpi']
    )
}}
SELECT 
	rls_region = cm.RegionCode
	,rls_group = CONCAT(cm.KyribaGrup, '_', cm.RegionCode)
	,rls_company = CONCAT(cm.RobiKisaKod, '_', cm.RegionCode)
	,rls_businessarea = CONCAT(ekkn.gsber, '_', cm.RegionCode)
	,company = ekkn.bukrs
	,businessarea = ekkn.gsber
	,businessarea_name = t001w.name1
	,deduction_id = kesint.kesinti_id
	,deduction_name = kesint.kesinti_txt
	,transaction_quantity = CAST(kesint.zzislem_miktari AS FLOAT)
	,unit_cost =  CAST(kesint.zzbirim_fiyat AS FLOAT)
	,posting_date = CAST(zzkayit_tarihi AS DATE)
	,TRIM(UPPER(zzwaers)) currency
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_020_t_kesint') }} kesint
LEFT JOIN (SELECT
				DISTINCT ekkn.ebeln, gsber, ekpo.bukrs 
			 FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekkn') }} ekkn
			 LEFT JOIN aws_stage.s4_odata.raw__s4hana_t_sap_ekpo ekpo ON ekkn.ebeln = ekpo.ebeln 
																		AND ekkn.ebelp = ekpo.ebelp
			 WHERE fipos <> 'DUMMY') ekkn ON kesint.ebeln = ekkn.ebeln
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm ON cm.RobiKisaKod = ekkn.bukrs
LEFT JOIN ( SELECT 
				*
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_001_t_ksnesl') }} ) ksnesl ON ksnesl.kesinti_id = kesint.kesinti_id
																							AND ksnesl.pspid = ekkn.gsber
																							AND CAST(zzkayit_tarihi AS DATE) >=  CAST(ksnesl.gec_bas_trh AS DATE)
																							AND CAST(zzkayit_tarihi AS DATE) <=  CAST(ksnesl.gec_bit_trh AS DATE)
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = ekkn.gsber
WHERE 1=1
	AND del_flag <> 'X'
	AND ksnesl.kesinti_id IS NOT NULL