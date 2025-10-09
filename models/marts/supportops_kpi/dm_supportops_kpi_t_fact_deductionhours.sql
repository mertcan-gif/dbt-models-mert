{{
  config(
    materialized = 'table',tags = ['supportops_kpi']
    )
}}	

/*
Filtre olarak verilen firmalar Tuğba Hanım'ın yönlendirilmesiyle eklenmiştir.
    Satıcı	Satıcı
    1009543	KSK TEMİZLİK TURİZM VE ORGANİZASYON
    1014157	ERAL TEMİZLİK LOJİSTİK İNŞAAT
    1031761	MCK İÇ VE DIŞ TİCARET YAPI İNŞAAT S
    1002027	DOMİNO ENTEGRE HİZMET YÖNETİMİ LOJİ
    1034132	SET ENTEGRE HİZMET YÖNETİMİ LOJİSTİ
    1038038	REAL DESTEK HİZMETLERİ İNŞ. TİC.LTD
*/


WITH project_company_mapping AS (
	SELECT
		name1
		,WERKS
		,w.BWKEY
		,bukrs
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
)

,deduction_union AS (
	SELECT 
		pernr
		,werks
		,[projead]
		,zekipno
		,[zztaseronno]
		,[personeltc]
		,datum
		,butcekodu1
		,[pozkodu1]
		,CASE 
			WHEN [poztnm1] = '' THEN 'NOT FILLED'
			ELSE [poztnm1]
		 END AS poztnm1
		,CAST(zzikgrs AS float) AS total_man_hour
		,kesintifirma1
		,CAST(kesintisaat1 AS float) AS kesintisaat1
		,kesintifirma2
		,CAST(kesintisaat2 AS float) AS kesintisaat2
		,kesintifirma3
		,CAST(kesintisaat3 AS float) AS kesintisaat3
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_cs_log') }}
	where 1=1
	and (pozkodu1 <> '' and pozkodu2 = '')

	UNION ALL

	SELECT 
		pernr
		,werks
		,[projead]
		,zekipno
		,[zztaseronno]
		,[personeltc]
		,datum
		,butcekodu2
		,[pozkodu2]
		,CASE 
			WHEN [poztnm2] = '' THEN 'NOT FILLED'
			ELSE [poztnm2]
		 END AS poztnm2
		,CAST(zzikgrs AS float) AS total_man_hour
		,kesintifirma1
		,CAST(kesintisaat1 AS float) AS deduction_hours_1
		,kesintifirma2
		,CAST(kesintisaat2 AS float) AS deduction_hours_2
		,kesintifirma3
		,CAST(kesintisaat3 AS float) AS deduction_hours_3
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_cs_log') }}
	where 1=1
	and (pozkodu2 <> '' and pozkodu1 = '')

	UNION ALL

	SELECT
		pernr,
		werks,
		projead,
		zekipno,
		zztaseronno,
		personeltc,
		datum,
		v.butcekodu,
		v.poz_kodu,
		--v.poz_tnm,
		CASE 
			WHEN v.poz_tnm = '' THEN 'NOT FILLED'
			ELSE v.poz_tnm
		 END AS poz_tnm,
		CAST(zzikgrs AS float) AS total_man_hour,
		kesintifirma1,
		CAST(kesintisaat1 AS float) / 2 AS deduction_hours_1,
		kesintifirma2,
		CAST(kesintisaat2 AS float) / 2 AS deduction_hours_2,
		kesintifirma3,
		CAST(kesintisaat3 AS float) / 2 AS deduction_hours_3
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_cs_log') }} t
	CROSS APPLY (
		VALUES
			(t.pozkodu1, t.poztnm1, t.butcekodu1),
			(t.pozkodu2, t.poztnm2, t.butcekodu2)
	) v(poz_kodu, poz_tnm, butcekodu)
	WHERE (t.pozkodu1 <> '' AND t.pozkodu2 <> '')

	UNION ALL

	SELECT
		pernr,
		werks,
		projead,
		zekipno,
		zztaseronno,
		personeltc,
		datum,
		t.butcekodu1,
		t.pozkodu1,
		'NOT FILLED' AS poz_tnm,
		CAST(zzikgrs AS float) AS total_man_hour,
		kesintifirma1,
		CAST(kesintisaat1 AS float) / 2 AS deduction_hours_1,
		kesintifirma2,
		CAST(kesintisaat2 AS float) / 2 AS deduction_hours_2,
		kesintifirma3,
		CAST(kesintisaat3 AS float) / 2 AS deduction_hours_3
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_cs_log') }} t
	WHERE (t.pozkodu1 = '' AND t.pozkodu2 = '')

	)
, final_cte AS (
	SELECT 
		rls_region = COALESCE(cm.RegionCode, 'NAR'),
		rls_group = CONCAT(cm.KyribaGrup, '_', cm.RegionCode),
		rls_company = CONCAT(cm.RobiKisaKod, '_', cm.RegionCode),
		rls_businessarea = CONCAT(du.werks, '_', cm.RegionCode),
		pernr AS personnel_number,
		du.werks AS business_area_code,
		t001w.name1 AS project_name,
		du.zekipno AS equip_number,
		du.zztaseronno AS subcontractor_no,
		lfa.name1 AS subcontractor_name,
		du.personeltc AS national_id_number,
		CAST(du.datum AS DATE) AS transaction_date,
		du.butcekodu1 AS budget_number,
		CASE	
			WHEN du.butcekodu1 = '13010120' THEN N'TEMİZLİK'
			WHEN du.butcekodu1 = '06010480' THEN N'TAMİR/TADİLAT'
			WHEN du.butcekodu1 = '13010110' THEN N'DESTEK HİZMETLER'
			WHEN du.butcekodu1 = '14050120' THEN N'ISG'
			WHEN du.butcekodu1 IN ('02100120', '13040110') THEN N'MAKİNA EKİPMAN'
			WHEN du.butcekodu1 IN ('01010350', '01040130', '02040210', 
							'03020180', '04010190', '05020140', 
							'06010110', '07020320', '08011010', 
							'09010810', '12010450', '14070110') THEN N'DİREKT İMALATLAR'
			ELSE du.butcekodu1
		END AS budget_group,
		du.pozkodu1 AS work_item,
		du.poztnm1 AS work_item_description,
		total_man_hour,
		real_company = N'Rönesans',
		CASE
			WHEN kesintifirma1 = N'Rönesans' THEN CAST(kesintisaat1 AS float)
			WHEN cast(total_man_hour as float) > (CAST(kesintisaat1 AS float) + CAST(kesintisaat2 AS float) + CAST(kesintisaat3 AS float))
			THEN cast(total_man_hour as float) - (CAST(kesintisaat1 AS float) + CAST(kesintisaat2 AS float) + CAST(kesintisaat3 AS float))
		END AS real_company_hours,
		CASE
			WHEN kesintifirma1 = N'Rönesans' THEN ''
			ELSE kesintifirma1
		END AS deduction_company_1,
		CASE
			WHEN kesintifirma1 = N'Rönesans' THEN ''
			ELSE kesintisaat1
		END AS deduction_hours_1,
		kesintifirma2 AS deduction_company_2,
		kesintisaat2 AS deduction_hours_2,
		kesintifirma3 AS deduction_company_3,
		kesintisaat3 AS deduction_hours_3
	FROM deduction_union du
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w on du.werks = t001w.werks
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa on lfa.lifnr = du.zztaseronno
	LEFT JOIN project_company_mapping pcm ON pcm.werks = du.werks
	LEFT JOIN {{ source('stg_dimensions','raw__dwh_t_dim_companymapping') }} cm on cm.RobiKisaKod = pcm.bukrs
	)

	SELECT 
		rls_region,
		rls_group,
		rls_company,
		rls_businessarea,
		personnel_number,
		fc.business_area_code,
		project_name,
		ekp.btrtl AS team_code,
		ekp.tanim as team_description,
		fc.equip_number,
		ekp.approver1 AS manager_id,
		CONCAT(emp.name, ' ', emp.surname) AS manager_fullname,
		subcontractor_no,
		subcontractor_name,
		national_id_number,
		transaction_date,
		budget_number,
		budget_group,
		work_item,
		work_item_description,
		total_man_hour,
		real_company,
		real_company_hours,
		deduction_company_1,
		deduction_company_1_name = lfa.name1,
		deduction_hours_1,
		deduction_company_2,
		deduction_company_2_name = lfa2.name1,
		deduction_hours_2,
		deduction_company_3,
		deduction_company_3_name = lfa3.name1,
		deduction_hours_3
	FROM  final_cte fc
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa ON fc.deduction_company_1 = lfa.lifnr
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa2 ON fc.deduction_company_2 = lfa2.lifnr
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa3 ON fc.deduction_company_3 = lfa3.lifnr
	LEFT JOIN project_company_mapping pcm ON pcm.werks = fc.business_area_code
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zhr_ekip_krilim2') }} ekp on ekp.werks = pcm.bukrs
                                                                        and ekp.ekipno = fc.equip_number
                                                                        and fc.transaction_date BETWEEN CAST(ekp.begda as date) AND CAST(ekp.endda as date)
	LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp on emp.sap_id = ekp.approver1
	WHERE 1=1
		and subcontractor_no IN (
							 '1009543','1014157'
							,'1031761','1002027'
							,'1034132','1038038'
							)
		--and transaction_date >= '2025-09-01'