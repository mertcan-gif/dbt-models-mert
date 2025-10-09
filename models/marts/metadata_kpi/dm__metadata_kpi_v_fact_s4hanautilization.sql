{{
  config(
    materialized = 'view',tags = ['metadata_kpi']
    )
}}

WITH main_projects AS (
	SELECT 
		bukrs,
		werks,
		name1
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = t001k.bwkey
	where t001w.fabkl = 'TR' 
),

/*********** KULLANIM GEREKLİLİKLERİNİN BULUNMASI *************/
	co_1 as (
		--CO: Araç Gider Takibi
		SELECT distinct csks.gsber
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }} aufk
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_csks') }} csks ON aufk.kostv = csks.kostl
		WHERE auart = 'R001'
	),

	co_2 as (
		--CO: Ay Sonu Bina Servis Dağıtım
		SELECT werks
		FROM main_projects
		WHERE 1=1
		AND bukrs in ('HOL', 'REC', 'RMH', 'RET', 'REN', 'TBO', 'RGY')
		AND werks like N'%M'
	),

	co_3 as (
		--CO: Ekipman Gider Takibi
		SELECT distinct gsber
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equi') }} equi
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equz') }} equz ON equi.equnr = equz.equnr
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }} eqkt ON equi.equnr = eqkt.equnr
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa ON equz.iloan = iloa.iloan
		WHERE eqtyp = 'T'
	),

	co_6 as (
		--CO: Uçak Gider Takibi
		SELECT distinct rbusa
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
		WHERE racct = N'7401801003'
	),

	co_7 as (
		--CO: Yapılmakta Olan Yatırım Gider Takibi
		SELECT distinct rbusa
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
		WHERE racct = N'2580101010'
	),

	fm_1 as (
		--FM: GYG Bütçe Yönetimi
		SELECT distinct rbusa
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
		WHERE 1=1
		AND fipex like N'100%'
		AND rbusa like N'%M'
	),

	fm_2 as (
		--FM: Proje Gelir Bütçe Yönetimi
		SELECT distinct rbusa
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }} a
		left join main_projects mp ON mp.werks = a.rbusa
		WHERE 1=1
		AND racct like N'60%'
		AND (left(rbusa, 1) in ('E', 'C', 'G') and right(rbusa, 1) <> N'M')
		OR bukrs = N'RMG'
	),

	fm_3 as (
		--FM: Proje Gider Bütçe Yönetimi
		SELECT distinct rbusa
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }} a
		left join main_projects mp ON mp.werks = a.rbusa
		WHERE 1=1
		AND racct like N'7%'
		AND (left(rbusa, 1) in ('E', 'C', 'G') and right(rbusa, 1) <> N'M')
		OR bukrs = N'RMG'
	),

	mm_1 as (
		--MM: Barkod Süreci
		SELECT distinct werks
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zlp_mm_t_0000') }}
	),

	mm_2 as (
		--MM: DV_Barkod_Seri No_Süreci
		SELECT distinct company_code
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zlp_mm_t_002') }}
		),

	mm_4 as (
		--MM: Sözleşme Yönetimi
		SELECT distinct gsber
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekkn') }} 
		WHERE ebeln like N'6%'
	),

	mm_5 as (
		--MM: Stok Yönetimi
		SELECT distinct gsber
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_mseg') }}
	),

	mm_6 as (
		--MM: Talep Yönetimi
		SELECT distinct werks
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eban') }}
	),

	pm_1 as (
		--PM: Araç Yönetimi
		SELECT distinct gsber
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equi') }} equi
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equz') }} equz ON equi.equnr = equz.equnr
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }} eqkt ON equi.equnr = eqkt.equnr
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa ON equz.iloan = iloa.iloan
		WHERE eqtyp = 'A'
	),
	
	pm_2 as (
		--PM: Arıza Bakım Yönetimi
		SELECT distinct gsber
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equi') }} equi
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equz') }} equz ON equi.equnr = equz.equnr
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }} eqkt ON equi.equnr = eqkt.equnr
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa ON equz.iloan = iloa.iloan
		WHERE swerk <> ''
	),

		pm_3 as (
		--PM: Planlı Bakım Yönetimi
		SELECT distinct gsber
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equi') }} equi
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equz') }} equz ON equi.equnr = equz.equnr
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }} eqkt ON equi.equnr = eqkt.equnr
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa ON equz.iloan = iloa.iloan
		WHERE swerk = gsber
	),

	pp_1 as (
		--PP: Ürün Ağacı-İş Planlı-Maliyet Yönetimli Mamul Yönetimi
		SELECT distinct rbusa
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
		WHERE rbusa like N'U%'
	),

	sd_1 as (
		--SD: İşveren/Kira/Hizmet/Pursantaj Sözleşme Yönetimi
		SELECT distinct gsber
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vbak') }}
	),

	sd_2_3 as (
		--SD: Sipariş Yönetimi & SD: Teklif Yönetimi
		SELECT distinct rbusa
		FROM {{ ref('stg__s4hana_t_sap_acdoca') }} 
		WHERE racct like N'6%'
	),

	ps_2_4 as (
		--PS: Hakediş Yönetimi & PS: Proje Bütçe Yönetimi (Şantiye)
		SELECT distinct main_projects.werks 
		FROM main_projects
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcj41') }} tcj ON main_projects.bukrs = tcj.bukrs
		WHERE 1=1
		AND tcj.bukrs <> ''
		AND main_projects.werks not like N'%M'
	),
	
	ps_5 as (
		--PS: Puantaj Yönetimi
		SELECT distinct main_projects.werks  
		FROM main_projects
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcj41') }} tcj ON main_projects.bukrs = tcj.bukrs
		WHERE 1=1
		AND (main_projects.werks  like N'C%' OR main_projects.werks  like N'H%' OR main_projects.werks  like N'G%' OR main_projects.werks  like N'R%')
		AND main_projects.werks  not like N'%M'
	)

/*********** KULLANIMLARIN BULUNMASI *************/
	/**** CO: Yapılmakta Olan Yatırım Gider Takibi ****/
	,co_yatirim_gider AS 
		(
			SELECT 	c.[gsber], count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }} a 
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_csks') }} c ON a.kostv = c.kostl 
			WHERE 1=1
				AND auart = 'R004'
			GROUP BY c.[gsber]
		)
	
	/**** CO: Araç Gider Takibi ****/
	,co_arac_gider AS 
		(
			SELECT 	c.[gsber], count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }}  a
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_csks') }} c ON a.kostv = c.kostl
			WHERE 1=1
				AND auart = 'R001'
			GROUP BY c.[gsber]
		)

	/**** CO: Ekipman Gider Takibi ****/
	,co_ekipman_gider AS 
		(
			SELECT 	c.[gsber], count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }}  a
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_csks') }} c ON a.kostv = c.kostl
			WHERE 1=1
				AND auart = 'R003'
			GROUP BY c.[gsber]
		)

	/**** CO: Uçak Gider Takibi ****/
	,co_ucak_gider AS 
		(
			SELECT c.gsber, COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }}  a
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_csks') }} c ON a.kostv = c.kostl
			WHERE auart = N'R008' 
			GROUP BY c.gsber
		)

	/**** CO: Şirket İçi Dağıtım Gider Takibi ****/
	,co_sirket_ici_dagitim_gider AS 
		(
			SELECT gsber, count(*) transaction_amount
			FROM {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf 
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_bseg') }} bseg ON bkpf.belnr = bseg.belnr AND bkpf.bukrs = bseg.bukrs
			WHERE tcode = N'ZCO0034'
			GROUP BY gsber
		)

	/**** CO: Ay Sonu Bina Servis Dağıtım ****/
	,co_aysonu_bina_servis_dagitim AS 
		(
			SELECT gsber1, COUNT(*) transaction_amount 
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zco_t_ksv5_log') }} 
			GROUP BY gsber1
		)

	/**** CO: Fatura Direk Dağıtım Yönetimi ****/
	,co_fatura_direk_dagitim_yonetimi AS 
		(
			SELECT is_alani, COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zco_001_t_fl') }} 
			GROUP BY is_alani
		)

	/**** MM: Barkod Süreci ****/
	,mm_barkod_sureci AS 
		(
			SELECT werks, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zlp_mm_t_0000') }}
			GROUP BY werks
		)

	/**** MM: Intercompany Süreci ****/
	,mm_intercompany_sureci AS 
		(
			SELECT bkm.gsber, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }} ekko 
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }} ekpo ON ekko.ebeln= ekpo.ebeln 
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_000_t_gsbbkm') }} bkm ON ekpo.lgort = bkm.lgort 
			WHERE ekko.bsart = 'ZS07'
			GROUP BY bkm.gsber
		)

	/**** MM: DV_Barkod_Seri No_Süreci ****/
	,mm_dv_barkod_seri_no_sureci AS 
		(
			SELECT anlz.gsber, COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zlp_mm_t_002') }} z2 
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_anlz') }} anlz ON z2.fixed_assetno = anlz.anln1 
			GROUP BY anlz.gsber
		)

	/**** MM: Talep Yönetimi ****/
	,mm_talep_yonetimi AS 
		(
			SELECT bkm.gsber, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }} ekko 
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekpo') }} ekpo ON ekko.ebeln= ekpo.ebeln
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_000_t_gsbbkm') }} bkm ON ekpo.lgort = bkm.lgort
			WHERE ekko.ernam = N'RES_PO'
			GROUP BY bkm.gsber
		)

	/**** MM: Stok Yönetimi ****/
	,mm_stok_yonetimi AS 
		(
			SELECT 	[werks], count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_mseg') }}
			WHERE 1=1
			GROUP BY [werks]
		)

	/**** MM: Sözleşme Yönetimi ****/
	,mm_sozlesme_yonetimi AS 
		(
			SELECT gsber, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }} ekko
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_000_t_gsbbkm') }} bkm ON ekko.bukrs = bkm.bukrs
			WHERE 1=1
				AND ernam = N'RES_PO'
				AND BSTYP = N'K'
			GROUP BY gsber
		)

	/**** FM: GYG Bütçe Yönetimi ****/
	,fm_gyg_butce_yonetimi AS 
		(
			SELECT bwkey , COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmbl') }} fmbl
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k  ON left(fmbl.fundsctr, 3) = k.bukrs
			WHERE 1=1
				AND fundsctr like N'[A-Z]%'
				AND cmmtitem like N'100%'
				AND LEFT(fmbl.fundsctr, 3) = left(k.bwkey,3)
			GROUP BY bwkey
		)

	/**** FM: Proje Gider Bütçe Yönetimi ****/
	,fm_proje_gider_butce_yonetimi AS 
		(
			SELECT bwkey , COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmbl') }} fmbl
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k  ON left(fmbl.fundsctr, 3) = k.bukrs
			WHERE 1=1
				AND fundsctr like N'[A-Z]%'
				AND cmmtitem not like N'100%'
				--AND LEFT(fmbl.fundsctr, 3) = left(k.bwkey,3)
				AND bwkey not like N'%M'
			GROUP BY bwkey
		)

	/**** FM: Proje Gelir Bütçe Yönetimi ****/
	,fm_proje_gelir_butce_yonetimi AS 
		(
			SELECT rbusa, rbukrs, COUNT(*) transaction_amount
			FROM {{ ref('stg__s4hana_t_sap_acdoca') }} 
			WHERE 1=1
				AND fipex <> N'DUMMY'
				AND fipex <> ''
				AND racct like N'60%'
			GROUP BY rbusa, rbukrs
		)

	/**** PM: Planlı Bakım Yönetimi ****/
	,pm_planli_bakim_yonetimi AS 
		(
			SELECT [gsber], COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }} 
			WHERE 1=1
				AND [auart] = N'ZPM2'
			GROUP BY [gsber]
		)

	/**** PM: Arıza Bakım Yönetimi ****/
	,pm_ariza_bakim_yonetimi AS 
		(
			SELECT [gsber], COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_aufk') }} 
			WHERE 1=1
				AND [auart] = N'ZPM1'
			GROUP BY [gsber]
		)

	/**** PM: Araç Yönetimi ****/
	,pm_arac_yonetimi AS 
		(
			SELECT gsber, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equi') }} equi
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equz') }} equz ON equi.equnr = equz.equnr
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }} eqkt ON equi.equnr = eqkt.equnr
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa ON equz.iloan = iloa.iloan
			WHERE eqtyp = N'A'
			GROUP BY gsber
		)

	/**** PM: Makine Ekipman Yönetimi ****/
	,pm_makine_ekipman_yonetimi AS 
		(
			SELECT gsber, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equi') }} equi
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_equz') }} equz ON equi.equnr = equz.equnr
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eqkt') }} eqkt ON equi.equnr = eqkt.equnr
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_iloa') }} iloa ON equz.iloan = iloa.iloan
			WHERE eqtyp <> N'A'
			GROUP BY gsber
		)

	/**** SD: İşveren/Kira/Hizmet/Pursantaj Sözleşme Yönetimi ****/
	,sd_isveren_kira_hizmet_sozlesme AS 
		(
			SELECT gsber, COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vbap') }}  
			WHERE SUBSTRING(vbeln, 3, LEN(vbeln) - 2 ) like N'4000%'
			GROUP BY gsber
		)

	/**** SD: Sipariş Yönetimi ****/
	,sd_siparis_yonetimi AS 
		(
			SELECT gsber, COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vbap') }}
			WHERE SUBSTRING(vbeln, 3, LEN(vbeln) - 2 ) not like N'4000%'
			GROUP BY gsber
		)


	/**** PS: Proje Bütçe Yönetimi (Şantiye) ****/
	,ps_proje_butce_yonetimi AS 
		(
			SELECT 	[fm_fund] as gsber, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfm_001_t_onylog') }} 
			WHERE 1=1
			GROUP BY [fm_fund]
		)

	/**** PS: Puantaj Yönetimi ****/
	,ps_puantaj_yonetimi AS 
		(
			SELECT [werks], COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_cs_log') }} 
			GROUP BY [werks]
		)

	/**** PS: Hakediş Yönetimi ****/
	,ps_hakedis_yonetimi AS 
		(
			SELECT werks, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_haktut') }} 
			GROUP BY werks
		)

	/**** FI: Ödeme Programı ****/
	,fi_odeme_programi AS 
		(
			SELECT gsber, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zfi_001_t_apprs') }}
			GROUP BY gsber
		)

	/**** FI: Vadeli Kokpit ****/
	,fi_vadeli_kokpit AS 
		(
			SELECT gsber, count(*) transaction_amount
			FROM {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf  
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_bseg') }} bseg ON bkpf.belnr = bseg.belnr 
			WHERE tcode like N'YFIN_VDL%'
			GROUP BY gsber
		)

	/**** FI: EHÖ ****/
	,fi_eho AS 
		(
			SELECT gsber, count(*) transaction_amount
			FROM {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_bseg') }} bseg ON bkpf.belnr = bseg.belnr
			WHERE tcode = N'YFINEKS002' OR tcode = N'ZEHO100B'
			GROUP BY gsber
		)

	/**** FI: E-Süreçler Yönetimi ****/
	,fi_e_surecler_yonetimi AS 
		(
			SELECT rbusa, count(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zsftmain') }} zsft 
			LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON zsft.serial = bkpf.xblnr 
														   AND zsft.bukrs = bkpf.bukrs 
			LEFT JOIN {{ ref('stg__s4hana_t_sap_acdoca') }} acd ON bkpf.belnr = acd.belnr
			WHERE appl = N'EA' OR appl = ''
			GROUP BY rbusa
		)

	/**** PP: Ürün Ağacı-İş Planlı-Maliyet Yönetimli Mamul Yönetimi ****/
	,pp_urun_agaci_is_planli_maliyet_yonetimli_mamul_yonetimi AS 
		(
			SELECT wrkan, COUNT(*) transaction_amount
			FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_stko') }} 
			GROUP BY wrkan
		)

, all_data AS (

/*********** KULLANIMLARIN UNIONLANMASI *************/
	/**** CO: Yapılmakta Olan Yatırım Gider Takibi ****/
	select 
		main_projects.*,
		modul = 'CO',
		kapsam = N'Yapılmakta Olan Yatırım Gider Takibi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN co.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN co_yatirim_gider fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN co_7 co ON main_projects.werks = co.rbusa

	UNION ALL

	/**** CO: Araç Gider Takibi ****/
	select 
		main_projects.*,
		modul = 'CO',
		kapsam = N'Araç Gider Takibi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN co.gsber is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN co_arac_gider fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN co_1 co ON main_projects.werks = co.gsber

	UNION ALL

	/**** CO: Ekipman Gider Takibi ****/
	select 
		main_projects.*,
		modul = 'CO',
		kapsam = N'Ekipman Gider Takibi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN co.gsber is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN co_ekipman_gider fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN co_3 co ON main_projects.werks = co.gsber

	UNION ALL

	/**** CO: Uçak Gider Takibi ****/
	select 
		main_projects.*,
		modul = 'CO',
		kapsam = N'Uçak Gider Takibi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN co.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN co_ucak_gider fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN co_6 co ON main_projects.werks = co.rbusa

	UNION ALL

	/**** CO: Şirket İçi Dağıtım Gider Takibi ****/
	select 
		main_projects.*,
		modul = 'CO',
		kapsam = N'Şirket İçi Dağıtım Gider Takibi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = 'TRUE'
	from main_projects
		LEFT JOIN co_sirket_ici_dagitim_gider fact_tbl on main_projects.werks = fact_tbl.[gsber]

	UNION ALL

	/**** CO: Ay Sonu Bina Servis Dağıtım ****/
	select 
		main_projects.*,
		modul = 'CO',
		kapsam = N'Ay Sonu Bina Servis Dağıtım',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN co.werks is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN co_aysonu_bina_servis_dagitim fact_tbl on main_projects.werks = fact_tbl.[gsber1]
		LEFT JOIN co_2 co ON main_projects.werks = co.werks

	UNION ALL

	/**** CO: Fatura Direk Dağıtım Yönetimi ****/
	select 
		main_projects.*,
		modul = 'CO',
		kapsam = N'Fatura Direk Dağıtım Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = 'TRUE'
	from main_projects
		LEFT JOIN co_fatura_direk_dagitim_yonetimi fact_tbl on main_projects.werks = fact_tbl.[is_alani]

	UNION ALL

	/**** MM: Barkod Süreci ****/
	select 
		main_projects.*,
		modul = 'MM',
		kapsam = N'Barkod Süreci',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN mm.werks is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN mm_barkod_sureci fact_tbl on main_projects.werks = fact_tbl.[werks]
		LEFT JOIN mm_1 mm ON main_projects.werks = mm.werks

	UNION ALL

	/**** MM: Intercompany Süreci ****/
	select 
		main_projects.*,
		modul = 'MM',
		kapsam = N'Intercompany Süreci',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = 'TRUE'
	from main_projects
		LEFT JOIN mm_intercompany_sureci fact_tbl on main_projects.werks = fact_tbl.[gsber]

	UNION ALL

	/**** MM: DV_Barkod_Seri No_Süreci ****/
	select 
		main_projects.*,
		modul = 'MM',
		kapsam = N'DV_Barkod_Seri No_Süreci',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN mm.company_code is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN mm_dv_barkod_seri_no_sureci fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN mm_2 mm ON main_projects.bukrs = mm.company_code

	UNION ALL

	/**** MM: Talep Yönetimi ****/
	select 
		main_projects.*,
		modul = 'MM',
		kapsam = N'Talep Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN mm.werks is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN mm_talep_yonetimi fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN mm_6 mm ON main_projects.werks = mm.werks

	UNION ALL

	/**** MM: Stok Yönetimi ****/
	select 
		main_projects.*,
		modul = 'MM',
		kapsam = N'Stok Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN mm.gsber is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN mm_stok_yonetimi fact_tbl on main_projects.werks = fact_tbl.[werks]
		LEFT JOIN mm_5 mm ON main_projects.werks = mm.gsber
	UNION ALL

	/**** MM: Teklif Yönetimi ****/
	select 
		main_projects.*,
		modul = 'MM',
		kapsam = N'Teklif Yönetimi',
		durum = N'YOK',
		necessity = NULL
	from main_projects

	UNION ALL

	/**** MM: Sözleşme Yönetimi ****/
	select 
		main_projects.*,
		modul = 'MM',
		kapsam = N'Sözleşme Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN mm.gsber is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN mm_sozlesme_yonetimi fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN mm_4 mm ON main_projects.werks = mm.gsber

	UNION ALL

	/**** FM: GYG Bütçe Yönetimi ****/
	select 
		main_projects.*,
		modul = 'FM',
		kapsam = N'GYG Bütçe Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN fm.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN fm_gyg_butce_yonetimi fact_tbl on main_projects.werks = fact_tbl.[bwkey]
		LEFT JOIN fm_1 fm ON main_projects.werks = fm.rbusa

	UNION ALL

	/**** FM: Proje Gider Bütçe Yönetimi ****/
	SELECT distinct main.* FROM (
		select 
			main_projects.*,
			modul = 'FM',
			kapsam = N'Proje Gider Bütçe Yönetimi',
			durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
			necessity = CASE WHEN fm.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
		from main_projects
			LEFT JOIN fm_proje_gider_butce_yonetimi fact_tbl on main_projects.werks = fact_tbl.[bwkey]
			LEFT JOIN fm_3 fm ON main_projects.werks = fm.rbusa) main
	RIGHT JOIN main_projects ON main.bukrs = main_projects.bukrs
	WHERE main_projects.werks not like N'%M'

	UNION ALL

	/**** FM: Proje Gelir Bütçe Yönetimi ****/
	select 
		main_projects.*,
		modul = 'FM',
		kapsam = N'Proje Gelir Bütçe Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN fm.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN fm_proje_gelir_butce_yonetimi fact_tbl on main_projects.werks = fact_tbl.[rbusa] and main_projects.bukrs = fact_tbl.rbukrs
		LEFT JOIN fm_2 fm ON main_projects.werks = fm.rbusa

	UNION ALL

	/**** PM: Planlı Bakım Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PM',
		kapsam = N'Planlı Bakım Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN pm.gsber is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN pm_planli_bakim_yonetimi fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN pm_3 pm ON main_projects.werks = pm.gsber

	UNION ALL

	/**** PM: Arıza Bakım Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PM',
		kapsam = N'Arıza Bakım Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN pm.gsber is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN pm_ariza_bakim_yonetimi fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN pm_2 pm ON main_projects.werks = pm.gsber

	UNION ALL

	/**** PM: Araç Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PM',
		kapsam = N'Araç Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN pm.gsber is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN pm_arac_yonetimi fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN pm_1 pm ON main_projects.werks = pm.gsber

	UNION ALL

	/**** PM: Makine Ekipman Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PM',
		kapsam = N'Makine Ekipman Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = 'TRUE'
	from main_projects
		LEFT JOIN pm_makine_ekipman_yonetimi fact_tbl on main_projects.werks = fact_tbl.[gsber]

	UNION ALL

	/**** SD: Teklif Yönetimi ****/
	select 
		main_projects.*,
		modul = 'SD',
		kapsam = N'Teklif Yönetimi',
		durum = N'YOK',
		necessity = CASE WHEN sd.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
	LEFT JOIN sd_2_3 sd ON main_projects.werks = sd.rbusa
	UNION ALL

	/**** SD: İşveren/Kira/Hizmet/Pursantaj Sözleşme Yönetimi ****/
	select 
		main_projects.*,
		modul = 'SD',
		kapsam = N'İşveren/Kira/Hizmet/Pursantaj Sözleşme Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN sd.gsber is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN sd_isveren_kira_hizmet_sozlesme fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN sd_1 sd ON main_projects.werks = sd.gsber

	UNION ALL

	/**** SD: Sipariş Yönetimi ****/
	select 
		main_projects.*,
		modul = 'SD',
		kapsam = N'Sipariş Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN sd.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN sd_siparis_yonetimi fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN sd_2_3 sd ON main_projects.werks = sd.rbusa

	UNION ALL

	/**** PS: Puantaj Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PS',
		kapsam = N'Puantaj Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN ps.werks is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN ps_puantaj_yonetimi fact_tbl on main_projects.werks = fact_tbl.[werks]
		LEFT JOIN ps_5 ps ON main_projects.werks = ps.werks

	UNION ALL
	
	/**** PS: Proje Bütçe Yönetimi (Şantiye) ****/
	select 
		main_projects.*,
		modul = 'PS',
		kapsam = N'Proje Bütçe Yönetimi (Şantiye)',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN ps.werks is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN ps_proje_butce_yonetimi fact_tbl on main_projects.werks = fact_tbl.[gsber]
		LEFT JOIN ps_2_4 ps ON main_projects.werks = ps.werks

	UNION ALL

	/**** PS: Hakediş Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PS',
		kapsam = N'Hakediş Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN ps.werks is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN ps_hakedis_yonetimi fact_tbl on main_projects.werks = fact_tbl.[WERKS]
		LEFT JOIN ps_2_4 ps ON main_projects.werks = ps.werks

	UNION ALL

	/**** PS: Gelir Bütçesi Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PS',
		kapsam = N'Gelir Bütçesi Yönetimi',
		durum = N'YOK',
		necessity = NULL
	from main_projects

	UNION ALL

	/**** PS: Kesinti / İlave İş Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PS',
		kapsam = N'Kesinti / İlave İş Yönetimi',
		durum = N'YOK',
		necessity = NULL
	from main_projects

	UNION ALL
	
	/**** FI: Ödeme Programı ****/
	select 
		main_projects.*,
		modul = 'FI',
		kapsam = N'Ödeme Programı',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = 'TRUE'
	from main_projects
		LEFT JOIN fi_odeme_programi fact_tbl on main_projects.werks = fact_tbl.[gsber]

	UNION ALL

	/**** FI: Vadeli Kokpit ****/
	select 
		main_projects.*,
		modul = 'FI',
		kapsam = N'Vadeli Kokpit',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = 'TRUE'
	from main_projects
		LEFT JOIN fi_vadeli_kokpit fact_tbl on main_projects.werks = fact_tbl.[gsber]

	UNION ALL

	/**** FI: EHÖ ****/
	select 
		main_projects.*,
		modul = 'FI',
		kapsam = N'EHÖ',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = 'TRUE'
	from main_projects
		LEFT JOIN fi_eho fact_tbl on main_projects.werks = fact_tbl.[gsber]

	UNION ALL

	/**** FI: E-Süreçler Yönetimi ****/
	select 
		main_projects.*,
		modul = 'FI',
		kapsam = N'E-Süreçler Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = 'TRUE'
	from main_projects
		LEFT JOIN fi_e_surecler_yonetimi fact_tbl on main_projects.werks = fact_tbl.[rbusa]

	UNION ALL
	
	/**** PP: Ürün Ağacı-İş Planlı-Maliyet Yönetimli Mamul Yönetimi ****/
	select 
		main_projects.*,
		modul = 'PP',
		kapsam = N'Ürün Ağacı-İş Planlı-Maliyet Yönetimli Mamul Yönetimi',
		durum = CASE WHEN fact_tbl.transaction_amount is null then 'YOK' ELSE 'VAR' END,
		necessity = CASE WHEN pp.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
	from main_projects
		LEFT JOIN pp_urun_agaci_is_planli_maliyet_yonetimli_mamul_yonetimi fact_tbl on main_projects.werks = fact_tbl.[wrkan]
		LEFT JOIN pp_1 pp ON main_projects.werks = pp.rbusa

),

activity as (
	SELECT distinct rbusa
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }}
	WHERE 1=1
	AND budat BETWEEN DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0) and GETDATE()
	AND (blart = 'KG' OR blart = 'KR' OR blart = 'KM')
)

SELECT 
	[rls_region],
	[rls_group],
	[rls_company],
	[rls_businessarea] = CONCAT(ad.werks , '_' , dim_comp.rls_region),
	KyribaGrup,
	bukrs,
	werks,
	name1,
	modul,
	kapsam,
	ad.durum,
	necessity,
	active = CASE WHEN ac.rbusa is not null THEN 'TRUE' ELSE 'FALSE' END
FROM all_data ad
LEFT JOIN "dwh_prod"."dimensions"."dm__dimensions_t_dim_companies" dim_comp ON ad.bukrs = dim_comp.RobiKisaKod
LEFT JOIN activity ac ON ad.werks = ac.rbusa
--ORDER BY 1,2,5,3,4
