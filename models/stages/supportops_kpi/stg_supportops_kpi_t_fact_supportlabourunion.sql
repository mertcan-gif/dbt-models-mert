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

WITH union_labour_data AS (
  SELECT
    [source] = 'Table 2024'
    ,[SiraNo]
    ,[MaliYil]
    ,[MaliAy]
    ,[PersonelAlani]
    ,[PersonelAltAlani]
    ,[UretimYeri]
    ,[TaseronNo]
    ,[TaseronAdi]
    ,[TcKimlikNo]
    ,[AdiSoyadi]
    ,[KullaniciAdi]
    ,[PozisyonTanimi]
    ,[EkipNo]
    ,[EkipTanimi]
    ,[GorevYeri]
    ,[IseGirisTarihi]
    ,[IstenCikisTarihi]
    ,[IstenCikisNedeniTnm]
    ,[Birim]
    ,[GunsayN]
    ,[GunsayHt]
    ,[GunsayHtc]
    ,[GunsayB]
    ,[GunsayBc]
    ,[GunsayYi]
    ,[GunsayIsk]
    ,[GunsayUmi]
    ,[GunsayUi]
    ,[GunsayD]
    ,[GunsayOdenecek]
    ,[SaatFazMesai]
    ,[SaatBc]
    ,[SaatHt]
    ,[StatFazMesai]
    ,[GunsaySgk]
    ,[GunsayPersOde]
    ,[SaathFazMesai]
    ,[SaathFazMesaB]
    ,[GunhHtc]
    ,[GunNihaiSgk]
    ,[YevBedel]
    ,[YevBedelUcret]
    ,[MaasBedel]
    ,[FazMesaiUcret]
    ,[BayramMesaiUcret]
    ,[HtFazMesaiUcret]
    ,[TopOdemeFazMesai]
    ,[PersTopOde]
    ,[Bordro1]
    ,[Bordro2]
    ,[FazMesaiSabasFark]
    ,[FirmaFark]
    ,[GunlukSgkUcret]
    ,[DigerKar]
    ,[HakBrmFiyat]
    ,[FirmayaSgkOdemesi]
    ,[FirmaPayi]
    ,[FirmaFmOdeme]
    ,[KidemBrut]
    ,[KidemNet]
    ,[IhbarBrut]
    ,[IhbarNet]
    ,[YiIsverenMaliyet]
    ,[YiBrut]
    ,[YiNet]
    ,[FirmaTopOdeme]
    ,[EksFazOdeme]
    ,[ToplamOdemeGun]
    ,[DigerGider]
    ,[FirmaPayTatilCalisma]
    ,[GdigerGider]
    ,[FirmaKarPayiOdeme]
    ,[BosGun]
    ,[PersonelNo]
    ,[UcretTuru]
    ,[KarOrani]
    ,[EfOdeme]
    ,[GunsayGfm]
    ,[DamgaVergisi]
    ,[GunsayIksr]
    ,[FmDuzeltme]
    ,[Tarih]
    ,[db_upload_timestamp]
  FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_supportlabour2024') }}
  WHERE 1=1
  and MaliYil = '2024'

    UNION ALL

  SELECT
    [source] = 'Table 2025'
    ,[SiraNo]
    ,[MaliYil]
    ,[MaliAy]
    ,[PersonelAlani]
    ,[PersonelAltAlani]
    ,[UretimYeri]
    ,[TaseronNo]
    ,[TaseronAdi]
    ,[TcKimlikNo]
    ,[AdiSoyadi]
    ,[KullaniciAdi]
    ,[PozisyonTanimi]
    ,[EkipNo]
    ,[EkipTanimi]
    ,[GorevYeri]
    ,[IseGirisTarihi]
    ,[IstenCikisTarihi]
    ,[IstenCikisNedeniTnm]
    ,[Birim]
    ,[GunsayN]
    ,[GunsayHt]
    ,[GunsayHtc]
    ,[GunsayB]
    ,[GunsayBc]
    ,[GunsayYi]
    ,[GunsayIsk]
    ,[GunsayUmi]
    ,[GunsayUi]
    ,[GunsayD]
    ,[GunsayOdenecek]
    ,[SaatFazMesai]
    ,[SaatBc]
    ,[SaatHt]
    ,[StatFazMesai]
    ,[GunsaySgk]
    ,[GunsayPersOde]
    ,[SaathFazMesai]
    ,[SaathFazMesaB]
    ,[GunhHtc]
    ,[GunNihaiSgk]
    ,[YevBedel]
    ,[YevBedelUcret]
    ,[MaasBedel]
    ,[FazMesaiUcret]
    ,[BayramMesaiUcret]
    ,[HtFazMesaiUcret]
    ,[TopOdemeFazMesai]
    ,[PersTopOde]
    ,[Bordro1]
    ,[Bordro2]
    ,[FazMesaiSabasFark]
    ,[FirmaFark]
    ,[GunlukSgkUcret]
    ,[DigerKar]
    ,[HakBrmFiyat]
    ,[FirmayaSgkOdemesi]
    ,[FirmaPayi]
    ,[FirmaFmOdeme]
    ,[KidemBrut]
    ,[KidemNet]
    ,[IhbarBrut]
    ,[IhbarNet]
    ,[YiIsverenMaliyet]
    ,[YiBrut]
    ,[YiNet]
    ,[FirmaTopOdeme]
    ,[EksFazOdeme]
    ,[ToplamOdemeGun]
    ,[DigerGider]
    ,[FirmaPayTatilCalisma]
    ,[GdigerGider]
    ,[FirmaKarPayiOdeme]
    ,[BosGun]
    ,[PersonelNo]
    ,[UcretTuru]
    ,[KarOrani]
    ,[EfOdeme]
    ,[GunsayGfm]
    ,[DamgaVergisi]
    ,[GunsayIksr]
    ,[FmDuzeltme]
    ,[Tarih]
    ,[db_upload_timestamp]
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_supportlabour2025') }}
)

SELECT 
  uld.[source]
  ,fiscal_year = uld.[MaliYil]
  ,fiscal_month = uld.[MaliAy]
  ,personnel_field = uld.[PersonelAlani]
  ,personnel_subfield = uld.[PersonelAltAlani]
  ,business_area = uld.[UretimYeri]
  ,business_area_name = t001w.name1
  ,subcontractor_no = uld.[TaseronNo]
  ,subcontractor_name = uld.[TaseronAdi]
  ,national_id_number = uld.[TcKimlikNo]
  ,full_name = uld.[AdiSoyadi]
  ,username = uld.[KullaniciAdi]
  ,position_description = uld.[PozisyonTanimi]
  ,team_no = uld.[EkipNo]
  ,manager_id = approver1
  ,manager_fullname = CONCAT(emp.name, ' ', emp.surname)
  ,team_description = uld.[EkipTanimi]
  ,work_location = uld.[GorevYeri]
  ,start_date = uld.[IseGirisTarihi]
  ,end_date = uld.[IstenCikisTarihi]
  ,leaving_reason = uld.[IstenCikisNedeniTnm]
  ,standard_work_hours = CAST(uld.[GunsayN] AS FLOAT)
  ,weekly_rest_hours = CAST(uld.[GunsayHt] AS FLOAT)
  ,weekend_work_hours = CAST(uld.[GunsayHtc] AS FLOAT)
  ,public_holiday_hours = CAST(uld.[GunsayB] AS FLOAT)
  ,public_holiday_work_hours = CAST(uld.[GunsayBc] AS FLOAT)
  ,annual_leave_hours = CAST(uld.[GunsayYi] AS FLOAT)
  ,work_accident_hours = CAST(uld.[GunsayIsk] AS FLOAT)
  ,paid_excused_leave_hours = CAST(uld.[GunsayUmi] AS FLOAT)
  ,unpaid_excused_leave_hours = CAST(uld.[GunsayUi] AS FLOAT)
  ,absence_hours = CAST(uld.[GunsayD] AS FLOAT)
  ,normal_payable_days = CAST(uld.[GunsayOdenecek] AS FLOAT)
  ,overtime_hours = CAST(uld.[SaatFazMesai] AS FLOAT)
  ,holiday_work_hours = CAST(uld.[SaatBc] AS FLOAT)
  --,weekend_work_multiplier_hours = uld.[SaatHt]
  ,overtime_status = uld.[StatFazMesai]
  ,sgk_days_count = CAST(uld.[GunsaySgk] AS FLOAT)
  ,payable_days_to_employee = CAST(uld.[GunsayPersOde] AS FLOAT)
  ,overtime_multiplier_hours = CAST(uld.[SaathFazMesai] AS FLOAT)
  ,holiday_work_multiplier_hours = CAST(uld.[SaathFazMesaB] AS FLOAT)
  ,weekend_work_multiplier_hours = CAST(uld.[GunhHtc] AS FLOAT)
  ,final_sgk_day = CAST(uld.[GunNihaiSgk] AS FLOAT)
  ,daily_wage = CAST(uld.[YevBedel] AS FLOAT)
  ,hourly_wage = CAST(uld.[YevBedelUcret] AS FLOAT)
  ,salary_amount = CAST(uld.[MaasBedel] AS FLOAT)
  ,overtime_pay = CAST(uld.[FazMesaiUcret] AS FLOAT)
  ,holiday_overtime_pay = CAST(uld.[BayramMesaiUcret] AS FLOAT)
  ,weekend_overtime_pay = CAST(uld.[HtFazMesaiUcret] AS FLOAT)
  ,total_overtime_payments = CAST(uld.[TopOdemeFazMesai] AS FLOAT)
  ,total_payable_to_employee = CAST(uld.[PersTopOde] AS FLOAT)
  ,payroll_1 = uld.[Bordro1]
  ,payroll_2 = uld.[Bordro2]
  ,hourly_overtime_company_difference = CAST(uld.[FazMesaiSabasFark] AS FLOAT)
  ,company_difference = CAST(uld.[FirmaFark] AS FLOAT)
  ,daily_sgk_fee = CAST(uld.[GunlukSgkUcret] AS FLOAT)
  ,other_profit = CAST(uld.[DigerKar] AS FLOAT)
  ,unit_price_of_progress_payment = CAST(uld.[HakBrmFiyat] AS FLOAT)
  ,company_sgk_payment = CAST(uld.[FirmayaSgkOdemesi] AS FLOAT)
  ,company_share = CAST(uld.[FirmaPayi] AS FLOAT)
  ,company_overtime_payment = CAST(uld.[FirmaFmOdeme] AS FLOAT)
  ,severance_gross = CAST(uld.[KidemBrut] AS FLOAT)
  ,severance_net = CAST(uld.[KidemNet] AS FLOAT)
  ,notice_gross = CAST(uld.[IhbarBrut] AS FLOAT)
  ,notice_net = CAST(uld.[IhbarNet] AS FLOAT)
  ,annual_leave_employer_cost = CAST(uld.[YiIsverenMaliyet] AS FLOAT)
  ,annual_leave_gross = CAST(uld.[YiBrut] AS FLOAT)
  ,annual_leave_net = CAST(uld.[YiNet] AS FLOAT)
  ,total_payable_to_company = CAST(uld.[FirmaTopOdeme] AS FLOAT)
  ,other_expenses = CAST(uld.[DigerGider] AS FLOAT)
  ,company_profit_share_payment = CAST(uld.[FirmaKarPayiOdeme] AS FLOAT)
  ,empty_day = CAST(uld.[BosGun] AS FLOAT)
  ,personnel_no = uld.[PersonelNo]
  ,profit_ration = CAST(uld.[KarOrani] AS FLOAT)
  ,stamp_tax = CAST(uld.[DamgaVergisi] AS FLOAT)
  ,work_accident_medical_report_hours = CAST(uld.[GunsayIksr] AS FLOAT)
  ,overtime_correction = CAST(uld.[FmDuzeltme] AS FLOAT)
  FROM union_labour_data uld
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zhr_ekip_krilim2') }} ekp on ekp.btrtl = uld.PersonelAltAlani
                                                                                  and ekp.ekipno = uld.EkipNo
                                                                                  and CAST(CONCAT(uld.MaliYil,'-',uld.MaliAy,'-01') as date) BETWEEN CAST(ekp.begda as date) AND CAST(ekp.endda as date)
  LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} emp on emp.sap_id = ekp.approver1
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = uld.[UretimYeri]
  WHERE 1=1
  AND uld.[TaseronNo] IN (
                        '1009543',
                        '1014157',
                        '1031761',
                        '1002027',
                        '1034132',
                        '1038038'
                    )