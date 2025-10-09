{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}

select 
    rls_region,
    rls_group,
    rls_company,
    rls_businessarea = CONCAT(acd.rbusa, '_', rls_region),
	rbukrs as company,
	rbusa as business_area,
	cast(acd.budat as date) as posting_date,
	financial_main_category = 
		case
			WHEN LEFT(acd.racct,1) IN ('1','2') THEN 'Assets'
			WHEN LEFT(acd.racct,1) IN ('3','4') THEN 'Liability'
			WHEN LEFT(acd.racct,1) IN ('5') THEN 'Equity'
		end,
	financial_sub_category	= 
		CASE	
			WHEN LEFT(acd.racct,1) IN ('1') THEN 'Current Assets'
			WHEN LEFT(acd.racct,1) IN ('2') THEN 'Noncurrent Assets'
			WHEN LEFT(acd.racct,1) IN ('3') THEN 'Current Liabilities'
			WHEN LEFT(acd.racct,1) IN ('4') THEN 'Noncurrent Liabilities'
			WHEN LEFT(acd.racct,1) IN ('5') THEN 'Equity'
		END,
	financial_item	= 
		CASE	
			WHEN LEFT(acd.racct,3) ='102' AND racct = '1810101001'	then 'Cash and cash equivalents' --	102-Bankalar
			WHEN LEFT(acd.racct,3) ='103'	then 'Trade payables' --	103-Verilen Çekler
			WHEN LEFT(acd.racct,3) ='120'	then 'Trade receivables' --	120-Alıcılar
			WHEN LEFT(acd.racct,3) ='136'	then 'Other receivables' --	136-DiğerÇeşitliAlacaklar
			WHEN LEFT(acd.racct,3) ='150'	then 'Inventories' --	150-Stoklar
			WHEN LEFT(acd.racct,3) ='157'	then 'Inventories' --	157-DiğerStoklar
			WHEN LEFT(acd.racct,3) ='159'	then 'Prepaid expenses' --	159-VerilenSiparişAvansları
			WHEN LEFT(acd.racct,3) ='180'	then 'Prepaid expenses' --	180-GelecekAylaraAitGiderler
			WHEN acd.racct = '1810101001'	then 'Cash and cash equivalents' --	181-GelirTahakkukları
			WHEN LEFT(acd.racct,3) ='181' AND racct <> '1810101001'	then 'Other current assets' --	181-GelirTahakkukları
			WHEN LEFT(acd.racct,3) ='190'	then 'Current tax assets' --	190-DevredenKdv
			WHEN LEFT(acd.racct,3) ='192'	then 'Other current assets' --	192-Diğer KDV
			WHEN LEFT(acd.racct,3) ='193'	then 'Current tax assets' --	193-PeşinÖdenenVergilerVeFonlar
			WHEN LEFT(acd.racct,3) ='195'	then 'Prepaid expenses' --	195-İşAvansları
			WHEN LEFT(acd.racct,3) ='196'	then 'Other current assets' --	196-PersonelAvansları
			WHEN LEFT(acd.racct,3) ='198'	then 'Other current assets' --	198-DiğerÇeşitliDönenVarlıklar
			WHEN LEFT(acd.racct,3) ='252'	then 'Property, plant and equipment' --	252-Binalar
			WHEN LEFT(acd.racct,3) ='253'	then 'Property, plant and equipment' --	253-Tesis,MakineVeCihazlar
			WHEN LEFT(acd.racct,3) ='254'	then 'Property, plant and equipment' --	254-Taşıtlar
			WHEN LEFT(acd.racct,3) ='255'	then 'Property, plant and equipment' --	255-Demirbaşlar
			WHEN LEFT(acd.racct,3) ='257'	then 'Property, plant and equipment' --	257-BirikmişAmortismanlar(-)
			WHEN LEFT(acd.racct,3) ='260'	then 'Intangible assets' --	260-Haklar
			WHEN LEFT(acd.racct,3) ='268'	then 'Intangible assets' --	268-BirikmişAmortismanlar(-)
			WHEN LEFT(acd.racct,3) ='280'	then 'Prepaid expenses' --	280-GelecekYıllaraAitGiderler
			WHEN LEFT(acd.racct,3) ='301'	then 'Short term borrowings' --	301-Finansal Kiralama
			WHEN LEFT(acd.racct,3) ='302'	then 'Short term borrowings' --	302-Ertelenmiş Finansal Kiralama Borçlanma Maliyetleri(-)
			WHEN LEFT(acd.racct,3) ='309'	then 'Short term borrowings' --	309-DiğerMaliBorçlar
			WHEN LEFT(acd.racct,3) ='320'	then 'Trade payables' --	320-Satıcılar
			WHEN LEFT(acd.racct,3) ='335'	then 'Payables related to employee benefits' --	335-PersoneleBorçlar
			WHEN LEFT(acd.racct,3) ='336' AND LEFT(acd.racct,3) ='381' AND racct NOT IN ('3810101010', '3810101019', '3810104019') AND LEFT(acd.racct,3) ='481' AND racct NOT IN ('4810104010', '4810104019')	then 'Other payables' --336-DiğerÇeşitliBorçlar
			WHEN LEFT(acd.racct,3) ='340'	then 'Deferred revenue' --	340-Alınan Sipariş Avansları
			WHEN LEFT(acd.racct,3) ='360'	then 'Other current liabilities' --	360-ÖdenecekVergiVeFonlar
			WHEN LEFT(acd.racct,3) ='361'	then 'Payables related to employee benefits' --	361-ÖdenecekSosyalGüvenlikKesintileri
			WHEN LEFT(acd.racct,3) ='379'	then 'Other current liabilities' --	379-DiğerBorçVeGiderKarşılıkları
			WHEN LEFT(acd.racct,3) ='380'	then 'Deferred revenue' --	380-GelecekAylaraAitGelirler
			WHEN racct IN ('3810101010', '3810101019', '3810104019')	then 'Short term borrowings' --	381-GiderTahakkukları
			WHEN LEFT(acd.racct,3) ='400'	then 'Long term borrowings' --	400-BankaKredileri
			WHEN LEFT(acd.racct,3) ='401'	then 'Long term borrowings' --	401-Finansal Kiralama İşlemlerinden Borçlar
			WHEN LEFT(acd.racct,3) ='402' AND  racct NOT IN ('4810104010', '4810104019')	then 'Long term borrowings' --	402-Ertelenmiş Finansal Kiralama Borçlanma Maliyetleri(-)
			WHEN LEFT(acd.racct,3) ='426'	then 'Other payables' --	426-AlınanDepozitoVeTeminatlar
			WHEN LEFT(acd.racct,3) ='480'	then 'Deferred revenue' --	480-Gelecek Yıllara ait gelirler
			WHEN LEFT(acd.racct,3) ='500'	then 'Paid-in capital' --	500-Sermaye
			WHEN LEFT(acd.racct,3) ='501'	then 'Paid-in capital' --	501-ÖdenmemişSermaye(-)
			WHEN LEFT(acd.racct,3) ='502'	then 'Paid-in capital' --	502-SermayeDüzeltmesiOlumluFarkları
			WHEN LEFT(acd.racct,3) ='540'	then 'Restricted reserves' --	540-YasalYedekler
			WHEN LEFT(acd.racct,3) ='570'	then 'Retained earnings' --	570-GeçmişYıllarKarları
			WHEN LEFT(acd.racct,3) ='580'	then 'Retained earnings' --	580-GeçmişYıllarZararları(-)
	END,
	acd.racct as account_number,
	acd.hsl as amount_in_try,
	acd.ksl as amount_in_eur
from {{ ref('stg__s4hana_t_sap_acdoca') }} acd
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} comp ON acd.rbukrs = comp.RobiKisaKod
WHERE 1=1
	AND rbukrs = 'RMG'
	AND LEFT(acd.racct,1) IN ('1','2','3','4','5')
	AND CAST(BUDAT AS date) >= '2025' 