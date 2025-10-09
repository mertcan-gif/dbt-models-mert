{{
  config(
    materialized = 'table',
    tags = ['scm_kpi']
  )
}}
 
SELECT 
	   rls_key
    ,rls_region
    ,rls_group
    ,rls_company
	, LEFT(rls_key, CHARINDEX('-', rls_key) - 1) AS rls_businessarea
	  ,first_pr_number --Birinci PR Numarası
    ,tdf_id  --TDF Numarası
	  ,second_pr_number -- İkinci PR Numarası
    ,tender_status -- Talep Durumu
	  ,company_name --Şirket
	  ,project_name -- Proje
    ,tender_id -- İhale Numarası
    ,tender_version --İhale Versiyon
    ,tender_procurement_personel --Satınalma Personeli
	  ,procurement_group --Satınalma Grubu
    ,tender_creation_date --İhale Yaratma Tarihi
    ,tender_opening_date --İhale Açılış
    ,tender_closing_date --İhale Kapanış
    ,tender_preview_date --İhale Ön izleme Tarihi
    ,tender_activity_area --İhale Faaliyet Alanı
	  ,document_type --Belge Türü
    ,tender_subject --Talep Konusu
	  ,country --Ülke
	  ,requester --Talep Eden
	  ,creator --Talep Oluşturan 
	  ,request_creation_date --Talep Oluşturma Tarihi
	  ,money_amount --Teklif Edilen Tutar
	  ,money_currency --TEKLİF Edilen Tutar Para Birimi
	  ,money_amount_usd -- Teklif Edilen Tutar (USD)
	  ,winning_price --TDF Toplam Tutar
	  ,winning_price_currency --TDF Toplam Tutar Para Birimi
	  ,pef_total_price_usd --TDF Toplam Tutar USD
	  ,[org_name] --Teklif İleten Firma Adı
	  ,winner_companies --Seçilen Firma Adı
	  ,bidder_mail --Teklif İleten Mail
	  ,NULL AS creator_ip_address --Teklif İleten IP Adresi
	  ,NULL as is_ronesans_personnel --Ronesans Personeli Mi?
	  ,NULL AS e_purchase_state ---E-satınalma durumu (talep)
	  ,NULL AS e_purchase_state_by_bidder ---e-satınalma durumu (teklif veren firma bazlı)
	  ,NULL AS bidder_manual_offers --teklif veren firma tarafından İhaleye verilen manuel teklifler
	  ,NULL AS number_off_all_offers_by_bidder --teklif veren firmanın İlgili İhaledeki toplam teklif sayısı
	  ,bidder_tax_id --Tedarikçi VKN
	  ,bidder_phone --Tedarikçi Telefon
	  ,bidder_address --Tedarikçi Telefon
    ,bidder_city --Tedarikçi Şehir
	  ,bidder_state --Tedarikçi Eyalet
	  ,bidder_country   --Tedarikçi Ülke   
	  ,bidder_inviter --Tedarikçiyi Davet Eden Kişi
	  ,submission_date --ihale Teklif Verilme Tarihi
from {{ ref('stg__scm_kpi_t_fact_epurchasereport') }}