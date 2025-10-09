{{
  config(
    materialized = 'table',
    tags = ['scm_kpi']
  )
}}
WITH document_types as (
SELECT DISTINCT
CONCAT(prj.rls_businessarea, '-', prj.rls_company, '-', prj.rls_group) as rls_key,
prj.rls_region,
prj.rls_group,
prj.rls_company,
cmp.Tanim as company_name,
t1.name1 AS project_name,
cmp.RobiKisaKod as company_code,
e.werks as project_code,
e.zzaribareqno,
CASE	
	WHEN e.knttp = ''  THEN 'Stoklu Alim'
	WHEN e.knttp = 'B' THEN 'Dp.ürt/mst.sprs çgr'
	WHEN e.knttp = 'C' THEN 'Dp.ürt/mst.sprs çgr'
	WHEN e.knttp = 'D' THEN  'Spr.üz.ürt/pr.msf.yk'
	WHEN e.knttp = 'E' THEN  'KD-CO ile sprs.üz.'
	WHEN e.knttp = 'F' THEN  'Siparis'
	WHEN e.knttp = 'G' THEN  'Depoya üretim/proje'
	WHEN e.knttp = 'K' THEN  'Masraf yeri'
	WHEN e.knttp = 'M' THEN  'KD-CO olm.sprs.üz.'
	WHEN e.knttp = 'N' THEN  'Ag plani'
	WHEN e.knttp = 'P' THEN  'Proje'
	WHEN e.knttp = 'Q' THEN  'Sprs.üzrn.ürt(proje)'
	WHEN e.knttp = 'T' THEN  'Tüm yeni yan msf.'
	WHEN e.knttp = 'U' THEN  'Taninmiyor'
	WHEN e.knttp = 'X' THEN  'Tüm yardimci hsp.tyn'
	WHEN e.knttp = 'Y' THEN  'Proje Aktivite Tsrn'
	WHEN e.knttp = 'Z' THEN  'Iade edlb.ambalaj'
END AS document_type,
t024.eknam as procurement_group,
t1.land1 as country,
zzbtpuser 
FROM  aws_stage.s4_odata.raw__s4hana_t_sap_eban e 
LEFT JOIN aws_stage.s4_odata.raw__s4hana_t_sap_t001w t1 on t1.werks= e.werks
LEFT JOIN dwh_prod.dimensions.dm__dimensions_t_dim_projects prj on prj.business_area = t1.werks
LEFT JOIN dwh_prod.dimensions.dm__dimensions_t_dim_companies cmp on cmp.RobiKisaKod = prj.company
LEFT JOIN aws_stage.s4_odata.raw__s4hana_t_sap_t024 t024 ON t024.ekgrp = e.ekgrp
   )
, document_types_aggregated as (
SELECT
STRING_AGG(document_type,',') doc_type,
zzaribareqno,
rls_key,
rls_region,
rls_group,
rls_company,
company_name,
project_name,
company_code,
project_code,
procurement_group,
zzbtpuser,
country
FROM document_types 
GROUP BY zzaribareqno,
		rls_key,
		rls_region,
		rls_group,
		rls_company,
		company_name,
		project_name,
		company_code,
		project_code,
		country,
		zzbtpuser,
		procurement_group
),
request_creation as (
SELECT
id,
MAX(process_date) as process_date
FROM dwh_prod.scm_kpi.dm__scm_kpi_t_fact_processingtimes
where 1=1
	and tdf_approval_group = 'Talep Yaratilma'
	and id is not null 
group by id
),
tender_offer_cte as (
SELECT  
	 t.[eventId]
	,t.[invitationId]
	,t.[itemId]
	,t.[submitRound]
	,t.[bidStatus]
	,t.[isRevisedBid]
	,t.[Itemtitle]
	,t.[fieldId]
	,t.money_amount
	,t.money_currency
	,tc.[org_name]
	,tc.[org_smVendorID]
	,tc.[org_erpVendorID]
	,tc.[org_taxID]
	,tc.[org_phone]
	,tc.[org_address_lines]
	,tc.[org_city]
	,tc.[org_state]
	,tc.[org_country]       
	,tc.[inviter_uniqueName]
	,tc.[submissionDate]
	,tc.[userId]
  FROM aws_stage.[scm_kpi].[raw__scm_kpi_t_fact_ariba_tenderbidhistory] t   
  LEFT JOIN (	
		  SELECT   [index]
				  ,[doc_id]
				  ,[invitationId]
				  ,[userId]
				  ,[submissionDate]
				  ,[inviter_uniqueName]
				  ,[supplierBidStatus]
				  ,[supplierBidStatusLocalized]
				  ,[hasBid]
				  ,[acceptedSupplierAgreement]
				  ,[org_name]
				  ,[org_smVendorID]
				  ,[org_erpVendorID]
				  ,[org_taxID]
				  ,[org_phone]
				  ,[org_address_lines]
				  ,[org_city]
				  ,[org_state]
				  ,[org_country]
		  FROM [aws_stage].[scm_kpi].[raw__scm_kpi_t_fact_ariba_tendercompanies] 
			) tc on tc.[invitationId] = t.[invitationId] and tc.doc_id =  t.[eventId]
  WHERE 1=1--
	and t.fieldId = 'EXTENDEDPRICE' 
 -- and t.[eventId] = 'Doc2424528628' and tc.[org_name] like '%SB BOT%' 
    and bidStatus !='Archived'
	and itemTitle != 'Toplamlar'
  ),
awards as (
	SELECT
	STRING_AGG(tc.[org_name],',') as org_name,
	a.eventId,
	SUM(a.EXTENDEDPRICE_supplier_amount) AS EXTENDEDPRICE_supplier_amount,
	a.EXTENDEDPRICE_supplier_currency
	FROM [aws_stage].[scm_kpi].[raw__scm_kpi_t_fact_aribatenderawards] a
	LEFT JOIN [aws_stage].[scm_kpi].[raw__scm_kpi_t_fact_ariba_tendercompanies] tc on tc.doc_id=a.eventId and tc.invitationId = a.invitationId
	where 1=1                   ---
	--and a.eventId = 'Doc2424528628'
	and a.invitationId is not null
	GROUP BY a.eventId,a.EXTENDEDPRICE_supplier_currency
)
SELECT 
	   dt.rls_key,
	   dt.rls_region,
	   dt.rls_group,
	   dt.rls_company,
	   dt.rls_businessarea
	  ,t2.requisitionnumber as first_pr_number --Birinci PR Numarası
	  ,t2.srid as srid_from509
	  ,ihale_olusturma.srid as srid_from511
	  ,ihale_olusturma.docid as docid_from511
      ,r.tdf_id as tdf_id  --TDF Numarası
	  ,r.created_pr_id as second_pr_number -- İkinci PR Numarası
      ,t.[status] as tender_status -- Talep Durumu
	   ,dt.company_name --Şirket
	   ,dt.project_name -- Proje
      ,t.[internalId] as tender_id
      ,t.[documentVersion] as tender_version
      ,t.[owner_name] as tender_procurement_personel --Satınalma Personeli
	  ,dt.procurement_group --Satınalma Grubu
      ,t.[owner_uniqueName] as tender_procurement_personel_mail
      ,t.[createDate] as tender_creation_date
      ,CAST(LEFT(t.[openDate], 23) AS datetime) as tender_opening_date --İhale Açılış
      ,CAST(LEFT(t.closeDate, 23) AS datetime)  as tender_closing_date --İhale Kapanış
      ,t.[previewDate] as tender_preview_date
      ,t.[commodity_name] as tender_activity_area
	  ,dt.doc_type as document_type --Belge Türü
      ,t.[title] as tender_subject --Talep Konusu
	  ,country --Ülke
	  ,zzbtpuser as requester --Talep Eden
	  ,zzbtpuser as creator --Talep Oluşturan 
	  ,rc.process_date as request_creation_date --Talep Oluşturma Tarihi
	  ,cast(toc.money_amount as float) as money_amount --Teklif Edilen Tutar
	  ,toc.money_currency --TEKLİF Edilen Tutar Para Birimi
	  ,NULL as money_amount_usd -- Teklif Edilen TUTAR (USD)
	  ,a.EXTENDEDPRICE_supplier_amount as winning_price --TDF Toplam Tutar
	  ,a.EXTENDEDPRICE_supplier_currency as winning_price_currency --TDF Toplam Tutar Para Birimi
	  ,TRY_CONVERT(DECIMAL(18, 5),
            REPLACE(REPLACE(r.bidding_amount, '.', ''), ',', '.')) as pef_total_price_usd --TDF Toplam Tutar USD
	  ,toc.[org_name] --Teklif İleten Firma Adı
	  ,a.org_name as winner_companies --Seçilen Firma Adı
	  ,toc.[userId] as bidder_mail --Teklif İleten Mail
	  ,NULL AS creator_ip_address --Teklif İleten IP Adresi
	  ,NULL as is_ronesans_personnel --Ronesans Personeli Mi?
	  ,NULL AS e_purchase_state ---E-satınalma durumu (talep)
	  ,NULL AS e_purchase_state_by_bidder ---e-satınalma durumu (teklif veren firma bazlı)
	  ,NULL AS bidder_manual_offers --teklif veren firma tarafından İhaleye verilen manuel teklifler
	  ,NULL AS number_off_all_offers_by_bidder --teklif veren firmanın İlgili İhaledeki toplam teklif sayısı
	  ,toc.[org_taxID] as bidder_tax_id --Tedarikçi VKN
	  ,toc.[org_phone] as bidder_phone --Tedarikçi Telefon
	  ,toc.[org_address_lines] as bidder_address --Tedarikçi Telefon
      ,toc.[org_city] as bidder_city --Tedarikçi Şehir
	  ,toc.[org_state] AS bidder_state --Tedarikçi Eyalet
	  ,toc.[org_country] as bidder_country   --Tedarikçi Ülke   
	  ,toc.[inviter_uniqueName] as bidder_inviter --Tedarikçiyi Davet Eden Kişi
	  ,[submissionDate] as submission_date --ihale Teklif Verilme Tarihi
  FROM [aws_stage].[scm_kpi].[raw__scm_kpi_t_dim_aribatenders] t
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_zmm_t_tdf_header" r on r.doc_id = t.internalId
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zarb_t_511T" ihale_olusturma ON t.internalId = ihale_olusturma.docid
    LEFT JOIN "aws_stage"."s4_odata"."raw__s4hana_t_sap_ariba_zarb_t_509t" t2 ON t2.srid = ihale_olusturma.srid
	LEFT JOIN document_types_aggregated dt ON dt.zzaribareqno = t2.requisitionnumber
	LEFT JOIN aws_stage.s4_odata.raw__s4hana_t_sap_zmm_t_tdf_header ht on ht.doc_id = t.internalId
	LEFT JOIN request_creation rc on rc.id = t2.requisitionnumber
	LEFT JOIN tender_offer_cte toc on toc.eventId = t.[internalId]
	LEFT JOIN  awards a on a.eventId = t.internalId
	where 1=1
	--and ihale_olusturma.docid= 'Doc2424528628'
	--WHERE t2.requisitionnumber = 'PR4231'
	and  rls_group is not null