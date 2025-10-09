

{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}
WITH doc_types as (
SELECT DISTINCT
zzariba_req_no,
CASE	
	WHEN knttp = ''  THEN 'Stoklu Alım'
	WHEN knttp = 'B' THEN 'Dp.ürt/mşt.sprş çğr'
	WHEN knttp = 'C' THEN 'Dp.ürt/mşt.sprş çğr'
	WHEN knttp = 'D' THEN  'Spr.üz.ürt/pr.msf.yk'
	WHEN knttp = 'E' THEN  'KD-CO ile sprş.üz.'
	WHEN knttp = 'F' THEN  'Sipariş'
	WHEN knttp = 'G' THEN  'Depoya üretim/proje'
	WHEN knttp = 'K' THEN  'Masraf yeri'
	WHEN knttp = 'M' THEN  'KD-CO olm.sprş.üz.'
	WHEN knttp = 'N' THEN  'Ağ planı'
	WHEN knttp = 'P' THEN  'Proje'
	WHEN knttp = 'Q' THEN  'Sprş.üzrn.ürt(proje)'
	WHEN knttp = 'T' THEN  'Tüm yeni yan msf.'
	WHEN knttp = 'U' THEN  'Tanınmıyor'
	WHEN knttp = 'X' THEN  'Tüm yardımcı hsp.tyn'
	WHEN knttp = 'Y' THEN  'Proje Aktivite Tşrn'
	WHEN knttp = 'Z' THEN  'İade edlb.ambalaj'
END AS document_type
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eban') }} 
where zzariba_req_no != ''
)
SELECT
rls_key=CONCAT(t.rls_businessarea, '-', t.rls_company, '-', t.rls_group)
,t.rls_region 
,t.rls_group 
,t.rls_company 
,t.rls_businessarea 
,[Talep Numarası] as code
,hd.[tdf_id] as tdf_id
,[Talep Konusu] as request_description
,k.Tanim as company
,t.project_name as project
,t1.land1 as country
,dd.document_type
,hd.purchase_type
,    CASE
        WHEN hd.status = 'C' THEN 'TDF Oluşturuldu'
        WHEN hd.status = 'S' THEN 'TDF Onaya Sunuldu'
        WHEN hd.status = 'R' THEN 'TDF Reddedildi-RO'
        WHEN hd.status = 'B' THEN 'TDF Geri Gönderildi -DM'
        WHEN hd.status = 'A' THEN 'TDF Onayladı'
        WHEN hd.status = 'D' THEN 'TDF İptal Edildi'
    END as [status]
--,[surec]
,k2.process_date as request_creation_date
,[TDF Oluşturulma Tarihi]  as pef_creation_date
,[TDF Onaya Sunma Tarihi] as pef_submission_date
,[Satınalma Personeli] as procurement_personnel -- buradaki aslında onaycı.
,hd.satinalma_yetkilisi as procurement_personnel_real --satınalma yetkilisi aslında satınalma personeli
,t024.eknam as procurement_group
--,[tdf_process_id]
--,[previous_proces_date] 
,[TDF Satınalma Onay Verme Tarihi]as pef_procurement_approval_date
,[TDF Satınalama Onay İşlem Süresi (Onaycı)] as  pef_procurement_approval_timelapse
,[TDF Yapım Onay Verme Tarihi] as pef_construction_approval_date
,[TDF Yapım Onay İşlem Süresi (Onaycı)] as  pef_construction_approval_timelapse
,[TDF Finans Onay Verme Tarihi] as pef_finance_approval_date
,[TDF Finans Onay İşlem Süresi (Onaycı)] as pef_finance_approval_timelapse
FROM {{ ref('stg__scm_kpi_t_fact_aribapefapproval') }} m
	LEFT JOIN {{ ref('dm__dimensions_t_dim_projects') }} t on t.business_area = m.Proje
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t1 on t1.werks = m.Proje
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} k on k.RobiKisaKod = m.Şirket
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} hd on hd.tdf_id = m.tdf_id
  LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t024') }} t024 ON t024.ekgrp = hd.satinalma_grubu
  LEFT JOIN (select zzariba_req_no,
                      string_agg(document_type,',') document_type
                from  doc_types group by zzariba_req_no) dd on dd.zzariba_req_no = m.[Talep Numarası]
  LEFT JOIN 
		(SELECT id,process_date
		 FROM {{ ref('stg__scm_kpi_t_fact_processingtimes') }}
		 WHERE tdf_approval_group = 'Talep Yaratilma') k2 on k2.id = m.[Talep Numarası]
  

