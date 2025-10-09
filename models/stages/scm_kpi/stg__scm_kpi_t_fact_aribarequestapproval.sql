
{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}
WITH full_talep_onay as (
SELECT
s.id as 'Talep Numarası',
s.company as 'Şirket',
s.billing_address as 'Proje',
s.[user] as 'Satınalma Personeli',
k.process_date as 'Talep Oluşturma Tarihi',
k.process_date as 'Talep Onaya Sunma Tarihi',
s.process_date as 'Talep Onay Tarihi',
CAST(DATEDIFF(SECOND, s.previous_proces_date, s.process_date) AS FLOAT) / 86400 as 'İşlem Süresi (Onaycı)',
tdf_approval_group as surec,
l.max_process_date as 'Talep Onay Tamamlanma Tarihi',
CAST(DATEDIFF(SECOND,k.process_date, l.max_process_date) AS FLOAT) / 86400 as 'Talep Onay Tamamlanma Süresi (Gün)'
FROM {{ ref('stg__scm_kpi_t_fact_processingtimes') }} s
LEFT JOIN 
		(SELECT id,process_date
		 FROM {{ ref('stg__scm_kpi_t_fact_processingtimes') }}
		 WHERE tdf_approval_group = 'Talep Yaratilma') k on k.[id] = s.id
LEFT JOIN 
		(SELECT id,MAX(process_date) as max_process_date
		 FROM {{ ref('stg__scm_kpi_t_fact_processingtimes') }}
		 WHERE process_category = 'Talep'
		 group by id) l on l.[id] = s.id
where 1=1
	--and s.first_pr_id = 'PR3093'
	and s.process_category = 'Talep'
	--and s.tdf_approval_group != 'Talep Yaratilma'
--ORDER BY s.process_date
),
satinalma_grupları as (
SELECT 
    sub.zzaribareqno,
    STRING_AGG(sub.eknam, ',') AS eknam
FROM (
    SELECT DISTINCT 
        b.zzaribareqno,
        t.eknam
    FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eban') }} b
    LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t024') }} t 
        ON t.ekgrp = b.ekgrp
    WHERE b.zzaribareqno != ''
) sub
GROUP BY sub.zzaribareqno
)
,onaycı_sayısı as (
SELECT
[Talep Numarası],
COUNT(distinct [Satınalma Personeli]) onaycı_sayısı
FROM full_talep_onay
group by [Talep Numarası]
)
,t as (
select
a.*,
ROW_NUMBER() OVER(PARTITION BY a.requisitionnumber order by try_cast(concat(crdat,' ',crzet) as datetime) desc) son_crdat
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_509') }}  a 
where requisitionnumber != ''
and srid != ''
),
t_filtered as (
select *
 from t where son_crdat = '1'
 )
,new_sr as (
select 
*,
ROW_NUMBER() OVER(PARTITION BY srid ORDER BY try_cast(concat(crdat,' ',crzet) as datetime) DESC ) rn_1
from  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_511T') }} 
where srid != '' 
)
,new_sr_filtered as (
SELECT
*
FROM new_sr WHERE rn_1= '1')
,
doc_types as (
SELECT DISTINCT
zzaribareqno,
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
where zzaribareqno != ''
),
aggregated_doc_types as (
SELECT
STRING_AGG(document_type,',') document_types,
zzaribareqno
FROM doc_types
GROUP BY zzaribareqno
)
,a as (
SELECT

rls_key=CONCAT(prj.rls_businessarea, '-', prj.rls_company, '-', prj.rls_group)
,prj.rls_region 
,prj.rls_group 
,prj.rls_company 
,prj.rls_businessarea 
,f.[Talep Numarası] as talep_numarasi,
pr.Name as talep_konusu,
--f.Şirket as srikett,
comp.Tanim as [sirket],
prj.project_name as [proje],
t1.land1 as country,
t1.name1 as project_name,
pr.StatusString as status,
--f.Proje as s,
[Satınalma Personeli] as satinalma_personeli,
sa.eknam as satınalma_grubu_uzun,
pr.PurchaseOrg_UniqueName AS satinalma_grubu,
[Talep Oluşturma Tarihi] as talep_olusturma_tarihi,
[Talep Onaya Sunma Tarihi] as talep_onaya_sunma_tarihi,
[Talep Onay Tarihi] as talep_onay_tarihi, 
ROUND([İşlem Süresi (Onaycı)], 3) as islem_suresi_onayci_gun ,
[Talep Onay Tamamlanma Tarihi] as talep_onay_tamamlanma_tarihi,
ROUND([Talep Onay Tamamlanma Süresi (Gün)], 3) as talep_onay_tamamlanma_suresi_gun,
os.onaycı_sayısı,
surec
,doclar.owner_name as satınalmacı_gercek
,adt.document_types as document_type
FROM full_talep_onay f
	LEFT JOIN {{ source('stg_scm_kpi', 'raw__scm_kpi_t_fact_aribaprocurementrequestsnew') }} pr on pr.InitialUniqueName = f.[Talep Numarası]
	LEFT JOIN  {{ ref('dm__dimensions_t_dim_projects') }}  prj on prj.[business_area] = LEFT(f.Proje,4)
	LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }}  t1 on t1.werks = LEFT(f.Proje,4)
	LEFT JOIN  {{ ref('dm__dimensions_t_dim_companies') }}  comp on comp.[RobiKisaKod] = f.[Şirket]
	left join satinalma_grupları sa on sa.zzaribareqno = f.[Talep Numarası]
	left join onaycı_sayısı os on os.[Talep Numarası]= f.[Talep Numarası]
	left join t_filtered sr_yaratma ON sr_yaratma.requisitionnumber = f.[Talep Numarası]
	left join  new_sr_filtered doclar on sr_yaratma.srid=doclar.srid
	left join aggregated_doc_types adt on adt.zzaribareqno= f.[Talep Numarası]
	WHERE 1=1
		AND [Talep Oluşturma Tarihi] IS NOT NULL
)
SELECT
*
FROM a
where 1=1
and surec != 'Talep Yaratilma'
and rls_region is not null
AND talep_numarasi  NOT IN  
						(
						select distinct
						created_pr_id
						 from aws_stage.s4_odata.raw__s4hana_t_sap_zmm_t_tdf_header ) 

 