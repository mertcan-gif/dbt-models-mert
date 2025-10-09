{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}
With a as (
SELECT
 [Kullanıcı Kodu] as user_code
,[Grup/Başkanlık] as company
,[İçerik Entegrasyon Kodu] as integration_code
,[İçerik Adı] as content_name
,[İçerik Tipi] as content_code
,[İçerik Kategorisi] as content_cat
,[Başlangıç Tarihi] as start_date
,[Bitiş Tarihi] as end_date
,[Puanı] as point
,[Başarı Durumu] as success_status
,[Etkinlik Adı]  as event_name 
,[Eğitim Kategorisi] as category
,[etkinlik yönetici] as event_organizer
,[Deneyim Süresi(Dakika)] as duration
,[Year]
,Month as month 
,case 
	 when [Grup/Başkanlık] = N'RÖNESANS SUPPLY CHAIN' THEN 'RSC'
	 when [Grup/Başkanlık] = N'YATIRIM FİNANSAL HİZMETLER' THEN 'HQ'
	 when [Grup/Başkanlık] = N'RÖNESANS MERKEZİ HİZMETLER' THEN 'HQ'
	 when [Grup/Başkanlık] = N'RÖNESANS MAKİNE VE STOK YÖNETİMİ' THEN 'RCT'
	 when [Grup/Başkanlık] = N'DESNA GAYRİMENKUL YATIRIM HOLDİNG' THEN 'DMG'
	 when [Grup/Başkanlık] = N'RÖNESANS ENDÜSTRİ TESİSLERİ' THEN 'RET'
	 when [Grup/Başkanlık] = N'TR İNŞAAT ÜSTYAPI' THEN 'RECÜ'
	 when [Grup/Başkanlık] = N'RÖNESANS GAYRİMENKUL YATIRIM' THEN 'RGY'
	 when [Grup/Başkanlık] = N'SAĞLIK İŞLETME HİZMETLERİ' THEN 'RSY'
	 when [Grup/Başkanlık] = N'RÖNESANS SAĞLIK YATIRIM' THEN 'RSY'
	 when [Grup/Başkanlık] = N'TR İNŞAAT ALTYAPI' THEN 'RECA'
	 when [Grup/Başkanlık] = N'ENERJİ' THEN 'REN'
  else null end as company_adjusted
,case 
	 when [Grup/Başkanlık] = N'RÖNESANS SUPPLY CHAIN' THEN 'RSC'
	 when [Grup/Başkanlık] = N'YATIRIM FİNANSAL HİZMETLER' THEN 'HOL'
	 when [Grup/Başkanlık] = N'RÖNESANS MERKEZİ HİZMETLER' THEN 'HOL'
	 when [Grup/Başkanlık] = N'RÖNESANS MAKİNE VE STOK YÖNETİMİ' THEN 'RCT'
	 when [Grup/Başkanlık] = N'DESNA GAYRİMENKUL YATIRIM HOLDİNG' THEN 'DMG'
	 when [Grup/Başkanlık] = N'RÖNESANS ENDÜSTRİ TESİSLERİ' THEN 'RET'
	 when [Grup/Başkanlık] = N'TR İNŞAAT ÜSTYAPI' THEN 'REC'
	 when [Grup/Başkanlık] = N'RÖNESANS GAYRİMENKUL YATIRIM' THEN 'RGY'
	 when [Grup/Başkanlık] = N'SAĞLIK İŞLETME HİZMETLERİ' THEN 'RSY'
	 when [Grup/Başkanlık] = N'RÖNESANS SAĞLIK YATIRIM' THEN 'RSY'
	 when [Grup/Başkanlık] = N'TR İNŞAAT ALTYAPI' THEN 'REC'
	 when [Grup/Başkanlık] = N'ENERJİ' THEN 'REN'
  else null end as company_adjusted_for_rls
,[db_upload_timestamp]
FROM  {{ ref('stg__sustain_kpi_v_fact_training') }} t
)
SELECT
	 rls_region = k.RegionCode
	,rls_group = CONCAT(k.KyribaGrup,'_',k.RegionCode)
	,rls_company = CONCAT(a.company_adjusted_for_rls,'_',k.RegionCode)
	,rls_businessarea = CONCAT('_',k.RegionCode)
    ,a.*
FROM a
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} k ON k.RobiKisaKod = a.company_adjusted_for_rls