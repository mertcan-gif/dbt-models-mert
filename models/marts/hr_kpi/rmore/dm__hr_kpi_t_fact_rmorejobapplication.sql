{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

/* 
Date: 20250905
Creator: Adem Numan Kaya
Report Owner: Meyse Ozkeskin - HR Systems
Explanation: RPeople sistemine gecildikten sonra Coach sisteminde aday bilgleri aktarilmadi. HR tarafinin adaylara ulasabilmesi icin eski sistemde kalan aday bilgileri ile yeni sistemdeki aday bilgileri eslestirildi ve tek bir tablo halinde RMore'a yansitildi.
    Asagida education ve language tablolari var ve string_agg function ile birlikte tek bir satir haline getirilmisir.Tek bir tabloda ayni degeri birden fazla girdikleri icin oncesinde distinct atilarak deduplication yapilmistir.
    
*/

WITH deduplication_education_rpeople AS (
    SELECT DISTINCT
        application_id,
        name_of_school_tr
    FROM {{ source('stg_sf_odata','raw__hr_kpi_t_sf_job_application_education_newsf_rmore') }}
),

deduplication_education_rpeople_final AS (
    SELECT 
        application_id, 
        STRING_AGG(name_of_school_tr, ',') AS education_information
    FROM deduplication_education_rpeople
    GROUP BY application_id
),

deduplication_education_coach AS (
    SELECT DISTINCT
        application_id,
        name_of_school_tr
    FROM {{ source('stg_sf_odata','raw__hr_kpi_t_sf_job_application_education_rmore') }}
),

deduplication_education_coach_final AS (
    SELECT 
        application_id, 
        STRING_AGG(name_of_school_tr, ',') AS education_information
    FROM deduplication_education_coach
    GROUP BY application_id
),

deduplicated_languages_rpeople AS (
    SELECT DISTINCT
        [application_id],
        [language_tr],
        [writing_tr],
        [reading_tr],
        [speaking_tr]
    FROM {{ source('stg_sf_odata','raw__hr_kpi_t_sf_job_application_languages_newsf_rmore') }}
    WHERE [language_tr] IS NOT NULL 
        AND ([writing_tr] IS NOT NULL OR [speaking_tr] IS NOT NULL OR [reading_tr] IS NOT NULL)
),

deduplicated_languages_rpeople_final AS (
    SELECT 
        [application_id],
        STRING_AGG(
            CONCAT(
                'Writing: ', [language_tr], '-', [writing_tr], 
                ', Speaking: ', [language_tr], '-', [speaking_tr], 
                ', Reading: ', [language_tr], '-', [reading_tr]
            ), 
            '; '
        ) AS language_skills
    FROM deduplicated_languages_rpeople
    GROUP BY [application_id]
),

finals_combined AS (
    -- Query 1: NEW_SF Data Source
    SELECT 
        CONCAT(ja_new.first_name, ' ', ja_new.last_name) AS full_name,
        ja_new.first_name,
        ja_new.last_name,
        CASE 
            WHEN rq_new.template_id = '584' THEN 'PUSULA' 
            ELSE 'NORMAL' 
        END AS candidate_type,
        ja_new.contact_email,
        ja_new.tc_no,
        ja_new.phone_number,
        ja_new.candidate_id,
        ja_new.application_id,
        ja_new.egitim,
        ja_new.mezun_bolum,
        ja_new.zamaninda_tamamladimi,
        ja_new.aile,
        ja_new.alan_deneyim,
        ja_new.toplam_deneyim,
        ja_new.calisma_durumu,
        ja_new.bolge,
        ja_new.bulundugu_kaynak_detay,
        ja_new.country_code AS country_,
        ja_new.gorusen_kisi_teknik1,
        ja_new.gorusen_kisi_teknik2,
        ja_new.gorusen_kisi_teknik3,
        ja_new.iletisim,
        ja_new.kariyer_hedefi,
        ja_new.is_basitarihi,
        ja_new.referans,
        CAST(NULL AS NVARCHAR(255)) AS referans2,  -- Only in OLD_SF
        ja_new.status_comments,
        ja_new.teknik_degerledirme_not,
        ja_new.tgf_not,
        ja_new.ygf_not,
        ja_new.ygf_not2,
        ja_new.teklif_retnedenleri_tr AS teklif_retnedenleri,
        ja_new.onr_departman_tr,
        ja_new.oneren_yetkili_tr AS oneren_yetkili,
        ja_new.oneren_yetkilitxt,
        CAST(ja_new.oneren_yetkili_tarih AS DATE) AS oneren_yetkili_tarih,
        CAST(NULL AS NVARCHAR(255)) AS gorusen_departman_tgf,  -- Has CASE logic in OLD_SF
        CAST(NULL AS NVARCHAR(255)) AS gorusen_departman_ygf,  -- Has CASE logic in OLD_SF
        ja_new.gorusen_kisi_ik_ygf_tr AS gorusen_kisi_ik_ygf,
        ja_new.gorusen_kisi_dgr,
        CAST(ja_new.gorusme_tarihi_tgf AS DATE) AS gorusme_tarihi_tgf,
        ja_new.gorusen_kisi_ik_ygf_iki_tr AS gorusen_kisi_ik_ygf_iki,
        ja_new.gorusen_kisi_ik_ygf_uc_tr AS gorusen_kisi_ik_ygf_uc,
        CAST(ja_new.gorusme_tarihi AS DATE) AS gorusme_tarihi,
        ja_new.gorusen_departman2,
        ja_new.gorusme_tarihi2,
        ja_new.gorusen_departman3,
        ja_new.gorusme_tarihi3,
        ja_new.dogum_tarihi,
        ja_new.dogum_yeri,
        ja_new.uyruk_tr AS uyruk,
        ja_new.medeni_hali,
        CAST(ja_new.sigorta AS NVARCHAR(10)) AS sigorta,
        CAST(ja_new.ulasim AS NVARCHAR(10)) AS ulasim,
        CAST(ja_new.ticket_hakki_var_mi AS NVARCHAR(10)) AS ticket_hakki_var_mi,
        CAST(ja_new.yemek AS NVARCHAR(10)) AS yemek,
        CAST(ja_new.konaklama AS NVARCHAR(10)) AS konaklama,
        CAST(ja_new.telefon AS NVARCHAR(10)) AS telefon,
        CAST(ja_new.arac AS NVARCHAR(10)) AS arac,
        ja_new.durum_tr AS durum,
        ja_new.cinsiyet,
        ja_new.engellilik_durumu_tr AS engellilik_durumu,
        ja_new.askerlik_durumu_tr AS askerlik_durumu,
        ja_new.job_req_id,
        deduplicated_languages_rpeople_final.language_skills AS languages,
        deduplication_education_rpeople_final.education_information,
        ja_new.alinan_burslar,
        ja_new.calismak_istedigi_sehir1,
        ja_new.calismak_istedigi_sehir2,
        ja_new.calismak_istedigi_sehir3,
        ja_new.calismak_istedigi_departman1,
        ja_new.calismak_istedigi_departman2,
        ja_new.calismak_istedigi_departman3,
        ja_new.burslu_mu_okudu,
        ja_new.burs_yuzdesi,
        ja_new.rev_bursiyerimi,
        ja_new.son_bir_ay_sigortali_is,
        ja_new.yasadigi_sehir,
        ja_new.yasadigi_ilce,
        ja_new.video_gorusu,
        ja_new.hrpeak_tracking_number,
        ja_new.kisilik_envanter_test_sonucu_linki,
        ja_new.genel_yetenek_sonucu,
        ja_new.genel_kultur_sonucu,
        ja_new.ingilizce_test_sonucu,
        ja_new.universite_bolum_tercih_sebebi,
        ja_new.aile_bilgileri,
        ja_new.basarili_proje,
        ja_new.onplana_cikaran_yetkinlik,
        ja_new.gelistirilmesi_gereken_yon,
        ja_new.ingilizce_bilgisi,
        ja_new.sehir_ve_departman_tercih_nedeni,
        ja_new.status,
        ja_new.basvuru_statusu,
        CAST(NULL AS NVARCHAR(255)) AS title_postn,  -- Only in OLD_SF
        rq_new.position_number,
        rq_new.template_id,
        rq_new.company_group_name,
        rq_new.company_group_code,
        rq_new.b_level_name,
        rq_new.b_level_code,
        rq_new.payroll_company,
        rq_new.payroll_company_code,
        rq_new.c_level_name,
        rq_new.c_level_code,
        rq_new.d_level_name,
        rq_new.d_level_code,
        rq_new.e_level_name,
        rq_new.e_level_code,
        rq_new.personel_alani,
        rq_new.personel_alt_alani,
        'RPeople' AS source
    FROM {{ source('stg_sf_odata','raw__hr_kpi_t_sf_job_applications_newsf_rmore') }} AS ja_new
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_job_requisition_newsf_rmore') }} AS rq_new 
        ON ja_new.job_req_id = rq_new.job_req_id
    LEFT JOIN deduplicated_languages_rpeople_final 
        ON deduplicated_languages_rpeople_final.application_id = ja_new.application_id 
    LEFT JOIN deduplication_education_rpeople_final 
        ON deduplication_education_rpeople_final.application_id = ja_new.application_id 

    UNION ALL 

    -- Query 2: OLD_SF Data Source
    SELECT 
        CONCAT(ja.first_name, ' ', ja.last_name) AS full_name,
        ja.first_name,
        ja.last_name,
        CASE 
            WHEN rq.template_id = '2082' THEN 'PUSULA' 
            ELSE 'NORMAL'
        END AS candidate_type,
        ja.contact_email,
        ja.tc_no,
        ja.phone_number,
        ja.candidate_id,
        ja.application_id,
        ja.egitim,
        ja.mezun_bolum,
        ja.zamaninda_tamamladimi,
        ja.aile,
        ja.alan_deneyim,
        ja.toplam_deneyim,
        ja.calisma_durumu,
        ja.bolge,
        ja.bulundugu_kaynak_detay,
        ja.country_,
        ja.gorusen_kisi_teknik1,
        ja.gorusen_kisi_teknik2,
        ja.gorusen_kisi_teknik3,
        ja.iletisim,
        ja.kariyer_hedefi,
        ja.is_basitarihi,
        ja.referans,
        ja.referans2,
        ja.status_comments,
        ja.teknik_degerledirme_not,
        ja.tgf_not,
        ja.ygf_not,
        ja.ygf_not2,
        ja.teklif_retnedenleri_tr AS teklif_retnedenleri,
        ja.onr_departman_tr,
        ja.oneren_yetkili,
        ja.oneren_yetkilitxt,
        CAST(ja.oneren_yetkili_tarih AS DATE) AS oneren_yetkili_tarih,
        CASE 
            WHEN ja.gorusen_departman_tgf = '1' THEN N'IK Dışı'
            WHEN ja.gorusen_departman_tgf = '2' THEN N'IK'
            WHEN ja.gorusen_departman_tgf = '3' THEN N'3'
            ELSE ja.gorusen_departman_tgf
        END AS gorusen_departman_tgf,
        CASE 
            WHEN ja.gorusen_departman_ygf = '1' THEN N'IK Dışı'
            WHEN ja.gorusen_departman_ygf = '2' THEN 'IK'
            ELSE ja.gorusen_departman_ygf
        END AS gorusen_departman_ygf,
        ja.gorusen_kisi_ik_ygf_tr AS gorusen_kisi_ik_ygf,
        ja.gorusen_kisi_dgr,
        CAST(ja.gorusme_tarihi_tgf AS DATE) AS gorusme_tarihi_tgf,
        ja.gorusen_kisi_ik_ygf_iki_tr AS gorusen_kisi_ik_ygf_iki,
        ja.gorusen_kisi_ik_ygf_uc_tr AS gorusen_kisi_ik_ygf_uc,
        CAST(ja.gorusme_tarihi AS DATE) AS gorusme_tarihi,
        ja.gorusen_departman2,
        ja.gorusme_tarihi2,
        ja.gorusen_departman3,
        ja.gorusme_tarihi3,
        ja.dogum_tarihi,
        ja.dogum_yeri,
        ja.uyruk,
        ja.medeni_hali,
        CAST(ja.sigorta AS NVARCHAR(10)) AS sigorta,
        CAST(ja.ulasim AS NVARCHAR(10)) AS ulasim,
        CAST(ja.ticket_hakki_var_mi AS NVARCHAR(10)) AS ticket_hakki_var_mi,
        CAST(ja.yemek AS NVARCHAR(10)) AS yemek,
        CAST(ja.konaklama AS NVARCHAR(10)) AS konaklama,
        CAST(ja.telefon AS NVARCHAR(10)) AS telefon,
        CAST(ja.arac AS NVARCHAR(10)) AS arac,
        ja.durum,
        ja.cinsiyet,
        ja.engellilik_durumu_tr AS engellilik_durumu,
        ja.askerlik_durumu,
        ja.job_req_id,
        ja.languages,
        deduplication_education_coach_final.education_information,
        ja.alinan_burslar,
        ja.calismak_istedigi_sehir1,
        ja.calismak_istedigi_sehir2,
        ja.calismak_istedigi_sehir3,
        ja.calismak_istedigi_departman1,
        ja.calismak_istedigi_departman2,
        ja.calismak_istedigi_departman3,
        ja.burslu_mu_okudu,
        ja.burs_yuzdesi,
        ja.rev_bursiyerimi,
        ja.son_bir_ay_sigortali_is,
        ja.yasadigi_sehir,
        ja.yasadigi_ilce,
        ja.video_gorusu,
        ja.hrpeak_tracking_number,
        ja.kisilik_envanter_test_sonucu_linki,
        ja.genel_yetenek_sonucu,
        ja.genel_kultur_sonucu,
        ja.ingilizce_test_sonucu,
        ja.universite_bolum_tercih_sebebi,
        ja.aile_bilgileri,
        ja.basarili_proje,
        ja.onplana_cikaran_yetkinlik,
        ja.gelistirilmesi_gereken_yon,
        ja.ingilizce_bilgisi,
        ja.sehir_ve_departman_tercih_nedeni,
        ja.status,
        ja.basvuru_statusu,
        rq.title_postn,
        rq.position_number,
        rq.template_id,
        rq.company_group_name,
        rq.company_group_code,
        rq.b_level_name,
        rq.b_level_code,
        rq.payroll_company,
        rq.payroll_company_code,
        rq.c_level_name,
        rq.c_level_code,
        rq.d_level_name,
        rq.d_level_code,
        rq.e_level_name,
        rq.e_level_code,
        rq.personel_alani,
        rq.personel_alt_alani,
        'Coach' AS source
    FROM {{ source('stg_sf_odata','raw__hr_kpi_t_sf_job_applications_rmore') }} AS ja
    LEFT JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_job_requisition_rmore') }} AS rq 
        ON ja.job_req_id = rq.job_req_id
    LEFT JOIN deduplication_education_coach_final 
        ON deduplication_education_coach_final.application_id = ja.application_id 
)

SELECT
    N'NAN' AS rls_region,
    N'GR_0000_NAN' AS rls_group,
    N'CO_0000_NAN' AS rls_company,
    N'BA_0000_NAN' AS rls_businessarea,
    *
FROM finals_combined;