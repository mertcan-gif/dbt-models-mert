{{
  config(
    materialized = 'view',tags = ['hr_kpi']
    )
}}

WITH language_dependent AS (
    /***
    Dökümantasyon:
    Tarih: 2023-31-05
    Yazan: Kaan Keskin

    Power BI'a ve AWS'e gitmeden önce ECSAC tablosu ile aldığımız personel listesini
        -EN
        -TR
        -RU
    olacak şekilde her bir personeli 3 dilde kırmaktayız, bazı kolonlar 3 dilde de aynı bazıları ise farklı yazılmaktadır.
    Bu view ''ÜÇ DİLDE DE FARKLI YAZILAN KOLONLARI içermektedir!

    Burada oluşturulan bir personel için 3 dilde row gelmesi durumunu, [hr_kpi].[DWH_Stage_hr_kpiV1_v_SAC_LanguageIndependent] view'u ile birleştirdiğimizde
    nihai view'a ulaşmaktayız. 
    Not:
    - [hr_kpi].[DWH_Stage_hr_kpiV1_v_SAC_LanguageIndpendent] 'viewunda tek bir kişi bir kez tekrar ederken
    - [hr_kpi].[DWH_Stage_hr_kpiV1_v_SAC_LanguageDependent] 'viewunda bir kişi 3 kez tekrar eder, bu iki view join edildiğinde bir kişi 3 kez her dil için tekrar etmektedir.
    language filtresinden filtrelenerek istenilen veriye ulaşılabilir.
    İki kolonu birleştiren key kolonu: [sf_id_number] yani [user_id]'dir
    ***/



    /*** İngilizce Kolonlar: ***/


    SELECT 
        [sf_id_number] = [user_id]
        ,[language] = 'EN'
        ,[dwh_data_group] = 'WHITE COLLAR'
        ,[name] = CASE WHEN sac.middle_name IS NULL THEN UPPER(COALESCE(sac.first_name_lat,sac.first_name)) ELSE UPPER(CONCAT(COALESCE(sac.first_name_lat,sac.first_name),' ',COALESCE(sac.middle_name_lat,sac.middle_name))) END
        ,[surname] = UPPER(COALESCE(sac.last_name_lat,sac.last_name))
        ,[full_name] = CONCAT(CASE WHEN sac.middle_name IS NULL THEN UPPER(COALESCE(sac.first_name_lat,sac.first_name)) ELSE UPPER(CONCAT(COALESCE(sac.first_name_lat,sac.first_name),' ',COALESCE(sac.middle_name_lat,sac.middle_name))) END,' ',UPPER(COALESCE(sac.last_name_lat,sac.last_name)))
        ,[event_reason] = UPPER(sac.neden_en)
        ,[payroll_company] = UPPER(sac.bordro_sirketi_en)
        ,[cost_center] = UPPER(sac.cost_center_en)
        ,[employee_group] = UPPER(sac.calisangrubu_r_en)
        ,[role] = UPPER(sac.gorev_tanimi_en)
        ,[a_level_group] = sac.[grup/baskanlik_en]
        ,[b_level_company] = UPPER(sac.is_birimi_en)
        ,[c_level_region] = UPPER([bolge/fonksiyon/bu_en])
        ,[d_level_department] = UPPER(sac.[bolum/projeler/isletmeler_en])
        ,[e_level_unit] = UPPER(sac.[birim_en])
        ,[collar_type] = 'WHITE COLLAR'
        ,[dwh_workplace] = CASE 
                                WHEN  sac.calisma_yeri_turu_en = N'Central' THEN N'Head Office' 
                                WHEN sac.calisma_yeri_turu_en= N'Corporate' THEN N'Facilities'
                                WHEN sac.calisma_yeri_turu_en= N'Site' THEN N'Site'
                                ELSE  sac.calisma_yeri_turu_en END
        ,[dwh_education_status] = egt.egitim_seviyesi_en
        ,[dwh_workplace_merged] = UPPER(sac.rapor_organizasyon_us)
    FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }} sac
        LEFT JOIN {{ ref('stg__hr_kpi_v_dim_maxeducation') }}  egt ON egt.person_id = sac.person_id

    UNION ALL 

    /*** Rusça Kolonlar: ***/
    SELECT 
        [sf_id_number] = [user_id]
        ,[language] = 'RU'
        ,[dwh_data_group] = N'БЕЛЫЙ ВОРОТНИЧОК'
        ,[name] = CASE WHEN sac.middle_name_ru IS NULL THEN UPPER(COALESCE(sac.first_name_ru,sac.first_name)) ELSE UPPER(CONCAT(COALESCE(sac.first_name_ru,sac.first_name),' ',COALESCE(sac.middle_name_ru,sac.middle_name))) END
        ,[surname] = UPPER(COALESCE(sac.last_name_ru,sac.last_name))
        ,[full_name] = CONCAT(CASE WHEN sac.middle_name_ru IS NULL THEN UPPER(COALESCE(sac.first_name_ru,sac.first_name)) ELSE UPPER(CONCAT(COALESCE(sac.first_name_ru,sac.first_name),' ',COALESCE(sac.middle_name_ru,sac.middle_name))) END,' ',UPPER(COALESCE(sac.last_name_ru,sac.last_name)))
        ,[event_reason] = UPPER(sac.neden_ru)
        ,[payroll_company] = UPPER(sac.bordro_sirketi_ru)
        ,[cost_center] = UPPER(sac.cost_center_ru)
        ,[employee_group] = UPPER(sac.calisangrubu_r_ru)
        ,[role] = UPPER(sac.gorev_tanimi_ru)
        ,[a_level_group] = sac.[grup/baskanlik_ru]
        ,[b_level_company] = UPPER(sac.is_birimi_ru)
        ,[c_level_region] = UPPER([bolge/fonksiyon/bu_ru])
        ,[d_level_department] = UPPER(sac.[bolum/projeler/isletmeler_ru])
        ,[e_level_unit] = UPPER(sac.[birim_ru])
        ,[collar_type] = N'БЕЛЫЙ ВОРОТНИЧОК'
        ,[dwh_workplace] = sac.calisma_yeri_turu_ru
        ,[dwh_education_status] = egt.egitim_seviyesi_tr
        ,[dwh_workplace_merged] = UPPER(sac.rapor_organizasyon_ru)
    FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }}  sac
        LEFT JOIN {{ ref('stg__hr_kpi_v_dim_maxeducation') }}  egt ON egt.person_id = sac.person_id
        UNION ALL 

    /*** Türkçe Kolonlar: ***/
    SELECT 
        [sf_id_number] = [user_id]
        ,[language] = 'TR'
        ,[dwh_data_group] = N'BEYAZ YAKA'
        ,[name] = CASE WHEN sac.middle_name IS NULL THEN UPPER(sac.first_name) ELSE UPPER(CONCAT(sac.first_name,' ',sac.middle_name)) END
        ,[surname] = UPPER(sac.last_name)
        ,[full_name] = CONCAT(CASE WHEN sac.middle_name IS NULL THEN UPPER(sac.first_name) ELSE UPPER(CONCAT(sac.first_name,' ',sac.middle_name)) END,' ',UPPER(sac.last_name))
        ,[event_reason] = UPPER(sac.neden_tr)
        ,[payroll_company] = UPPER(sac.bordro_sirketi_tr)
        ,[cost_center] = UPPER(sac.cost_center_tr)
        ,[employee_group] = UPPER(sac.calisangrubu_r_tr)
        ,[role] = UPPER(sac.gorev_tanimi_tr)
        ,[a_level_group] = sac.[grup/baskanlik_tr]
        ,[b_level_company] = UPPER(sac.is_birimi_tr)
        ,[c_level_region] = UPPER([bolge/fonksiyon/bu_tr])
        ,[d_level_department] = UPPER(sac.[bolum/projeler/isletmeler_tr])
        ,[e_level_unit] = UPPER(sac.[birim_tr])
        ,[collar_type] = N'BEYAZ YAKA'
        ,[dwh_workplace] = CASE 
                                WHEN sac.calisma_yeri_turu_tr= N'Şantiye' THEN 'Site' 
                                WHEN sac.calisma_yeri_turu_tr= N'Merkez' THEN 'Head Office' 
                                WHEN sac.calisma_yeri_turu_tr= N'İşletme' THEN 'Facilities' 
                                ELSE sac.calisma_yeri_turu_tr 
                        END
        ,[dwh_education_status] = egt.egitim_seviyesi_tr
        ,[dwh_workplace_merged] = UPPER(sac.rapor_organizasyon_tr)
    FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }} sac
        LEFT JOIN {{ ref('stg__hr_kpi_v_dim_maxeducation') }} egt ON egt.person_id = sac.person_id
)


SELECT *
FROM language_dependent