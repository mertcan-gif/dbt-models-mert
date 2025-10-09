{{
  config(
    materialized = 'view',tags = ['hr_kpi']
    )
}}
		SELECT
			hierarcy_level = 'a_level'
			,CAST(pl.sf_sicil AS nvarchar(max)) as sf_scil
			,CONCAT(pl.first_name, ' ', pl.last_name) AS Personel
			,pl.manager_jobinfo
			,[Nesne Türü] = 
				CASE 
					WHEN leg.head_of_unit_position IS NULL THEN 'Pozisyon'
					ELSE 'Grup/Başkanlık'
				END
			,CASE
				WHEN leg.head_of_unit_position IS NOT NULL OR org.[yonetici_mi?]=1 THEN 1
				ELSE 0
			  END AS flag
			,CASE 
				WHEN leg.head_of_unit_position IS NULL THEN org.pozisyon_adi
				ELSE leg.[entity_name]
			END	 AS NesneAdı
			
			,CASE 
				WHEN leg.head_of_unit_position IS NULL THEN CAST(org.pozisyon_kodu AS nvarchar(max))
				ELSE cast(leg.[entity_code] as nvarchar(max))
			END	 AS NesneKodu
			,cast(org.pozisyon_kodu as nvarchar(max)) as [pozisyon_kodu]
			,(CASE
			WHEN leg.[icra_kurullari_(cust_group7)] IS NOT NULL THEN 'İCRA KURULLARI'
					ELSE
						(CASE
							WHEN leg.[yonetim_kurullari_(cust_group6)] IS NOT NULL THEN 'YÖNETİM KURULLARI'
							ELSE
								(CASE
									WHEN leg.[gruplar_(cust_group5)] IS NOT NULL THEN 'GRUPLAR'
									ELSE
										(CASE
											WHEN leg.[sub_holding_seviyeleri(cust_group4)] IS NOT NULL THEN 'SUB HOLDİNG SEVİYELERİ'
											ELSE
												(CASE
													WHEN leg.[holding_seviyeleri_(cust_group3)] IS NOT NULL THEN 'HOLDİNG SEVİYELERİ'
													ELSE
														(CASE
															WHEN leg.[degerlendirme_kurulu_(cust_group2)] IS NOT NULL THEN 'DEĞERLENDİRME KURULU'
															ELSE leg.[entity_name]
														END)
												END)
										END)
								END)
						END)
END) AS [Bağlı olduğu nesne (Türü)]
						
			,[Bağlı olduğu nesne (Adı)] = COALESCE (
						 leg.[icra_kurullari_adi_(cust_group7)] 
						,leg.[yonetim_kurullari_adi_(cust_group6)]
						,leg.[gruplar_adi_(cust_group5)]
						,leg.[sub_holding_seviyeleri_adi_(cust_group4)]
						,leg.[holding_seviyeleri_adi_(cust_group3)]
						,leg.[degerlendirme_kurulu_adi_(cust_group_2)]
						,leg.[entity_name] 
						)
			, pl.manager_position
			,[Bağlı olduğu nesne kodu] = COALESCE (
						 leg.[icra_kurullari_(cust_group7)] 
						,leg.[yonetim_kurullari_(cust_group6)]
						,leg.[gruplar_(cust_group5)] 
						,leg.[sub_holding_seviyeleri(cust_group4)] 
						,leg.[holding_seviyeleri_(cust_group3)] 
						,leg.[degerlendirme_kurulu_(cust_group2)] 
						,leg.[entity_code] 
						)
			,[Organizasyon Şeması Bağlı olduğu nesne (Türü)] =
	CASE
			WHEN leg.[icra_kurullari_adi_(cust_group7)] IS NOT NULL THEN 'İCRA KURULLARI'
    ELSE
        CASE
            WHEN leg.[yonetim_kurullari_adi_(cust_group6)] IS NOT NULL THEN 'YÖNETİM KURULLARI'
            ELSE
                CASE
                    WHEN leg.[gruplar_adi_(cust_group5)] IS NOT NULL THEN 'GRUPLAR'
                    ELSE
                        CASE
                            WHEN leg.[sub_holding_seviyeleri_adi_(cust_group4)] IS NOT NULL THEN 'SUB HOLDİNG SEVİYELERİ'
                            ELSE
                                CASE
                                    WHEN leg.[holding_seviyeleri_adi_(cust_group3)] IS NOT NULL THEN 'HOLDİNG SEVİYELERİ'
                                    ELSE
                                        CASE
                                            WHEN leg.[degerlendirme_kurulu_adi_(cust_group_2)] IS NOT NULL THEN 'DEĞERLENDİRME KURULU'
                                            ELSE leg.[entity_name]
                                        END
                                END
                        END
                END
        END
END
		, [Organizasyon Şeması Bağlı olduğu nesne (Kodu)] =
			COALESCE (
						 leg.[icra_kurullari_(cust_group7)] 
						,leg.[yonetim_kurullari_(cust_group6)]
						,leg.[gruplar_(cust_group5)] 
						,leg.[sub_holding_seviyeleri(cust_group4)] 
						,leg.[holding_seviyeleri_(cust_group3)] 
						,leg.[degerlendirme_kurulu_(cust_group2)] 
						,leg.[entity_code] 
						)
		,[Organizasyon Şeması Bağlı olduğu nesne (Adı)] = 
		CASE
			WHEN leg.[icra_kurullari_adi_(cust_group7)] IS NOT NULL THEN 'İCRA KURULLARI'
    ELSE
        CASE
            WHEN leg.[yonetim_kurullari_adi_(cust_group6)] IS NOT NULL THEN 'YÖNETİM KURULLARI'
            ELSE
                CASE
                    WHEN leg.[gruplar_adi_(cust_group5)] IS NOT NULL THEN 'GRUPLAR'
                    ELSE
                        CASE
                            WHEN leg.[sub_holding_seviyeleri_adi_(cust_group4)] IS NOT NULL THEN 'SUB HOLDİNG SEVİYELERİ'
                            ELSE
                                CASE
                                    WHEN leg.[holding_seviyeleri_adi_(cust_group3)] IS NOT NULL THEN 'HOLDİNG SEVİYELERİ'
                                    ELSE
                                        CASE
                                            WHEN leg.[degerlendirme_kurulu_adi_(cust_group_2)] IS NOT NULL THEN 'DEĞERLENDİRME KURULU'
                                            ELSE leg.[entity_name]
                                        END
                                END
                        END
                END
        END
END
			 ,org.[yonetici_mi?]
			 ,pl.parent_position_code
			 ,pl.parent_position_title
			 ,pl.pozisyon_kodu as p_kodu
			 ,leg.head_of_unit_position
			 ,pl.position_a_Level as position_a_level
			 ,pl.position_paa as position_paa
			 ,pl.position_b_Level
			 ,pl.position_c_Level
			 ,pl.position_d_Level
			 ,pl.adi_birim as position_e_level
	FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_personnellist') }} pl
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchart') }}  org ON pl.sf_sicil = org.gorevdeki_kisi
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_legalentity') }} leg ON pl.pozisyon_kodu = leg.head_of_unit_position