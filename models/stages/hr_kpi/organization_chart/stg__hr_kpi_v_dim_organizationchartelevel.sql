{{
  config(
    materialized = 'view',tags = ['hr_kpi']
    )
}}		
                
	SELECT 
			hierarcy_level = 'e_level'
			,CAST(pl.sf_sicil AS nvarchar(max)) as sf_scil
			,CONCAT(pl.first_name, ' ', pl.last_name) AS Personel
			,pl.manager_jobinfo
			,[Nesne Türü] = 
				CASE 
					WHEN level_entity.head_of_unit_position IS NULL THEN 'Pozisyon'
					ELSE 'Birim'
				END
			,CASE
				WHEN level_entity.head_of_unit_position IS NOT NULL OR org.[yonetici_mi?]=1 THEN 1
				ELSE 0
			  END AS flag
			,CASE 
				WHEN level_entity.head_of_unit_position IS NULL THEN org.pozisyon_adi
				ELSE level_entity.[entity_name]
			END	 AS NesneAdı
			,CASE 
				WHEN level_entity.head_of_unit_position IS NULL THEN cast(org.pozisyon_kodu as nvarchar(max))
				ELSE level_entity.[entity_code] 
			END	AS NesneKodu
			,cast(org.pozisyon_kodu as nvarchar(max)) as [pozisyon_kodu]
			,[Bağlı olduğu nesne (Türü)] = CASE WHEN [pstn_entity_parent_code] IS NULL THEN NULL ELSE  'Bölüm/Proje/İşletme' END
			,[Bağlı olduğu nesne (Adı)] = [pstn_entity_parent_name]
			,pl.manager_position
			,[Bağlı olduğu nesne kodu] = [pstn_entity_parent_code]
			,[Organizasyon Şeması Bağlı olduğu nesne (Türü)] =
				CASE 
					WHEN level_entity.[org_entity_samelevel_code] IS NOT NULL THEN 'Birim'
					WHEN level_entity.head_of_unit_position IS NOT NULL THEN 'Bölüm/Proje/İşletme'
					--WHEN level_entity.
					ELSE NULL
				END	  
			, [Organizasyon Şeması Bağlı olduğu nesne (Kodu)] =
				CASE 
					WHEN level_entity.[org_entity_samelevel_code] IS NULL THEN level_entity.[pstn_entity_parent_code]
					ELSE level_entity.[org_entity_samelevel_code]
				END	 
			,[Organizasyon Şeması Bağlı olduğu nesne (Adı)] =
				CASE 
					WHEN level_entity.[org_entity_samelevel_code] IS NULL THEN level_entity.[pstn_entity_parent_name]
					ELSE level_entity.org_entity_samelevel_name
				END
			,org.[yonetici_mi?]
			,pl.parent_position_code
			,pl.parent_position_title
			,pl.pozisyon_kodu as p_kodu
			,level_entity.head_of_unit_position
			 ,pl.position_a_Level as position_a_level
			 ,pl.position_paa as position_paa
			,pl.position_b_Level
			,pl.position_c_Level
			,pl.position_d_Level
			,pl.adi_birim as position_e_level
		FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_personnellist') }} pl
			LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_department') }} level_entity ON pl.pozisyon_kodu = level_entity.head_of_unit_position
			LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchart') }} org ON pl.sf_sicil = org.gorevdeki_kisi
		