{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}
-- new comment
WITH a_level AS (
	SELECT *
	FROM {{ ref('stg__hr_kpi_v_dim_organizationchartalevel') }}
)

,b_level AS (
	SELECT *
	FROM {{ ref('stg__hr_kpi_v_dim_organizationchartblevel') }}
)

,c_level AS (
	SELECT *
	FROM {{ ref('stg__hr_kpi_v_dim_organizationchartclevel') }}
)

,d_level AS (
	SELECT *
	FROM {{ ref('stg__hr_kpi_v_dim_organizationchartdlevel') }}
)
,e_level AS (
	SELECT *
	FROM {{ ref('stg__hr_kpi_v_dim_organizationchartelevel') }}
)

,cte_2 as (

SELECT * FROM a_level 

UNION ALL

SELECT * FROM b_level

UNION ALL

SELECT * FROM c_level 

UNION ALL

SELECT * FROM d_level 

UNION ALL

SELECT * FROm e_level

)


,CTE_2_WithRowNumber AS (

----- a/b/c/d/e levellara her personel için hiyerarsi atadım. -----

    SELECT
        cte_2.*,
        ROW_NUMBER() OVER (PARTITION BY sf_scil ORDER BY sf_scil,hierarcy_level DESC) AS row_num
    FROM
        cte_2
	GROUP BY 
		   cte_2.NesneAdı
		  ,cte_2.[Nesne Türü]
		  ,cte_2.NesneKodu
		  ,cte_2.pozisyon_kodu
		  ,cte_2.[Bağlı olduğu nesne (Adı)]
		  ,cte_2.[Bağlı olduğu nesne (Türü)]
		  ,cte_2.[Bağlı olduğu nesne kodu]
		  ,cte_2.[Organizasyon Şeması Bağlı olduğu nesne (Adı)]
		  ,cte_2.[Organizasyon Şeması Bağlı olduğu nesne (Kodu)]
		  ,cte_2.[Organizasyon Şeması Bağlı olduğu nesne (Türü)]
		  ,cte_2.[yonetici_mi?]
		  ,cte_2.hierarcy_level
		  ,cte_2.flag
		  ,cte_2.sf_scil
		  ,cte_2.Personel
		  ,cte_2.manager_jobinfo
		  ,cte_2.manager_position
		  ,cte_2.parent_position_code
		  ,cte_2.parent_position_title
		  ,cte_2.p_kodu
		  ,cte_2.head_of_unit_position
		  ,cte_2.position_a_level 
		  ,cte_2.position_paa
		  ,cte_2.position_b_Level
			,cte_2.position_c_Level
			,cte_2.position_d_Level
			,cte_2.position_e_level )
, CTE_3 AS (

--	YONETICILER ICIN ----
    SELECT 
		ROW_NUMBER() OVER(PARTITION BY sf_scil, [Nesne Türü] ORDER BY hierarcy_level) as 'ana_row_2'
		,e_row = 0
        ,CTE_2_WithRowNumber.*
    FROM
        CTE_2_WithRowNumber
    WHERE 1=1
		AND [yonetici_mi?] = '1' 
		AND flag = 1
	GROUP BY 
		   CTE_2_WithRowNumber.NesneAdı
		  ,CTE_2_WithRowNumber.[Nesne Türü]
		  ,CTE_2_WithRowNumber.NesneKodu
		  ,CTE_2_WithRowNumber.pozisyon_kodu
		  ,CTE_2_WithRowNumber.[Bağlı olduğu nesne (Adı)]
		  ,CTE_2_WithRowNumber.[Bağlı olduğu nesne (Türü)]
		  ,CTE_2_WithRowNumber.[Bağlı olduğu nesne kodu]
		  ,CTE_2_WithRowNumber.[Organizasyon Şeması Bağlı olduğu nesne (Adı)]
		  ,CTE_2_WithRowNumber.[Organizasyon Şeması Bağlı olduğu nesne (Kodu)]
		  ,CTE_2_WithRowNumber.[Organizasyon Şeması Bağlı olduğu nesne (Türü)]
		  ,CTE_2_WithRowNumber.[yonetici_mi?]
		  ,CTE_2_WithRowNumber.hierarcy_level
		  ,CTE_2_WithRowNumber.flag
		  ,CTE_2_WithRowNumber.sf_scil
		  ,CTE_2_WithRowNumber.Personel
		  ,CTE_2_WithRowNumber.manager_jobinfo
		  ,CTE_2_WithRowNumber.manager_position
		  ,CTE_2_WithRowNumber.row_num
		  ,CTE_2_WithRowNumber.parent_position_code
		  ,CTE_2_WithRowNumber.parent_position_title
		  ,CTE_2_WithRowNumber.p_kodu
		  ,CTE_2_WithRowNumber.head_of_unit_position
		  ,CTE_2_WithRowNumber.position_a_level 
		  ,CTE_2_WithRowNumber.position_paa
		,CTE_2_WithRowNumber.position_b_Level
		,CTE_2_WithRowNumber.position_c_Level
		,CTE_2_WithRowNumber.position_d_Level
		,CTE_2_WithRowNumber.position_e_level
)


, CTE_4 AS (

----- YONETICI OLMAYANLAR ICIN --------

	SELECT 
	ana_row_2 = 1
	,e_row = 0
	,c2wn.*
	FROM
	CTE_2_WithRowNumber c2wn
	WHERE 1=1
		  --AND [yonetici_mi?] !=1
		  AND c2wn.row_num = '1'
		)
, CTE_4_5 AS (

----- YONETICI  olmayanlar icin --------

	SELECT
	ana_row_2 = 1
	,e_row = 0
	,c2.*
	FROM CTE_2_WithRowNumber c2
	WHERE 1=1
		  AND [yonetici_mi?] !=1
		  AND c2.head_of_unit_position = c2.p_kodu

		)

--, CTE_4_6 AS (

----- YONETICI OLMAYANLAR icin --------

--	SELECT
--	ana_row_2 = 1
--	,e_row = 1
--	,c2.*
---	FROM CTE_2_WithRowNumber c2
--	WHERE 1=1
--		  AND [yonetici_mi?] !=1
--		  and hierarcy_level = 'e_level'
--		)

, CTE_5 as (
SELECT  * FROM CTE_3 c3 WHERE c3.ana_row_2=1
UNION ALL
SELECT  * FROM CTE_4
UNION ALL
SELECT  * FROM CTE_4_5
--UNION ALL
--SELECT  * FROM CTE_4_6
)
, cte_6 as (
SELECT DISTINCT
		 CTE_5.position_a_level as [Grup/Başkanlık]
		,CTE_5.position_paa as [Personel Alt Alanı]
		,CTE_5.position_b_Level as [Şirket]
		,CTE_5.position_c_Level as [Bölge/Fonksiyon/BU]
		,CTE_5.position_d_Level as [Bölüm/Projeler/İşletmeler]
		,CTE_5.position_e_level as [Birim]
		,CASE WHEN ([Nesne Türü]= 'Pozisyon' and e_row = 0) THEN 'Pozisyon' ELSE hierarcy_level END AS hierarcy_level
		,[sf_scil] as sf_sicil
		,[Personel]
		,[manager_jobinfo] as 'Yönetici'
		,[Nesne Türü]
		,[flag]
		,[NesneAdı]
		,[NesneKodu]
		,CTE_5.[pozisyon_kodu]
		,(CASE WHEN	([Nesne Türü]= 'Pozisyon' ) THEN parent_position_title ELSE [Bağlı olduğu nesne (Adı)] END) AS 'Pozisyon Organizasyon Şeması Adı'
		,[manager_position] AS 'Pozisyon Organizasyon Şeması Yönetici'
		,(CASE WHEN ([Nesne Türü]= 'Pozisyon' ) THEN [parent_position_code] ELSE [Bağlı olduğu nesne kodu] END) AS 'Pozisyon Organizasyon Şeması Kodu'
		,(CASE WHEN ([Nesne Türü]= 'Pozisyon' ) THEN 'Pozisyon' ELSE [Bağlı olduğu nesne (Türü)] END) AS 'Pozisyon Organizasyon Şeması Türü'
		,(CASE 
			WHEN [Nesne Türü] = 'Pozisyon' THEN 
				COALESCE (
						 (CASE WHEN (select dp1.entity_code  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_legalentity') }}  dp1 where opo.[legal_entity_code] = dp1.entity_code) IS NOT NULL THEN 'Grup/Baskanlik' END)
						,(CASE WHEN (select dp2.entity_code  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_businessunit') }} dp2 where opo.[business_unit_code] = dp2.entity_code)  IS NOT NULL THEN 'Sirket' END)  
						,(CASE WHEN (select dp3.entity_code from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_division')}} dp3 where opo.[division_code] = dp3.entity_code) IS NOT NULL THEN 'Bölge/fonksiyon/BU' END)
						,(CASE WHEN (select dp4.entity_code from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_subdivision')}} dp4 where opo.[sub_division_code] = dp4.entity_code) IS NOT NULL THEN 'bölüm/proje/işletme' END) 
						,(CASE WHEN (select dp5.entity_code from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_department') }} dp5 where opo.[departmenr_code]= dp5.entity_code) IS NOT NULL THEN 'Birim' END)
						,(CASE WHEN opo.[parent_position] IS NOT NULL THEN 'Pozisyon' END)
						)
			ELSE CTE_5.[Organizasyon Şeması Bağlı olduğu nesne (Türü)] END ) AS 'Organizasyon Yapısı Türü'
		,(CASE 
				WHEN [Nesne Türü] = 'Pozisyon' THEN 
				COALESCE (
						 (select dp1.entity_code  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_legalentity') }}  dp1 where opo.[legal_entity_code] = dp1.entity_code)
						,(select dp2.entity_code  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_businessunit') }} dp2 where opo.[business_unit_code] = dp2.entity_code)
						,(select dp3.entity_code from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_division')}} dp3 where opo.[division_code] = dp3.entity_code)
						,(select dp4.entity_code from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_subdivision')}} dp4 where opo.[sub_division_code] = dp4.entity_code) 
						,(select dp5.entity_code from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_department') }} dp5 where opo.[departmenr_code]= dp5.entity_code)
						,opo.[parent_position] 
						)
				ELSE [Organizasyon Şeması Bağlı olduğu nesne (Kodu)]  end) AS 'Organizasyon Yapısı Kodu'
		,(CASE 
				WHEN [Nesne Türü] = 'Pozisyon' THEN 
				COALESCE (
						 (select dp1.entity_name  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_legalentity') }} dp1 where opo.[legal_entity_code] = dp1.entity_code)
						,(select dp2.entity_name  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_businessunit') }} dp2 where opo.[business_unit_code] = dp2.entity_code)
						,(select dp3.entity_name from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_division')}} dp3 where opo.[division_code] = dp3.entity_code)
						,(select dp4.entity_name from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_subdivision')}} dp4 where opo.[sub_division_code] = dp4.entity_code) 
						,(select dp5.entity_name from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_department') }} dp5 where opo.[departmenr_code]= dp5.entity_code)
						,(select dp6.position_name from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchartobject') }}  dp6 where opo.[parent_position]= dp6.position_code) 
						)
				ELSE [Organizasyon Şeması Bağlı olduğu nesne (Adı)] end) AS 'Organizasyon Yapısı Adı'
		,CTE_5.[yonetici_mi?]
		,e_row
		,(CASE 
			   WHEN	[Organizasyon Şeması Bağlı olduğu nesne (Türü)]= 'Bölüm/Proje/Isletme' THEN  (SELECT sub.gorevdeki_kisi_adi FROM {{ source('stg_hr_kpi','raw__hr_kpi_t_dim_subdivision') }} sub WHERE [Organizasyon Şeması Bağlı olduğu nesne (Kodu)] = sub.entity_code )
			   WHEN	[Organizasyon Şeması Bağlı olduğu nesne (Türü)]= 'Bölge/fonksiyon/BU' THEN  (SELECT div.gorevdeki_kisi_adi FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_division')}} div WHERE [Organizasyon Şeması Bağlı olduğu nesne (Kodu)] = div.entity_code )
			   WHEN	[Organizasyon Şeması Bağlı olduğu nesne (Türü)]= 'Sirket' THEN  (SELECT bus.gorevdeki_kisi_adi FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_businessunit') }} bus WHERE [Organizasyon Şeması Bağlı olduğu nesne (Kodu)] = bus.entity_code )
			   WHEN	[Organizasyon Şeması Bağlı olduğu nesne (Türü)]= 'Grup/Baskanlik' THEN  (SELECT leg.gorevdeki_kisi_adi FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_legalentity') }} leg WHERE [Organizasyon Şeması Bağlı olduğu nesne (Kodu)] = leg.entity_code )
			   WHEN	[Organizasyon Şeması Bağlı olduğu nesne (Türü)]= 'Birim' THEN  (SELECT dep.gorevdeki_kisi_adi FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_department') }} dep WHERE [Organizasyon Şeması Bağlı olduğu nesne (Kodu)] = dep.entity_code )			   WHEN [Nesne Türü] = 'Pozisyon' THEN
						(select dp.incumbent_name from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchartobject') }}  dp where opo.[parent_position] = dp.position_code)
		 END	 ) AS 'Organizasyon Yapısı Yönetici'
FROM CTE_5
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchart') }}  org ON CTE_5.[parent_position_code]= org.pozisyon_kodu	
		LEFT JOIN {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_positionorgchartobject') }} opo ON opo.[position_code] = CTE_5.[pozisyon_kodu]
)
select 
c.*
,(CASE 
	WHEN [Organizasyon Yapısı Türü] != 'Pozisyon' THEN 
		COALESCE (
				 (CASE WHEN [Organizasyon Yapısı Türü] = 'Grup/Baskanlik' THEN (select dp1.head_of_unit_position  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_legalentity') }}  dp1 where [Organizasyon Yapısı Kodu] = dp1.entity_code) END)
				,(CASE WHEN [Organizasyon Yapısı Türü] = 'Sirket'  THEN (select dp1.head_of_unit_position  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_businessunit') }}  dp1 where [Organizasyon Yapısı Kodu]= dp1.entity_code) END)
				,(CASE WHEN [Organizasyon Yapısı Türü] = 'Bölge/fonksiyon/BU' THEN (select dp1.head_of_unit_position  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_division') }}  dp1 where [Organizasyon Yapısı Kodu]  = dp1.entity_code) END)
				,(CASE WHEN [Organizasyon Yapısı Türü] = 'Bölüm/Proje/Isletme'  THEN (select dp1.head_of_unit_position  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_subdivision') }}  dp1 where [Organizasyon Yapısı Kodu] = dp1.entity_code) END)
				,(CASE WHEN [Organizasyon Yapısı Türü] = 'Birim'  THEN (select dp1.head_of_unit_position  from {{ source('stg_hr_kpi', 'raw__hr_kpi_t_dim_department') }}  dp1 where [Organizasyon Yapısı Kodu] = dp1.entity_code) END)
				)
	ELSE [Organizasyon Yapısı Kodu] END ) AS 'head_of_unit_position'
from cte_6 c
where 1=1