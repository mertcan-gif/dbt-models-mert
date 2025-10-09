{{
  config(
    materialized = 'table',tags = ['hr_kpi_puantaj']
    )
}}
WITH project_company_mapping AS (
  SELECT
    name1
    ,WERKS
    ,w.BWKEY
    ,bukrs
  FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
  )

SELECT
	c.rls_region
	,c.rls_group
	,c.rls_company
	,rls_businessarea = CONCAT(p.werks , '_' , c.rls_region)
	,[Persno] AS [sap_id]
	,[Adsoyad] AS [full_name]
	,CONVERT(DATE, Baslg0, 103) AS [start_date]
	,CONVERT(DATE, Bitis0, 103) AS [end_date]
	,[Islmdizisi] AS [transaction_distribution]
	,[Pozisyon] AS [position]
	,[Statu] AS [statu]
	,[Massg] AS [reason_for_termination_code]
	,[Mgtxt] AS [leaving_reason]
	,CASE
		WHEN Statu = 'Mavi Yaka - Direkt' THEN 'Direct'
		WHEN Statu = 'Mavi Yaka - Endirekt' THEN 'Indirect'
		ELSE NULL
	END AS [direct_indirect]
	,CASE
		WHEN Statu = 'Mavi Yaka - Direkt' THEN 'Mavi Yaka'
		WHEN Statu = 'Mavi Yaka - Endirekt' THEN 'Mavi Yaka'
		WHEN Statu = 'Beyaz Yaka' Then 'Beyaz Yaka'
		ELSE NULL
	END AS [blue_white_collar]
	,t001w.name1 AS [project]
	,CASE
	WHEN Kidemyili = '00.00.0000' THEN NULL
	ELSE CONVERT(DATE, REPLACE(Kidemyili, '.', '/'),104)
	END AS year_of_seniority
	,[Cinsiyet] AS [gender]
	,[Uretimsinifi] AS [production_class]
	,CASE
		WHEN ISDATE([Yas]) = 1 THEN
			DATEDIFF(YEAR, CONVERT(DATE, [Yas], 104), GETDATE()) -
			CASE
				WHEN MONTH(CONVERT(DATE, [Yas], 104)) > MONTH(GETDATE()) OR
					(MONTH(CONVERT(DATE, [Yas], 104)) = MONTH(GETDATE()) AND DAY(CONVERT(DATE, [Yas], 104)) > DAY(GETDATE()))
				THEN 1
				ELSE 0
			END
		ELSE NULL
	END AS [age]
	,CASE
	WHEN Istencikis = '00.00.0000' THEN NULL
	ELSE CONVERT(DATE, REPLACE(Istencikis, '.', '/'),104)
	END AS leaving_work
	,[Uyruk] AS [nationality]
	,[Ekipkodu] AS [team_code]
	,[Ekipbazli] AS [team_based]
	,sp.[Ulke] AS [country]
	,[Ulasim] AS [transportation]
	,[Konaklama] AS [accommodation]
	,[Egitim] AS [education]
	,[Firmasinifi] AS [company_class]
	,[Anadisiplin] AS [main_discipline]
	,[Altyuklenici] AS [sub_subcontractor]
	,[Alttaseron] AS [subcontractor]
	,[Calisangrubu] AS [employee_group]
	,sp.[Btrtl] AS [personnel_subfield]
	,[Gorevturu] AS [task_type]
	,[Lokasyon] AS [location]
	,skala AS scale
	,skala_text as scale_description
	,p.werks as business_area
	,sp.db_upload_timestamp
	,c.KyribaGrup as [group]
	,c.RobiKisaKod as company
	,t001w.name1 as name
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zhr_000_t_dwhlog') }} sp
INNER JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zps_010_t_uypaat') }} uy ON sp.Btrtl = uy.btrtl
LEFT JOIN project_company_mapping p ON p.werks = uy.werks
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} c ON c.RobiKisaKod = p.bukrs
LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON t001w.werks = p.werks
WHERE Calisangrubu = N'Taşeron'