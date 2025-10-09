{{
  config(
    materialized = 'table',tags = ['hos_kpi']
    )
}}

WITH raw_data AS (
	SELECT 
		BakimPlani AS maintenance_period_code
		,[BoUretimYeri] AS plant
		,tw.NAME1 AS plant_name
		,[TeknikBirim] AS technical_unit
		,[UstEkipmanNo] AS equipment_no
		,[UstEkipmanTanimi] AS equipment_description
		,[EkipmanNo] AS subequipment_no
		,[EkipmanTanimi] AS subequipment_description
		,[IslemMaddesi] AS operation_description
		,RIGHT(IslemMaddesi, 2) AS operation_type
		,[NesneTuru] AS equipment_group
		,[NesneTuruTanimi] AS equipment_group_description
		,CASE
				WHEN LEFT(BakimPeriyodu, 2) = '00' THEN RIGHT(BakimPeriyodu, 1)
				WHEN LEFT(BakimPeriyodu, 1) = '0' THEN RIGHT(BakimPeriyodu, 2)
		ELSE BakimPeriyodu 
		END  AS maintenance_period
		,BakimPeriyoduBirimi AS maintenance_period_type  
		,CASE 
			WHEN PlanlamaTarihi = '' 
			THEN NULL
			ELSE CONVERT(date, PlanlamaTarihi, 104)
		END AS planned_date
		,CASE 
			WHEN [SiparisAcilisTarihi] = '' 
			THEN NULL
			ELSE CONVERT(date, [SiparisAcilisTarihi], 104)
		END AS order_opening_date
		,CASE 
			WHEN [SiparisKapanmaTarihi] = '' 
			THEN NULL
			ELSE CONVERT(date, [SiparisKapanmaTarihi], 104)
		END AS order_closing_date
		,[Yer] AS equipment_type
		,[IsYeri] AS responsible_unit
		,CAST([PlanlananMaliyet] AS float) AS planned_cost
		,[PmParaBirimi] AS currency_planned_cost
		,CAST([GerceklesenMaliyet] AS float) AS realized_cost
		,[GmParaBirimi] AS currency_realized_cost
		,CAST([SatinalmaMaliyeti] AS float) AS purchase_cost
		,CAST([DepoMaliyeti] AS float) AS storage_cost
		,CAST([IscilikMaliyeti] AS float) AS labour_cost
		,CAST([db_upload_timestamp] AS datetime) AS db_upload_timestamp
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_hospitalmaintenance') }} spm
	LEFT JOIN (SELECT 
					[NAME1]
					,[WERKS]
				FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }}
				WHERE WERKS <> '') tw on spm.BoUretimYeri = tw.WERKS
),
final_data as (
	SELECT 
		maintenance_period_code
		,planned_date
		,CASE
			WHEN operation_type = 'CD' THEN N'Cihaz Değişimi'
			WHEN operation_type = 'TM' THEN N'Tamir'
			WHEN operation_type = 'PD' THEN N'Parça Değişimi'
			WHEN operation_type = 'KL' THEN N'Kalibrasyon'
			ELSE NULL
		END AS operation_type
		,operation_type_index = 
			CASE 
				WHEN planned_cost = 0 THEN 3 -- dahil etme
				WHEN operation_type = 'CD' THEN 2 
				ELSE 1 
			END
		,planned_cost
		,realized_cost
		,plant
		,plant_name
		,technical_unit
		,equipment_no
		,equipment_description
		,subequipment_no
		,subequipment_description
		,operation_description
		,equipment_group
		,equipment_group_description
		,maintenance_period
		,maintenance_period_type
		,order_opening_date
		,order_closing_date
		,equipment_type
		,responsible_unit
		,currency_planned_cost
		,currency_realized_cost
		,purchase_cost
		,storage_cost
		,labour_cost
	FROM raw_data

	UNION ALL

	SELECT 
		maintenance_period_code
		,CAST(planned_date AS date) planned_date
		,CASE
			WHEN operation_type = 'CD' THEN N'Cihaz Değişimi'
			WHEN operation_type = 'TM' THEN N'Tamir'
			WHEN operation_type = 'PD' THEN N'Parça Değişimi'
			WHEN operation_type = 'KL' THEN N'Kalibrasyon'
			ELSE NULL
		END AS operation_type
		,operation_type_index = 
			CASE 
				WHEN planned_cost = 0 THEN 3 -- dahil etme
				WHEN operation_type = 'CD' THEN 2 
				ELSE 1 
			END
		,planned_cost
		,realized_cost
		,plant
		,tw.NAME1
		,technical_unit
		,equipment_no
		,equipment_description
		,subequipment_no
		,subequipment_description
		,operation_description = NULL
		,equipment_group
		,equipment_group_description
		,maintenance_period = NULL
		,maintenance_period_type = NULL
		,order_opening_date = NULL
		,order_closing_date = planned_date
		,equipment_type
		,responsible_unit = NULL
		,currency_planned_cost
		,currency_realized_cost
		,purchase_cost = NULL
		,storage_cost = NULL
		,labour_cost = NULL
	FROM {{ source('stg_sharepoint', 'raw__hos_kpi_t_fact_hospitalmaintenanceadjustments') }} spm
	LEFT JOIN (SELECT 
					 [NAME1]
					,[WERKS]
				FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }}
				WHERE WERKS <> '') tw on spm.plant = tw.WERKS
),
project_company_mapping AS (
SELECT
	name1
	,WERKS
	,w.BWKEY
	,bukrs
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} w
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} k ON w.bwkey = k.bwkey
)

/****
	Planlanan tarihe göre:
		- Aynı Yıl içerisinde
		- Bir maintenance_period_code'da eğer 
			- Hem cihaz değişimi var ise 
			- Hem de diğer veriler var ise diğer verileri

 ****/
SELECT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea = CONCAT(m.werks , '_' , c.rls_region)
	,company = c.RobiKisaKod 
	,businessarea = m.werks
	,planned_cost_adjusment_flag = 
		CASE 
			WHEN operation_type = N'Cihaz Değişimi' AND operation_type_adjustment_helper > 1 THEN 0
			ELSE 1
		END,
		outer_query.*
FROM (
	SELECT
		operation_type_adjustment_helper =
			ROW_NUMBER()
			OVER(
				PARTITION BY 
					maintenance_period_code,
					year(planned_date)
				ORDER BY 
					operation_type_index asc
				) ,
		*
	FROM final_data f
) outer_query
LEFT JOIN project_company_mapping m ON outer_query.plant = m.werks
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} c ON c.RobiKisaKod = m.bukrs
