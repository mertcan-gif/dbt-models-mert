{{
  config(
    materialized = 'table',tags = ['fms_kpi']
    )
}}

/*
'vehicles' ve 'vehicle_debits' tablolarından veriyi her 4 saatte bir incremental olarak çekiyoruz.
Bu nedenle aynı gün içinde aynı veriler birden fazla kez yer alabiliyor.
Bu durumu önlemek için, aynı gün içinde tekrar eden verileri ROW_NUMBER() fonksiyonu kullanarak tekilleştirdim.
*/

WITH CompanyUnionMappingTable AS (
	SELECT 
		RobiKisaKod AS company,
		KyribaGrup AS [group],
		RegionCode AS region 
	FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }})

,vehicles AS (
SELECT 
	[rls_region] = (SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)

	,[rls_group] = CONCAT(
							(SELECT TOP 1 [group] FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
							,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT) 
						)
	,[rls_company] = CONCAT(
							BUKRS,'_',
							(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
						)
	,[rls_businessarea] = CONCAT(
								GSBER,'_',
								(SELECT TOP 1 region FROM CompanyUnionMappingTable cpn_unn WHERE cpn_unn.company = BUKRS COLLATE DATABASE_DEFAULT)
						)
	,equipment = EQUNR
	,vehicle_owner_company = ZZITO_ARAC_SAHIPSIRKET
	,company_code = BUKRS
	,business_area = GSBER
	,vehicle_model = TYPBZ
	,model_year = BAUJJ
	,supply_type = ZZITO_ARAC_TEDARIK_TIPI
	,usage_type = ZZITO_ARAC_KULLTIPI
	,supplier = CASE 
				WHEN ZZITO_ARAC_TEDARIIKCI = 'ÖZMAL' THEN ZZITO_ARAC_TEDARIIKCI
				ELSE lfa.name1
			END
	,vehicle_contract_starting_date = CAST(ZZITO_ARAC_SOZ_BAS_TAR AS DATE) 
	,vehicle_contract_ending_date = CAST(ZZITO_ARAC_SOZ_BIT_TAR AS DATE)
	,vehicle_status = ZZITO_ARAC_AKTIF_PASIF
	,driver_id = ZZITO_ARAC_SURUCU
	,driver_assignment_date = CAST(ZZITO_ARAC_SRCATM_TAR AS DATE)
	,leasing_cost_expense = ZZITO_ARAC_NUM_KRLM_UCRETI
	,traffic_policy_number = ZZITO_POLNOTRF
	,traffic_insurance_cost = ZZITO_PRMBDTRF
	,traffic_start_date = CAST(ZZITO_BEGDATRF AS DATE)
	,traffic_end_date = CAST(ZZITO_ENDDATRF AS DATE)
	,kasko_policy_number = ZZITO_POLNOKSK
	,kasko_insurance_cost = ZZITO_PRMBDKSK
	,kasko_start_date = CAST(ZZITO_BEGDAKSK AS DATE)
	,kasko_end_date = CAST(ZZITO_ENDDAKSK AS DATE)
	,return_date = CAST(ZZITO_ARAC_IADE_TAR AS DATE)
	,sales_type = ZZITO_ARAC_SATISTIPI
	,sales_date = CAST(ZZITO_ARAC_SATISTAR AS DATE)
	,sales_amount = ZZITO_ARAC_NUM_SATBEDEL
	,contract_start_date_income = CAST(ZZITO_ARAC_SOZLESME_BS_GLR AS DATE)
	,contract_end_date_income = CAST(ZZITO_ARAC_SOZLESME_BT_GLR AS DATE)
	,leasing_cost_income = ZZITO_ARAC_NUM_KRLU_GLR
	,report_description = ZZITO_ARAC_RAPORLAMA_TXT
	,inspection_expiry_date = CAST(ZZITO_ARAC_MUAY_GEC_TAR AS DATE)
	,validity_start = CAST(DATAB AS DATE)
	,reporting_date = CAST(vhc.db_upload_timestamp AS DATE)
	,ROW_NUMBER() OVER (PARTITION BY vhc.EQUNR, CAST(vhc.db_upload_timestamp AS DATE) order by vhc.db_upload_timestamp desc) rn
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicles') }} vhc
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa on REPLACE(vhc.ZZITO_ARAC_TEDARIIKCI, ' ', '') = lfa.lifnr
)

,vehicle_debits AS (
	SELECT 
		PERNR
		,INVNR
		,full_name
		,db_upload_timestamp
		,ROW_NUMBER() OVER (PARTITION BY INVNR, CAST(db_upload_timestamp AS DATE) ORDER BY db_upload_timestamp DESC) rn_debits
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_vehicledebitsinfo') }} r
	LEFT JOIN (
				SELECT DISTINCT 
					full_name
					,sap_id
					FROM {{ ref('dm__hr_kpi_t_dim_hrall') }}
				) hr on hr.sap_id = r.PERNR
)

SELECT 
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,v.equipment
	,v.vehicle_owner_company
	,v.company_code
	,v.business_area
	,v.vehicle_model
	,v.model_year
	,v.supply_type
	,-- 06.03.2025 tarihinde aynı harf başka bir anlama gelme durumu oluşturuğu için reporting_date koşullara eklenmiştir.
	CASE 
		WHEN v.usage_type = 'K' THEN N'Kurum'
		WHEN v.usage_type = 'A' THEN N'Aile'
		WHEN v.reporting_date < '2024-03-06' AND v.usage_type = 'S' THEN N'Tahsis'
		WHEN v.reporting_date >= '2024-03-06' AND v.usage_type = 'S' THEN N'Satılacak'
		WHEN v.reporting_date >= '2024-03-06' AND v.usage_type = 'T' THEN N'Tahsis'
		WHEN v.usage_type = 'R' THEN N'Koruma Aracı'
		WHEN v.reporting_date < '2024-03-06' AND v.usage_type = 'G' THEN N'Genel Kullanım - Operasyon'
		WHEN v.reporting_date >= '2024-03-06' AND v.usage_type = 'G' THEN N'Geçici Tahsis'
		WHEN v.reporting_date >= '2024-03-06' AND v.usage_type = 'O' THEN N'Genel Kullanım - Operasyon'
		WHEN v.usage_type = 'D' THEN N'Genel Kullanım - Dış Seyahatler'
		WHEN v.usage_type = 'M' THEN N'Genel Kullanım - Makam'
		WHEN v.usage_type = 'J' THEN N'Satılacak'
		WHEN v.usage_type = 'F' THEN N'Transfer'
		WHEN v.usage_type = 'P' THEN N'Aile-Operasyon'
		ELSE v.usage_type
	END AS usage_type
	,v.supplier
	,v.vehicle_contract_starting_date
	,v.vehicle_contract_ending_date
	,v.vehicle_status
	,v.driver_id
	,v.driver_assignment_date
	,v.leasing_cost_expense
	,v.traffic_policy_number
	,v.traffic_insurance_cost
	,v.traffic_start_date
	,v.traffic_end_date
	,v.kasko_policy_number
	,v.kasko_insurance_cost
	,v.kasko_start_date
	,v.kasko_end_date
	,v.return_date
	,v.sales_type
	,v.sales_date
	,v.sales_amount
	,v.contract_start_date_income 
	,v.contract_end_date_income 
	,v.leasing_cost_income 
	,v.report_description 
	,v.inspection_expiry_date 
	,v.validity_start 
	,v.reporting_date
FROM vehicles v
LEFT JOIN (SELECT * FROM vehicle_debits
			WHERE rn_debits = 1) vd on v.equipment = vd.INVNR
									AND v.reporting_date = CAST(vd.db_upload_timestamp AS DATE)
WHERE 1=1
	and rn = 1






