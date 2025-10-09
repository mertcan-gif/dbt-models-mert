
{{
  config(
    materialized = 'view',tags = ['enrg_kpi']
    )
}}	

WITH S4_HOURLY_PRODUCTION AS 
	(
/** Saatlik Üretim Tablosu Tablosu **/
	SELECT 	
		URETIM_YERI AS businessarea_code,
		tw.NAME1 AS businessarea_name, 
	   CAST(CONCAT(TARIH,' ',BASLANGIC_SAATI) AS DATETIME) AS datetime_by_hour,
		GUP as gup,
		REVIZE_GUP AS revised_gup
	from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zco_002_t_rm_ent') }} prd
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} tw ON prd.URETIM_YERI = tw.WERKS collate database_default
) 
,SANTRAL_SAATLIK_TOPLAM_URETIM AS
(
		SELECT 
			werks = WERKS
			,datetime_by_hour =DATEADD(
                        HOUR, -1,
                        CASE
                            WHEN CPUTM = '24:00:00' THEN
                                DATEADD(DAY, 1, CONVERT(DATETIME, BUDAT + ' ' + '00:00:00', 121))
                            ELSE
                                CONVERT(DATETIME, BUDAT + ' ' + CPUTM, 121)
                        END
                    ) /* Başlangıç saatine göre alınacağı için bir saat geriye aldım */
			,hourly_production_mwh = SUM(CAST(ZZMWH AS FLOAT))-(LAG(SUM(CAST(ZZMWH AS FLOAT))) OVER (ORDER BY WERKS,BUDAT,CPUTM))
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zco_004_t_eus') }}
		WHERE 1=1
			AND ZZSYC IN ('HAT1','HAT2')
			AND WERKS <> 'E002'
		GROUP BY 
			WERKS,BUDAT,CPUTM

		UNION ALL

/** 
	İş birimi sadece E002 tesisinde santral üretiminin UNITE sayacından alınacağını söyledi. 
	O sebeple E002 tesisine özel ayrı bir query aşağıda yazılmıştır.
**/
		SELECT 
			werks = WERKS,
			datetime_by_hour = DATEADD(
                        HOUR, -1,
                        CASE
                            WHEN CPUTM = '24:00:00' THEN
                                DATEADD(DAY, 1, CONVERT(DATETIME, BUDAT + ' ' + '00:00:00', 121))
                            ELSE
                                CONVERT(DATETIME, BUDAT + ' ' + CPUTM, 121)
                        END
                    ),
			hourly_production_mwh = SUM(CAST(ZZMWH AS FLOAT))
		FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zco_004_t_eus') }}
		WHERE 1 = 1
			AND WERKS = 'E002' 
			AND ZZSYC LIKE '%UNITE%'
		GROUP BY WERKS, BUDAT, CPUTM

	)
, final_table_currency_not_adjusted AS
(
SELECT
						rls_region = pp.region
						,rls_group = CONCAT(pp.[group],'_',pp.region)
						,rls_company = CONCAT(pp.company,'_',pp.region)
						,rls_businessarea =  CONCAT(s4_prod.businessarea_code,'_','TUR')
						,company= pp.company
						,pp.revenue_calculation_type
						,s4_prod.businessarea_code
						,s4_prod.businessarea_name
						,s4_prod.datetime_by_hour
						--,planned_production_daily = gup
/* GÜP VE GÖP*/			,planned_production_daily_revised = revised_gup
/*SANTRAL TOPLAM*/		,realized_production = eus.hourly_production_mwh --SANTRAL TOPLAM
						,yek_revenue_usd = 
							CASE 
								WHEN pp.[revenue_calculation_type] = 'yek' and YEAR(s4_prod.datetime_by_hour) != '2024' THEN yek.[renewable_energy_resource]*eus.hourly_production_mwh
								WHEN YEAR(s4_prod.datetime_by_hour) = '2024' THEN 0								
								ELSE 0
							END -- dolar
						,auf_revenue_try = 
							CASE 
								WHEN auf.gop_maximum_price IS NULL and YEAR(s4_prod.datetime_by_hour) != '2024'
									THEN ptf.price*eus.hourly_production_mwh
								WHEN pp.[revenue_calculation_type] = 'auf' AND ptf.price > COALESCE(auf.gop_maximum_price,0) and YEAR(s4_prod.datetime_by_hour) != '2024'
									THEN auf.[gop_maximum_price]*eus.hourly_production_mwh
								WHEN pp.[revenue_calculation_type] = 'auf' AND ptf.price <= COALESCE(auf.gop_maximum_price,0) and YEAR(s4_prod.datetime_by_hour) != '2024'
									THEN ptf.price*eus.hourly_production_mwh
								WHEN YEAR(s4_prod.datetime_by_hour) = '2024' then ptf.price*eus.hourly_production_mwh
								ELSE 0
							END	 -- tl dolar ile çarpılacak
						,1/cr.usd_value  as 'usd_to_try'
						,1/cr.eur_value as 'eur_to_try'
--/* return */	yek_revenue + auf_revenue -- yek ile auf'un toplamı
FROM S4_HOURLY_PRODUCTION s4_prod
	LEFT JOIN {{ source('stg_enrg_kpi', 'raw__enrg_kpi_t_fact_renewableenergyresource') }} yek ON year(s4_prod.datetime_by_hour) = yek.[year]
	LEFT JOIN  {{ source('stg_enrg_kpi', 'raw__enrg_kpi_t_fact_marketclearingprice') }} ptf ON s4_prod.datetime_by_hour = ptf.date
	LEFT JOIN  {{ source('stg_enrg_kpi', 'raw__enrg_kpi_t_fact_maximumsettlementprice') }} auf ON FORMAT(s4_prod.datetime_by_hour, 'yyyy-MM') = FORMAT(auf.start_date, 'yyyy-MM')
	LEFT JOIN SANTRAL_SAATLIK_TOPLAM_URETIM eus ON eus.datetime_by_hour=s4_prod.datetime_by_hour AND eus.werks = s4_prod.businessarea_code
	LEFT JOIN {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }} pp ON s4_prod.businessarea_code = pp.werks collate database_default
	LEFT JOIN (
		SELECT * FROM {{ ref('stg__dimensions_t_dim_dailys4currencies') }} WHERE currency = 'TRY'
	) cr ON  CONVERT(DATE,s4_prod.datetime_by_hour) = cr.date_value
)


SELECT distinct
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,company
	,revenue_calculation_type
	,businessarea_code
	,businessarea_name
	,datetime_by_hour
	,planned_production_daily_revised
	,realized_production
	,yek_revenue_usd
	,auf_revenue_try
	,revenue_try = COALESCE(yek_revenue_usd,0)*usd_to_try + COALESCE(auf_revenue_try,0)
	,eur_to_try
	,usd_to_try
FROM final_table_currency_not_adjusted
WHERE YEAR(datetime_by_hour)>2022