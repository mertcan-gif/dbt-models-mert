{{
  config(
    materialized = 'table',tags = ['nwc_kpi','rlsdimensions']
    )
}}

WITH AP_NONSAP AS (

SELECT DISTINCT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,kyriba_group = KyribaGrup
	,company = RBUKRS
	,business_area = RBUSA
	,business_area_description = PROJECTNAME
FROM {{ ref('dm__nwc_kpi_t_fact_accountspayablewithcleareditems') }} WHERE [SOURCE] <> 'SAP'

)

,AR_NONSAP AS (

SELECT DISTINCT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,KyribaGrup
	,RBUKRS
	,RBUSA
	,PROJECTNAME
FROM {{ ref('dm__nwc_kpi_t_fact_accountsreceivablewithcleareditems') }} WHERE [SOURCE] <> 'SAP'
and RBUSA IS NOT NULL

)

,MONTHLY_NONSAP AS (

SELECT DISTINCT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,KyribaGrup
	,RBUKRS
	,business_area_code
	,business_area
FROM {{ ref('dm__nwc_kpi_t_fact_monthlyreport') }} 
WHERE 1=1
	AND business_area_code NOT IN (SELECT DISTINCT rbusa FROM {{ ref('stg__s4hana_t_sap_acdoca') }})
	AND business_area_code IS NOT NULL 
	AND business_area_code <> 'NULL' --RET için elle NULL yazılmış excelde
)

,PROJECTSTATUS_NONSAP AS (

SELECT DISTINCT
	rls_region
	,rls_group
	,rls_company
	,rls_businessarea
	,KyribaGrup
	,company
	,business_area_code
	,projects
FROM {{ ref('dm__nwc_kpi_t_fact_projectstatus') }} 
WHERE 1=1
	AND KyribaGrup <> 'RTIGROUP' -- RTIGROUP'tan gelen iş alanı kodları SAP kodlarına göre geliyor. Kalanı manuel oluşturuldu.
)

,STOCKAGING_NONSAP AS (

SELECT DISTINCT
	rls_region = CASE WHEN company IN ('TKM','ARN') THEN 'EUR' ELSE kuc.RegionCode END
	,rls_group = CASE 
					WHEN company IN ('TKM','ARN') THEN 'RETGROUP_EUR' 
					ELSE CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
			      END
	,rls_company = CASE
						WHEN company IN ('TKM','ARN') THEN CONCAT(COALESCE(company ,''),'_EUR','')
						ELSE CONCAT(COALESCE(company,''),'_'	,COALESCE(kuc.RegionCode,''),'')
					 END
	,rls_businessarea = CONCAT(COALESCE(s.business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,kuc.KyribaGrup
	,s.company
	,s.business_area
	,s.business_area_description
FROM {{ ref('stg__nwc_kpi_t_fact_stockagingexternal') }}  s
	LEFT JOIN {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} kuc ON s.company = kuc.RobiKisaKod
WHERE 1=1
	AND kuc.RobiKisaKod IS NOT NULL -- RobiKyribaUnionCompany'de olmayan şirketlerin filtrelenmesi
)

,NONSAP_UNION AS (

	SELECT * FROM AP_NONSAP
		UNION
	SELECT * FROM AR_NONSAP
		UNION
	SELECT * FROM MONTHLY_NONSAP
		UNION
	SELECT * FROM PROJECTSTATUS_NONSAP
		UNION
	SELECT * FROM STOCKAGING_NONSAP
)


SELECT 
	*
	,ROW_NUMBER() OVER(PARTITION BY company,business_area ORDER BY LEN(business_area_description) DESC) RN
FROM NONSAP_UNION