{{
  config(
    materialized = 'table',tags = ['nwc_kpi','rlsdimensions']
    )
}}


WITH RLS_SAP AS (
	SELECT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,kyriba_group
		,rbukrs
		,business_area_code
		,business_area
		,ROW_NUMBER() OVER(PARTITION BY rls_region,rls_group,rls_company,rls_businessarea ORDER BY LEN(business_area) DESC, LOWER(business_area) DESC) RN
		FROM {{ ref('stg__nwc_kpi_t_dim_rlsdimensionssap') }}
),

UNIONIZED_DATA AS (
  SELECT 
      [rls_region]
        ,[rls_group]
        ,[rls_company]
        ,[rls_businessarea]
        ,[group_code] = kyriba_group
        ,[company_code] = rbukrs
        ,[businessarea_code] = business_area_code
        ,[project_name] = UPPER(business_area)
  FROM RLS_SAP
  WHERE RN = 1

  UNION

  SELECT 	  
    [rls_region] 
      ,[rls_group] 
      ,[rls_company]
      ,[rls_businessarea] 
      ,[kyriba_group]
      ,[company]
      ,[business_area]
      ,[business_area_description]
  FROM {{ ref('stg__nwc_kpi_t_dim_rlsdimensionsnonsap') }}
  WHERE RN = 1
)


/** Bir iş alanı farklı martlarda farklı isimlerle kullanılabiliyor, aşağıda bu kısmı tekilleştirmek adına proje dimensionu oluşturulmuştur.
Order by kısmındaki koşul, NULL değerlerin Row Number'ı 1 gelirken asıl proje isimlerinin 2,3 vb. gelmesini engellemek içindir **/
,PROJECTNAME_DIM AS (
	SELECT DISTINCT [businessarea_code],[project_name],ROW_NUMBER() OVER(PARTITION BY businessarea_code ORDER BY IIF(project_name IS NOT NULL, 1, 2) ) rn FROM UNIONIZED_DATA
)

SELECT 
	u.[rls_region]
	,u.[rls_group]
	,u.[rls_company]
	,u.[rls_businessarea]
	,u.[group_code]
	,u.[company_code]
	,u.[businessarea_code]
	,p.[project_name]
FROM UNIONIZED_DATA u
	LEFT JOIN PROJECTNAME_DIM p ON p.businessarea_code = u.businessarea_code
WHERE 
	p.rn = 1 OR p.rn IS NULL
