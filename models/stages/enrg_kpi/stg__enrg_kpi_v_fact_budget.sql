{{
  config(
    materialized = 'view',tags = ['enrg_kpi']
    )
}}	
WITH Currency AS(
SELECT 
		YEAR(CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104)) AS [year],
		MONTH(CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104)) AS [month],
		avg(CASE WHEN FCURR = 'EUR' THEN  cast(UKURS as float) ELSE null END) AS [eur_to_try],
		avg(CASE WHEN FCURR = 'USD'  THEN  cast(UKURS as float) ELSE null END) AS [usd_to_try]
FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurr') }} AS T 
WHERE 1=1
	AND (TCURR = 'TRY') 
	AND (KURST = 'BT') 
	--AND UKURS < 0
	AND (RIGHT(gdatu, 4) > 2021)
	GROUP BY
	YEAR(CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104)),
	MONTH(CONVERT(DATE, LTRIM(RTRIM(gdatu)), 104))  
	)
,RAW_BUDGETS AS (
	SELECT  
		fmbl.fiscyear as fiscal_year, -- BURASI ESKIDEN FISCYEAR idi. Ancak FISCYEAR kolonu kaldırıldıgı için doc
		fmfcrt.MCTXT as financial_center_description,
		fmbl.FUNDSCTR as financial_center_code,
		fmbl.DOCNR as document_number,
		CMMTITEM as commitment_item_code,
		fmcit.BEZEI as commitment_item_definition,
		ba_fc_mapping.werks as business_area,
		[1]=fmbl.TVAL01,
		[2]=fmbl.TVAL02,
		[3]=fmbl.TVAL03,
		[4]=fmbl.TVAL04,
		[5]=fmbl.TVAL05,
		[6]=fmbl.TVAL06,
		[7]=fmbl.TVAL07,
		[8]=fmbl.TVAL08,
		[9]=fmbl.TVAL09,
		[10]=fmbl.TVAL10,
		[11]=fmbl.TVAL11,
		[12]=fmbl.TVAL12
		--,fmi.TRBTR 
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmbl') }} fmbl
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmbh') }} fmbh ON fmbl.DOCNR = fmbh.DOCNR 
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmfctrt') }} fmfcrt ON fmfcrt.FICTR = fmbl.FUNDSCTR
		LEFT JOIN {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }} ba_fc_mapping ON fmbl.FUNDSCTR=ba_fc_mapping.funding_center collate database_default		
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_fmcit') }}  fmcit ON fmbl.CMMTITEM = fmcit.FIPEX collate database_default
	WHERE 1=1 
		AND [VERSION] = '0'
		AND FUNDSCTR IN (
			SELECT funding_center collate database_default FROM {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }})
)

 

,UnpivottedData AS (
	SELECT business_area, financial_center_description, financial_center_code, fiscal_year, commitment_item_code,commitment_item_definition, [Month]
	, [Value] =  SUM(case when RIGHT([Value],1) = '-' THEN cast(left([Value], LEN([Value]) - 1) as float)*(-1) else cast([Value] as float) end )
	FROM 
	(
	  SELECT business_area, financial_center_description, financial_center_code, fiscal_year, commitment_item_code,commitment_item_definition, [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12]
	  FROM RAW_BUDGETS
	) AS SourceTable
	UNPIVOT
	(
	  [Value] FOR [Month] IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])
	) AS UnpivotTable
	    GROUP BY
        business_area,
        financial_center_description,
        financial_center_code,
        fiscal_year,
        commitment_item_code,
        commitment_item_definition,
        [Month]
)
,dimension_businessarea_commitment_item_mapping
AS (
	SELECT 
		year_int,
		month_int,
		business_area,
		commitment_item_code
	FROM (
			SELECT 	DISTINCT
				business_area,
				commitment_item_code
			FROM UnpivottedData WHERE business_area IS NOT NULL
			UNION 
			SELECT DISTINCT
				RBUSA collate database_default,
				FIPEX collate database_default
			FROM  {{ ref('stg__s4hana_t_sap_acdoca') }}
			WHERE 1=1
				AND RBUKRS collate database_default IN ( SELECT company from {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }}) 
				AND RBUSA IS NOT NULL 
				AND RBUSA <> ''
		) commitments
	LEFT JOIN (
		SELECT DISTINCT year_int, month_int
		FROM {{ source('stg_dimensions', 'raw__dwh_t_dim_dates') }}
		WHERE 1=1
		 AND year_int >= 2022
		 AND year_int <= 2024
		 ) date_dim ON 1=1
)
SELECT
    COALESCE(plants.region,'NAR') AS rls_region,
    CONCAT(plants.[group], '_', COALESCE(plants.region,'NAR')) AS rls_group,
    CONCAT(plants.company, '_', COALESCE(plants.region,'NAR')) AS rls_company,
    CONCAT(dims.business_area, '_', COALESCE(plants.region,'NAR')) AS 'rls_business_area',
	CONCAT(dims.business_area, '_', COALESCE(plants.region,'NAR')) AS 'rls_businessarea',
    dims.business_area,
    plants.company,
    UnpivottedData.financial_center_description,
    UnpivottedData.financial_center_code,
    dims.year_int as 'fiscal_year',
    dims.month_int AS 'month',
    dims.commitment_item_code,
    UnpivottedData.commitment_item_definition AS commitment_item_definition_tr,
    cmitems.[commitment_item_definition_en] AS commitment_item_definition,
    cmitems.[commitment_item_category] AS commitment_item_category,
	UnpivottedData.[Value]*(-1)*curr.eur_to_try AS 'budget_tl',
	UnpivottedData.[Value]*(-1)*curr.eur_to_try/curr.usd_to_try AS 'budget_usd', ---- budget eur'idi. budget
    UnpivottedData.[Value]*(-1) AS 'budget_eur',
	COALESCE(fmi.try_value, 0)*(-1) AS 'realized_budget_tl',
	COALESCE(fmi.usd_value, 0)*(-1) AS 'realized_budget_usd',
	COALESCE(fmi.eur_value, 0)*(-1) AS 'realized_budget_eur',
	cmitems.commitment_item_core as commitment_item_definition_related,
	cmitems.commitment_item_category_core as commitment_item_category_related
FROM dimension_businessarea_commitment_item_mapping dims
	LEFT JOIN UnpivottedData ON 
				UnpivottedData.business_area = dims.business_area 
				AND UnpivottedData.commitment_item_code = dims.commitment_item_code
				AND UnpivottedData.fiscal_year = dims.year_int
				AND UnpivottedData.Month = dims.month_int
	LEFT JOIN {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }}  AS plants ON dims.business_area = plants.werks
	LEFT JOIN {{ source('stg_enrg_kpi', 'raw__enrg_kpi_t_dim_commitmentitems') }} AS cmitems ON dims.commitment_item_code = cmitems.[commitment_item_code]
	LEFT JOIN Currency AS curr ON UnpivottedData.fiscal_year = curr.[year] and UnpivottedData.[Month] = curr.[month]
	LEFT JOIN (
		 SELECT
			year_ = YEAR(CAST(hbldat AS DATE)),
			month_ = MONTH(CAST(hbldat AS DATE)),
			bus_area = gsber,
			company = bukrs,
			commitment_item_code = fipos,
			try_value = sum(IIF(shkzg = 'S',cast(dmbtr AS float),cast(dmbtr AS float)*-1)),
			eur_value = sum(IIF(shkzg = 'S',cast(dmbe2 AS float),cast(dmbe2 AS float)*-1)), 
			usd_value = sum(IIF(shkzg = 'S',cast(dmbe3 AS float),cast(dmbe3 AS float)*-1))
	FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_energy_budget') }}
		WHERE 1=1	
			AND bukrs collate database_default IN ( SELECT company from {{ source('stg_dimensions', 'raw__enrg_kpi_t_dim_powerplants') }})
		GROUP BY
				gsber, bukrs,fipos, YEAR(CAST(hbldat AS DATE)),MONTH(CAST(hbldat AS DATE))
	) fmi ON 
		dims.business_area = fmi.bus_area COLLATE database_default 
		AND dims.commitment_item_code = fmi.commitment_item_code COLLATE database_default
		AND dims.year_int = fmi.year_ 
		AND dims.month_int = fmi.month_
	WHERE 1=1 
			AND plants.region IS NOT NULL
			 -- and dims.business_area = 'E017'
			 --and dims.commitment_item_code = 'YEN012500'
