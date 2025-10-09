{{
  config(
    materialized = 'table',tags = ['superstructure_kpi']
    )
}}	


WITH bu_cte AS (

	SELECT 
		business_unit = CASE
							WHEN LEN(functional_description) - LEN('Ust Yapı Business Unit') IS NULL 
								OR LEN(functional_description) - LEN('Ust Yapı Business Unit') < 0 THEN NULL
							ELSE CONCAT('BU-',RIGHT(functional_description, LEN(functional_description) - LEN('Ust Yapı Business Unit')))
						END,
		business_area = LEFT(rls_businessarea,LEN(rls_businessarea)-4)
	FROM {{ ref('vw__rls_v_dim_profileentitymapping') }}
	where 1=1
		and rls_profile like '%USTYA%'
		and rls_profile <> 'DWH_LS_BA_RECUSTYAPI'
		and rls_businessarea NOT LIKE '%0000%'
		and rls_businessarea NOT IN ('R006_TUR','R033_TUR','R036_TUR','R037_TUR','R007_TUR','R032_TUR','R043_TUR','R052_TUR')
		and rls_businessarea IN (
						'R065_TUR',
						'R055_TUR',
						'R054_TUR',
						'R010_TUR',
						'R001_TUR',
						'R005_TUR',
						'R040_TUR',
						'R067_TUR',
						'R057_TUR',
						'R058_TUR',
						'R061_TUR',
						'R062_TUR',
						'R068_TUR',
						'R070_TUR',
						'R071_TUR'
						)
),	

poc_invoiced_rev_y AS 
(
	SELECT 
		cr.business_area,
		posting_date = EOMONTH(posting_date),
		-- amount_in_eur = SUM(amount_in_eur * -1)
		amount_in_eur = SUM(amount_in_eur * -1),
		document_currency
	FROM {{ ref('dm__nwc_kpi_t_fact_costrealization') }} cr
		RIGHT JOIN {{ source('stg_sharepoint', 'raw__superstructure_kpi_t_dim_businessunits') }} bu ON bu.business_area = cr.business_area
		-- LEFT JOIN bu_cte ON bu_cte.business_area = cr.business_area
	WHERE 1=1
		-- AND business_unit IS NOT NULL
		AND cr.company = 'REC'
		AND cr.[type] IN (N'GELİR', N'GELIR')
		AND (cr.[fiscal_period] NOT IN ('13','14','15','16') OR cr.[fiscal_period] IS NULL)
	GROUP BY 
		cr.business_area,
		EOMONTH(posting_date),
		document_currency
),


DATE_TABLE as
(
	SELECT 
		EOMONTH(DATEADD(MONTH, number, '2023-01-01')) AS end_of_month
	FROM 
		master..spt_values
	WHERE 
		type = 'P' 
		AND DATEADD(MONTH, number, '2023-01-01') <= GETDATE()
),

DIMENSION_WITH_ALL_DATES AS (
	SELECT * 
	FROM (SELECT DISTINCT business_area, document_currency FROM poc_invoiced_rev_y) RAW_D
		CROSS JOIN DATE_TABLE
),

TOTAL_AMOUNTS_BEFORE_CUMULATIVE AS (
	SELECT
		dt.business_area
		,posting_date = dt.end_of_month
		,budat_year = LEFT(dt.end_of_month,4)
		,dt.document_currency
		,total_amount_eur = SUM(COALESCE(amount_in_eur,0))
	FROM DIMENSION_WITH_ALL_DATES dt
		LEFT JOIN poc_invoiced_rev_y tg ON dt.end_of_month = tg.posting_date
									AND dt.business_area = tg.business_area
									AND dt.document_currency = tg.document_currency
	  
	GROUP BY
		dt.business_area
		,dt.end_of_month
		,dt.document_currency

)

,CUMULATIVE_TOTALS AS (
	SELECT
		business_area
		,posting_date
		,document_currency
		,total_amount_eur
		,SUM(total_amount_eur) over (partition by business_area,budat_year, document_currency order by posting_date) as cumulative_total_eur
	FROM TOTAL_AMOUNTS_BEFORE_CUMULATIVE dt
)

	SELECT
	business_area
	,[type] = 'Ciro (Faturalı) - YTD' ,budat_eomonth = posting_date
	,document_currency
	,cumulative_total_eur
	FROM CUMULATIVE_TOTALS t
	

	

