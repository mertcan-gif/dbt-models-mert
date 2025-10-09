{{
  config(
    materialized = 'table',tags = ['rmg_kpi']
    )
}}


WITH main_cte AS (
	SELECT
		CAST(matdoc.cpudt as date) as entry_date,
		matdoc.bukrs AS company,
		matdoc.werks AS business_area,
		matdoc.matnr AS material_code,
		maktx AS material,
		matdoc.meins AS unit,
		CAST(stockqty as float) AS stock_quantity,
		amount = dmbtrstock,
		matdoc.waers AS currency,
		matdoc.bwart AS transaction_type,
		tag = 'tire',
		reference_document = xblnr
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_matdoc') }} matdoc
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }} makt ON matdoc.matnr = substring(makt.matnr, 11, len(makt.matnr))
	WHERE 1=1
	AND matdoc.werks = 'RMGM'
	AND spras = 'T'
	AND matdoc.bwart IN ('101', '102', '261', '262')
	AND matdoc.matnr IN (
		'20006300',
		'20033947',
		'20033948',
		'20060206',
		'30009174',
		'30009175',
		'30009176',
		'30010002',
		'30013556',
		'30013922',
		'30019453',
		'30019454',
		'30019455',
		'30019456',
		'30019457',
		'30019458',
		'30020187',
		'30020188',
		'30020191',
		'30020192',
		'30020193',
		'30020194',
		'30020197',
		'30020198',
		'30020201',
		'30020202',
		'30022588',
		'30023786',
		'30023865',
		'30023866',
		'30023867',
		'30023868',
		'30023869',
		'30023872',
		'80000428',
		'80000429',
		'80000531',
		'80000606',
		'80000650'
	)

	UNION ALL

	SELECT
		CAST(matdoc.cpudt as date) as entry_date,
		matdoc.bukrs AS company,
		matdoc.werks AS business_area,
		matdoc.matnr AS material_code,
		maktx AS material,
		matdoc.meins AS unit,
		CAST(stockqty as float) AS stock_quantity,
		amount = dmbtrstock,
		matdoc.waers AS currency,
		matdoc.bwart AS transaction_type,
		tag = 'fuel',
		reference_document = xblnr
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_matdoc') }} matdoc
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }} makt ON matdoc.matnr = substring(makt.matnr, 11, len(makt.matnr))
	WHERE 1=1
	AND matdoc.werks = 'RMGM'
	AND spras = 'T'
	AND matdoc.bwart IN ('101', '102', '261', '262')
	AND matdoc.matnr IN ('20003376', '30009025', '30009014')

)

SELECT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = CONCAT(main_cte.business_area, '_', rls_region),
	main_cte.*
FROM main_cte
LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON main_cte.company = dim_comp.RobiKisaKod