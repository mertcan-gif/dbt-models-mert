{{
  config(
    materialized = 'view',tags = ['fc_kpi']
    )
}}
SELECT
	ref.id
	,tablo = 
		CASE 
			WHEN LEN(ref.id) = 1 THEN CONCAT('ct_co000',ref.id)
			WHEN LEN(ref.id) = 2 THEN CONCAT('ct_co00',ref.id)
			WHEN LEN(ref.id) = 3 THEN CONCAT('ct_co0',ref.id)
			ELSE CONCAT('ct_co',ref.id)
		END
	,year = (1900 + (ref.updper & 536608768) / 262144) 
	,month = (ref.updper & 253952) / 8192
	,ph.name
	,ref.scope AS scope_code
	,sc.name AS scope
	,vrt.name AS variant
	,crn.name AS currency
FROM [FC].[FCPRODNEW].[dbo].ct_coref ref
	LEFT JOIN [FC].[FCPRODNEW].[dbo].ct_curncy crn ON ref.curncy = crn.id
	LEFT JOIN [FC].[FCPRODNEW].[dbo].ct_variant vrt ON ref.variant = vrt.id
	LEFT JOIN [FC].[FCPRODNEW].[dbo].ct_scope_code sc ON ref.scope = sc.id
	LEFT JOIN [FC].[FCPRODNEW].[dbo].ct_phase ph ON ref.phase = ph.id
WHERE 1=1 
	AND
	(
		(sc.[name] = 'HOL-NEW' AND crn.[name] = 'TRY')

	)
	AND variant = 1