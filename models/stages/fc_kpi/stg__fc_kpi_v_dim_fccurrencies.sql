{{
  config(
    materialized = 'view',tags = ['fc_kpi']
    )
}}

SELECT DISTINCT 
	CONCAT((1900 + (ct_period & 536608768) / 262144),'-',((ct_period & 253952) / 8192)) AS year_month,
	ct_exrate_type.name ext_rate,
	ct_exrate
FROM [FC].[FCPRODNEW].[dbo].ct_exrate cxt
		LEFT JOIN [FC].[FCPRODNEW].[dbo].ct_exrate_type ON  ct_exrate_type.id = cxt.ct_type
WHERE ct_currency = 52
	AND (name = 'AR' OR name = 'CR')
	AND ct_entity = 0