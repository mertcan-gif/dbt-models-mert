{{
  config(
    materialized = 'table',tags = ['hr_kpi', 'levels']
    )
}}

WITH union_data as (
SELECT 
	[code]
	,[name_tr]
	,[name_en]
	,sf_system = 'Coach'
FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_level_a') }}
	UNION
SELECT
	[code]
	,[name_tr]
	,[name_en]
	,sf_system = 'Rpeople'
FROM  {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_level_a') }}
)

,subq AS (
	SELECT 
		*
		,ROW_NUMBER() OVER (PARTITION BY code ORDER BY sf_system DESC) rn
	FROM union_data
)

SELECT 
	*
FROM subq
WHERE rn = 1
