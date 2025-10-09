{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}

WITH final_data AS (
	SELECT 
		rls_region =
		CASE
				WHEN realized_gyg.[group] = 'BNGROUP' THEN 'EUR' 
				WHEN realized_gyg.[group] = 'DESNAGROUP' THEN 'RUS' 
				WHEN realized_gyg.[group] = 'ENERJI' THEN 'TUR' 
				WHEN realized_gyg.[group] = 'HKPGROUP' THEN 'EUR' 
				WHEN realized_gyg.[group] = 'HOLDING' THEN 'TUR' 
				WHEN realized_gyg.[group] = 'IVFGROUP' THEN 'SGP' 
				WHEN realized_gyg.[group] = 'RETGROUP' THEN 'TUR' 
				WHEN realized_gyg.[group] = 'RGYGROUP' THEN 'TUR' 
				WHEN realized_gyg.[group] = 'RSYGROUP' THEN 'TUR' 
				WHEN realized_gyg.[group] = 'RTIGROUP' THEN 'TUR' 
			END
		,rls_group =
			CASE
				WHEN realized_gyg.[group] = 'BNGROUP' THEN 'BNGROUP_EUR'
				WHEN realized_gyg.[group] = 'DESNAGROUP' THEN 'DESNAGROUP_RUS' 
				WHEN realized_gyg.[group] = 'ENERJI' THEN 'ENERJI_TUR' 
				WHEN realized_gyg.[group] = 'HKPGROUP' THEN 'HKPGROUP_EUR' 
				WHEN realized_gyg.[group] = 'HOLDING' THEN 'HOLDING_TUR' 
				WHEN realized_gyg.[group] = 'IVFGROUP' THEN 'IVFGROUP_SGP' 
				WHEN realized_gyg.[group] = 'RETGROUP' THEN 'RETGROUP_TUR' 
				WHEN realized_gyg.[group] = 'RGYGROUP' THEN 'RGYGROUP_TUR' 
				WHEN realized_gyg.[group] = 'RSYGROUP' THEN 'RSYGROUP_TUR' 
				WHEN realized_gyg.[group] = 'RTIGROUP' THEN 'RTIGROUP_TUR' 
			END
		,rls_company = ''
		,rls_businessarea = ''
		,realized_gyg.*
	FROM {{ source('stg_sharepoint', 'raw__gyg_kpi_t_dim_realizedgygrevenues') }} realized_gyg

	-- UNION ALL

	-- 	SELECT 
	-- 	rls_region =
	-- 	CASE
	-- 			WHEN realized.[group] = 'BNGROUP' THEN 'EUR' 
	-- 			WHEN realized.[group] = 'DESNAGROUP' THEN 'RUS' 
	-- 			WHEN realized.[group] = 'ENERJI' THEN 'TUR' 
	-- 			WHEN realized.[group] = 'HKPGROUP' THEN 'EUR' 
	-- 			WHEN realized.[group] = 'HOLDING' THEN 'TUR' 
	-- 			WHEN realized.[group] = 'IVFGROUP' THEN 'SGP' 
	-- 			WHEN realized.[group] = 'RETGROUP' THEN 'TUR' 
	-- 			WHEN realized.[group] = 'RGYGROUP' THEN 'TUR' 
	-- 			WHEN realized.[group] = 'RSYGROUP' THEN 'TUR' 
	-- 			WHEN realized.[group] = 'RTIGROUP' THEN 'TUR' 
	-- 		END
	-- 	,rls_group =
	-- 		CASE
	-- 			WHEN realized.[group] = 'BNGROUP' THEN 'BNGROUP_EUR' 
	-- 			WHEN realized.[group] = 'DESNAGROUP' THEN 'DESNAGROUP_RUS' 
	-- 			WHEN realized.[group] = 'ENERJI' THEN 'ENERJI_TUR' 
	-- 			WHEN realized.[group] = 'HKPGROUP' THEN 'HKPGROUP_EUR' 
	-- 			WHEN realized.[group] = 'HOLDING' THEN 'HOLDING_TUR' 
	-- 			WHEN realized.[group] = 'IVFGROUP' THEN 'IVFGROUP_SGP' 
	-- 			WHEN realized.[group] = 'RETGROUP' THEN 'RETGROUP_TUR' 
	-- 			WHEN realized.[group] = 'RGYGROUP' THEN 'RGYGROUP_TUR' 
	-- 			WHEN realized.[group] = 'RSYGROUP' THEN 'RSYGROUP_TUR' 
	-- 			WHEN realized.[group] = 'RTIGROUP' THEN 'RTIGROUP_TUR' 
	-- 		END
	-- 	,rls_company = ''
	-- 	,rls_businessarea = ''
	-- 	,[group]
	-- 	,company
	-- 	,type_of_metric
	-- 	,metric
	-- 	,[year]
	-- 	,eur_value * 0.8
	-- 	,try_value * 0.8
	-- 	,budget_version = 'V2'
	-- FROM {{ source('stg_sharepoint', 'raw__gyg_kpi_t_dim_realizedgygrevenues') }} realized
)
select *
from final_data
