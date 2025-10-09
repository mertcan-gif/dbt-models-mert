
{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}

WITH final_data AS (
	SELECT 
		dim_comp.rls_region
		,dim_comp.rls_group
		,dim_comp.rls_company
		,dim_comp.rls_businessarea
		,dim_comp.KyribaGrup as [group]
		,dim_comp.RobiKisaKod as company
		,fcc.*
	FROM {{ ref('stg__gyg_kpi_t_fact_realizedrevenue') }} fcc
		LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp 
			ON  dim_comp.FCKisaKod = fcc.fc_company
		WHERE dim_comp.kyriba_ust_group = N'RÖNESANS'
)
select *
from final_data