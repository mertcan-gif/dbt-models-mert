{{
  config(
    materialized = 'table',tags = ['rmore','dimensions']
    )
}}

select 
	[rls_region] = RegionCode 
	,[rls_group] =
		CONCAT(
			COALESCE(TRIM(KyribaGrup),'')
			,'_'
			,COALESCE(RegionCode,'')
			)
	,[rls_company] =
		CONCAT(
			COALESCE(TRIM([KyribaKisaKod]),'')
			,'_'
			,COALESCE(RegionCode,'')
			)
	,[rls_businessarea] =
		CONCAT('_'
			,COALESCE(RegionCode,'')
			)	
	,t001.bukrs company
	,UPPER(t001.butxt) company_tr_name
	,cmp.KyribaGrup as [group]
	,RegionCode as region
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001') }} t001
	left join {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cmp on t001.bukrs = cmp.RobiKisaKod
where 1=1
	and t001.bukrs is not null --id check
	and t001.spras = 'TR'