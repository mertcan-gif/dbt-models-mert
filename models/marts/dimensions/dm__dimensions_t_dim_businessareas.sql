{{
  config(
    materialized = 'table',tags = ['rmore','dimensions']
    )
}}

--id kolonunun distinct ve dolu olduğundan emin ol, bunun için dbtnin id diye bir standardı vardır bakılabilir
select 
	[rls_region] = COALESCE(RegionCode,'NAR') 
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
		CONCAT(
			COALESCE(TRIM(werks),'')
			,'_'
			,COALESCE(RegionCode,'')
			)	
	,werks as business_area_code
	,UPPER(t001w.name1) business_area_tr_name
	,t001k.bukrs company
	,cmp.KyribaGrup as [group]
	,RegionCode as region
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w
	left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001k') }} t001k on t001k.bwkey = t001w.werks
	left join {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} cmp on t001k.bukrs = cmp.RobiKisaKod
where 1=1
	and werks is not null --id check
	and t001w.spras = 'T'