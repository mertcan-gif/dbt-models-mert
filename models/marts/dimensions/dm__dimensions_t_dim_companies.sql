{{
  config(
    materialized = 'table',tags = ['fi_kpi','dimensions','companies'],grants = {'select': ['rflow_user']}
    )
}}


SELECT 
		[rls_region] = COALESCE(RegionCode,'NAR') 
		,[rls_group] =
			CONCAT(
				COALESCE(TRIM(KyribaGrup),'')
				,'_'
				,COALESCE(RegionCode,'')
				)
		,[rls_company] =
			CONCAT(
				COALESCE(TRIM([RobiKisaKod]),'')
				,'_'
				,COALESCE(RegionCode,'')
				)	
		,rls_businessarea= CONCAT('_',COALESCE(RegionCode,''))
		  ,[fsp_business_type]
		  ,[fps_segment_group]
		  ,[fsp_segment_subgroup]
		  ,[fsp_geography]
		  ,[kyriba_ust_group]
		  ,[KyribaGrup] = TRIM(KyribaGrup)
		  ,[KyribaKisaKod] = TRIM([KyribaKisaKod])
		  ,[RobiKisaKod]=TRIM([RobiKisaKod])
		  ,[FCKisaKod]
		  ,[Ulke]
		  ,[Tanim]
		  ,[FinalTablosuSiniflandirma]
		  ,[Durum]
		  ,[BusinessArea]
		  ,[CounterParty]
		  ,[TypeLabel]
		  ,[CompanyLabel]
		  ,[GrupOrani]
		  ,[YK_KrediKisiti]
		  ,[ToplamAlinanBasliklar]
		  ,[Siniflama1]
		  ,[Siniflama2]
		  ,[UnionCompanyID]
		  ,[RegionCode]
		  ,[CountryCode]
		  ,IsNWC
		  ,MaliIslerMapping
from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }}
where 1=1
	and [KyribaKisaKod] is not null