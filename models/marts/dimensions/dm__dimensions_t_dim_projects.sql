{{
  config(
    materialized = 'table',tags = ['nwc_kpi','dimensions','project_dimension']
    )
}}

/* 2025-01-16 Adem Numan Kaya: RLS tablo yapisi degistirildi. RLS columnlari bos olursa sistem patlar diye main queryinin altina filtre eklendi
	dim_projects exceline eklenen yeni projelerin yansimasi icin region, group, company ya da business area columnlarinin concat edilmis halinin
	bos olmamasi gerekli. RLS key bundan sonra bizim tarafimizda olusturulacak. Tablonun bir onceki versionuna to_kpi_t_dim_projectsten ulasilabilir.
  */

with rls_unfiltered as (
select 
    [region] as rls_region
    ,concat(coalesce([group],''),'_',coalesce([region],'')) as rls_group
    ,concat(coalesce([company],''),'_',coalesce([region],'')) as rls_company
    ,concat(coalesce([business_area],''),'_',coalesce([region],'')) as rls_businessarea
    ,[project_id]
    ,[business_area]
    ,[risk_portal_project_id]
    ,[res_project_id]
    ,[rsafe_id]
    ,[project_name]
    ,[project_shortname]
    ,[group]
    ,[subgroup]
    ,[company]
    ,[contractor]
    ,[status]
    ,[country]
    ,[city]
    ,[latitude]
    ,[longitude]
    ,[latitude_secondary]
    ,[longitude_secondary]
    ,[sector]
    ,[currency]
    ,[contract_type]
    ,[is_only_rmore]
    ,[region]
    ,[db_upload_timestamp]
from {{ source('stg_dimensions', 'raw__dwh_t_dim_project') }}
  )

	select 
		rls_key=CONCAT(rls_businessarea, '-', rls_company, '-', rls_group),
		* 
	from rls_unfiltered
	where 1=1
		and rls_region is not null 
		and rls_company is not null
		and rls_businessarea is not null 
		and rls_group is not null 