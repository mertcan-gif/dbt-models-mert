{{
  config(
    materialized = 'view',tags = ['rls']
    )
}}

SELECT 
	   [type] = [type]
      ,[rls_region] = COALESCE([rls_region],'')
      ,[rls_profile] = [rls_profile]
      ,[rls_group] = IIF(rls_type = 'group',CONCAT(rls_value,'_',rls_region),'')
      ,[rls_company] = IIF(rls_type = 'company',CONCAT(rls_value,'_',rls_region),'')
      ,[rls_businessarea] = IIF(rls_type = 'businessarea',CONCAT(rls_value,'_',rls_region),'')
      ,[functional_description] = [functional_description]
      ,[group_description] = IIF(rls_type = 'group',rls_description,'')
      ,[company_description] = IIF(rls_type = 'company',rls_description,'')
      ,[busines_area_description]  = IIF(rls_type = 'businessarea',rls_description,'')
FROM {{ source('stg_rls', 'raw__rls_t_dim_profileentitymapping') }}
WHERE [rls_profile] IN 
	(
		SELECT rls_profile
		FROM {{ source('stg_rls', 'raw__rls_t_dim_functionalmatrixhol') }}
	)


UNION ALL


/** 
	Bir tabloya test datası aşağıdaki rls değerleri ile girildiğinde;

		rls_region : NAN
		rls_group : TO_0000_NAN
		rls_company : TO_0000_NAN
		rls_businessarea : TO_0000_NAN

	aşağıda bulunan unionlar ile tüm yetki profillerine bu değerler tanım olarak eklenmiştir.

	İlk union'da "CASE WHEN" kısmında;
		Grup bazlı profillere : rls_group TO_0000_NAN tanımlanmıştır 
		Şirket bazlı profillere : rls_company TO_0000_NAN tanımlanmıştır
		İş alanı bazlı profillere : rls_businessarea TO_0000_NAN tanımlanmıştır

	İkinci union'da ise Region bazlı L0 profillere : rls_region NAN tanımlanmıştır

**/
{# SELECT DISTINCT
	[type] = 'LS DUMMY'
	,rls_region = 'NAN'
	,rls_profile
	,CASE WHEN rls_profile like 'DWH_L1_%' OR rls_profile like 'DWH_LS_GR_%' THEN 'TO_0000_NAN' ELSE '' END
	,CASE WHEN rls_profile like 'DWH_L2_%' OR rls_profile like 'DWH_LS_CO_%' THEN 'TO_0000_NAN' ELSE '' END
	,CASE WHEN rls_profile like 'DWH_L3_%' OR rls_profile like 'DWH_LS_BA_%' THEN 'TO_0000_NAN' ELSE '' END
	,functional_description = 'DUMMY'
	,''
	,''
	,''
from {{ source('stg_rls', 'raw__rls_t_dim_profileentitymapping') }}
WHERE 1=1
	AND rls_profile NOT LIKE 'DWH_L0_%'
	AND [rls_profile] IN 
	(
		SELECT rls_profile
		FROM {{ source('stg_rls', 'raw__rls_t_dim_functionalmatrixhol') }}
	)

UNION ALL #}

SELECT DISTINCT
	[type] = 'LS DUMMY'
	,rls_region = 'NAN'
	,rls_profile
	,''
	,''
	,''
	,functional_description = 'DUMMY'
	,''
	,''
	,''
from {{ source('stg_rls', 'raw__rls_t_dim_profileentitymapping') }}
WHERE 1=1
	AND rls_profile LIKE 'DWH_L0_%'
	AND rls_profile <> 'DWH_L0_GLOBAL'
	AND [rls_profile] IN 
	(
		SELECT rls_profile
		FROM {{ source('stg_rls', 'raw__rls_t_dim_functionalmatrixhol') }}
	)

UNION ALL

/** 
	Aşağıda bulunan unionlar ile tüm yetki profillerine bu değerler tanım olarak eklenmiştir.

	İlk union'da "CASE WHEN" kısmında;
		Grup bazlı profillere : rls_group GR_0000_NAN tanımlanmıştır 
		Şirket bazlı profillere : rls_company CO_0000_NAN tanımlanmıştır
		İş alanı bazlı profillere : rls_businessarea BA_0000_NAN tanımlanmıştır

	İkinci union'da ise Region bazlı L0 profillere : rls_region NAN tanımlanmıştır

**/
SELECT DISTINCT
	[type] = 'LS NONRLS'
	,rls_region = 'NAN'
	,rls_profile
	,CASE WHEN rls_profile like 'DWH_L1_%' OR rls_profile like 'DWH_LS_GR_%' THEN 'GR_0000_NAN' ELSE '' END
	,CASE WHEN rls_profile like 'DWH_L2_%' OR rls_profile like 'DWH_LS_CO_%' THEN 'CO_0000_NAN' ELSE '' END
	,CASE WHEN rls_profile like 'DWH_L3_%' OR rls_profile like 'DWH_LS_BA_%' THEN 'BA_0000_NAN' ELSE '' END
	,functional_description = 'NONRLS'
	,''
	,''
	,''
from {{ source('stg_rls', 'raw__rls_t_dim_profileentitymapping') }}
WHERE 1=1
	AND rls_profile NOT LIKE 'DWH_L0_%'
	AND [rls_profile] IN 
	(
		SELECT rls_profile
		FROM {{ source('stg_rls', 'raw__rls_t_dim_functionalmatrixhol') }}
	)

UNION ALL

SELECT DISTINCT
	[type] = 'LS NONRLS'
	,rls_region = 'NAN'
	,rls_profile
	,''
	,''
	,''
	,functional_description = 'NONRLS'
	,''
	,''
	,''
from {{ source('stg_rls', 'raw__rls_t_dim_profileentitymapping') }}
WHERE 1=1
	AND rls_profile LIKE 'DWH_L0_%'
	AND rls_profile <> 'DWH_L0_GLOBAL'
	AND [rls_profile] IN 
	(
		SELECT rls_profile
		FROM {{ source('stg_rls', 'raw__rls_t_dim_functionalmatrixhol') }}
	)