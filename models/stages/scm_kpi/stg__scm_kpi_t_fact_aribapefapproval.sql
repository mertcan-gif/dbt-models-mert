
{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}


WITH tdf_onay as (
select
	s.first_pr_id as 'Talep Numarası',
	s.id as 'tdf_id',
	s.pr_name as 'Talep Konusu',
	s.company as 'Şirket',
	s.billing_address as 'Proje',
	s.tdf_approval_group as 'surec',
	s.previous_proces_date,
	k.process_date as 'TDF Oluşturulma Tarihi',
	k.process_date as 'TDF Onaya Sunma Tarihi',
	s.process_date as 'TDF Onay Tarihi',
	s.[user] as 'Satınalma Personeli',
	s.tdf_process_id,
	NULL AS 'TDF Satınalma Onay Verme Tarihi',
	NULL AS 'TDF Yapım Onay Verme Tarihi',
	NULL AS 'TDF Finans Onay Verme Tarihi'
from {{ ref('stg__scm_kpi_t_fact_processingtimes') }} s
LEFT JOIN 
		(SELECT id,process_date
		 FROM {{ ref('stg__scm_kpi_t_fact_processingtimes') }}
		 WHERE tdf_approval_group = 'TDF Yaratilma') k on k.[id] = s.id
LEFT JOIN 
		(SELECT id,process_date
		 FROM {{ ref('stg__scm_kpi_t_fact_processingtimes') }}
		 WHERE tdf_approval_group = 'TDF Onaya Sunulma') y on y.[id] = s.id
where 1=1
	  and process_category = 'TDF'
	  --and first_pr_id = 'PR3093'
	)



SELECT
	[Talep Numarası],
	tdf_id as 'tdf_id',
	[Talep Konusu],
	[Şirket],
	[Proje],
	surec,
	[TDF Oluşturulma Tarihi],
	[TDF Onaya Sunma Tarihi],
	[Satınalma Personeli],
	[tdf_process_id],
	previous_proces_date,
	[TDF Satınalma Onay Verme Tarihi] = [TDF Onay Tarihi],
	(CAST(DATEDIFF(HOUR, previous_proces_date,  [TDF Onay Tarihi]) AS FLOAT) / 24) as 'TDF Satınalama Onay İşlem Süresi (Onaycı)',
	[TDF Yapım Onay Verme Tarihi],
	NULL AS 'TDF Yapım Onay İşlem Süresi (Onaycı)',
	[TDF Finans Onay Verme Tarihi],
	NULL AS 'TDF Finans Onay İşlem Süresi (Onaycı)'
FROM tdf_onay
where 1=1 
	 -- and tdf_approval_group != 'TDF Yaratilma'
	  --and tdf_approval_group !='TDF Onaya Sunulma'
	  and (tdf_process_id = '0')
	--  and [Talep Numarası] = 'PR1690'
UNION ALL
	SELECT
	[Talep Numarası],
	tdf_id as 'tdf_id',
	[Talep Konusu],
	[Şirket],
	[Proje],
	surec,
	[TDF Oluşturulma Tarihi],
	[TDF Onaya Sunma Tarihi],
	[Satınalma Personeli],
	[tdf_process_id],
	previous_proces_date,
	[TDF Satınalma Onay Verme Tarihi] = [TDF Onay Tarihi],
	(CAST(DATEDIFF(HOUR, previous_proces_date,  [TDF Onay Tarihi]) AS FLOAT)  / 24)  as 'TDF Satınalama Onay İşlem Süresi (Onaycı)',
	[TDF Yapım Onay Verme Tarihi],
	NULL AS 'TDF Yapım Onay İşlem Süresi (Onaycı)',
	[TDF Finans Onay Verme Tarihi],
	NULL AS 'TDF Finans Onay İşlem Süresi (Onaycı)'
	FROM tdf_onay
	where 1=1 
		  and surec != 'TDF Yaratilma'
		  and surec !='TDF Onaya Sunulma'
		  and (tdf_process_id = '1')
		--	  and [Talep Numarası] = 'PR1690'
UNION ALL
	SELECT
	[Talep Numarası],
	tdf_id as 'tdf_id',
	[Talep Konusu],
	[Şirket],
	[Proje],
	surec,
	[TDF Oluşturulma Tarihi],
	[TDF Onaya Sunma Tarihi],
	[Satınalma Personeli],
	[tdf_process_id],
	previous_proces_date,
	[TDF Satınalma Onay Verme Tarihi],
	NULL AS 'TDF Satınalama Onay İşlem Süresi (Onaycı)',
	[TDF Yapım Onay Verme Tarihi]= [TDF Onay Tarihi],
		(CAST(DATEDIFF(HOUR, previous_proces_date,  [TDF Onay Tarihi]) AS FLOAT)  / 24) as 'TDF Yapım Onay İşlem Süresi (Onaycı)',
	[TDF Finans Onay Verme Tarihi],
	NULL AS 'TDF Finans Onay İşlem Süresi (Onaycı)'
	FROM tdf_onay
	where 1=1 
		  and surec != 'TDF Yaratilma'
		  and surec !='TDF Onaya Sunulma'
		  and (tdf_process_id = '2')
	--	  and [Talep Numarası] = 'PR1690'
UNION ALL
	SELECT
	[Talep Numarası],
	tdf_id as 'tdf_id',
	[Talep Konusu],
	[Şirket],
	[Proje],
	surec,
	[TDF Oluşturulma Tarihi],
	[TDF Onaya Sunma Tarihi],
	[Satınalma Personeli],
	[tdf_process_id],
	previous_proces_date,
	[TDF Satınalma Onay Verme Tarihi] ,
	NULL AS 'TDF Satınalama Onay İşlem Süresi (Onaycı)',
	[TDF Yapım Onay Verme Tarihi],
	NULL AS 'TDF Yapım Onay İşlem Süresi (Onaycı)',
	[TDF Finans Onay Verme Tarihi]= [TDF Onay Tarihi],
	(CAST(DATEDIFF(HOUR, previous_proces_date,  [TDF Onay Tarihi]) AS FLOAT)  / 24)  as 'TDF Finans Onay İşlem Süresi (Onaycı)'
	FROM tdf_onay
	where 1=1 
		  and surec != 'TDF Yaratilma'
		  and surec !='TDF Onaya Sunulma'
		  and (tdf_process_id = '3')
	--	  and [Talep Numarası] = 'PR1690'
