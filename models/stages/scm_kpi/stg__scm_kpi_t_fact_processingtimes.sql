{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}
WITH main_data AS (
    SELECT DISTINCT 
	   [InitialUniqueName]
      ,[Name]
      ,[StatusString]
      ,[ApprovedState]
      ,[unique_name_version]
      ,[UniqueName]
      ,[ERPRequisitionID]
      ,[CreateDate]
      ,[SubmitDate]
      ,[ApprovedDate]
      ,[PurchaseOrg_UniqueName]
      ,[Preparer_UniqueName]
      ,[Requester_UniqueName]
      ,[CostCenter.ProcurementUnit]
      ,[CostCenter.UniqueName]
      ,[CostCenter.CostCenterDescription]
	  ,[BillingAddress_UniqueName]
    FROM {{ source('stg_scm_kpi', 'raw__scm_kpi_t_fact_aribaprocurementrequestsnew') }}
	--where [InitialUniqueName] = 'PR10585'
),

cost_center_grouped_main as (
SELECT 
	   [InitialUniqueName]
      ,[Name]
      ,[StatusString]
      ,[ApprovedState]
      ,[UniqueName]
      ,[ERPRequisitionID]
      ,[CreateDate]
      ,[SubmitDate]
      ,[ApprovedDate]
      ,[PurchaseOrg_UniqueName]
      ,[Preparer_UniqueName]
    ,STRING_AGG([CostCenter.CostCenterDescription], ', ') AS cost_center_description
    ,STRING_AGG([CostCenter.UniqueName], ', ') AS cost_center_code
	,STRING_AGG([unique_name_version], ', ') AS unique_name_version
	,STRING_AGG([BillingAddress_UniqueName],', ') AS billing_address
FROM 
    main_data
GROUP BY 
	  [InitialUniqueName]
      ,[Name]
      ,[StatusString]
      ,[ApprovedState]
      ,[UniqueName]
      ,[ERPRequisitionID]
      ,[CreateDate]
      ,[SubmitDate]
      ,[ApprovedDate]
      ,[PurchaseOrg_UniqueName]
      ,[Preparer_UniqueName]
),
all_data as (
SELECT
	jb.InitialUniqueName as ID,
	jb.InitialUniqueName as birinci_pr_id,
--	tdf_header.created_pr_id as ikinci_pr_id,
	NULL AS ikinci_pr_id,
	'Talep' AS ust_kategori,
	jb.[Preparer_UniqueName] as kullanıcı,
	jb2.kullanıcı_talep_onaya_sunulma_tarih  as previous_process_date,
	jb2.kullanıcı_talep_onaya_sunulma_tarih  as process_date,
	'Talep Yaratılma' as asama,
	NULL as tdf_process_id,
	NULL as doc_id
FROM cost_center_grouped_main jb
	--LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tdf_header  on tdf_header.talep_sap_id = jb.InitialUniqueName
	LEFT JOIN 

	(
		SELECT
	jb.meta_InitialUniqueName as ID,

	MIN(DATEADD(HOUR, 3, CAST(jb.record_ActivationDate as datetime))) AS kullanıcı_talep_onaya_sunulma_tarih,
	'Talep Onay Tarihleri' as surec
	FROM {{ source('stg_scm_kpi', 'raw__scm_kpi_t_fact_aribaprocurementrequestsjobresult2') }} jb
		where 1=1
			--and meta_InitialUniqueName =  'PR3093'
			and jb.record_Date is not null
	GROUP BY jb.meta_InitialUniqueName
	) jb2 on jb2.ID = jb.InitialUniqueName

WHERE 1=1
	--and jb.InitialUniqueName = 'PR2691'

UNION ALL


SELECT
	t.srid as ID,
	jb.InitialUniqueName,
	NULL as ikinci_pr_id,
	--tdf_header.created_pr_id as ikinci_pr_id,
	'İhale Hazırlık Süreci (SR)' AS ust_kategori,
	'' as creator,
	NULL as previous_process_date,
	CAST(CONCAT(t.crdat,' ',t.crzet ) as datetime) as process_date,
	'SR Yaratılma Tarihi' as surec,
	NULL as tdf_process_id,
	NULL as doc_id
FROM cost_center_grouped_main  jb
	--LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tdf_header on tdf_header.talep_sap_id = jb.InitialUniqueName
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_509') }} t on t.requisitionnumber = jb.InitialUniqueName
	
WHERE 1=1
	--and concat(t.crdat,' ',t.crzet) NOT IN ('0000-00-00 00:00:00')
	--and InitialUniqueName = 'PR2691'
	and t.srid is not null
	and t.srid != ''
	and t.status != '04'
	and t.srid not in ( SELECT srid FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_602') }} )


--UNION ALL
 
--SELECT
--	jb.InitialUniqueName as ID,
--	jb.InitialUniqueName,
--	--tdf_header.created_pr_id as ikinci_pr_id,
--	NULL AS ikinci_pr_id,
--	'Talep' AS ust_kategori,
--	jb.[Preparer_UniqueName] as creator,
--	DATEADD(HOUR, 3, CAST(jb.SubmitDate as datetime)) as previous_process_date,
--	DATEADD(HOUR, 3, CAST(jb.SubmitDate as datetime)) as process_date,
--	'Talep Onaya Sunulma' as surec,
--	NULL as tdf_process_id
--FROM cost_center_grouped_main jb
--	--LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tdf_header on tdf_header.talep_sap_id = jb.InitialUniqueName
--WHERE 1=1
--	and jb.SubmitDate is not null
--	--and InitialUniqueName = 'PR2691'
 
UNION ALL
 
SELECT
	jb.meta_InitialUniqueName as ID,
	jb.meta_InitialUniqueName as talep,
	--tdf_header.created_pr_id as ikinci_pr_id,
	NULL AS ikinci_pr_id,
	'Talep' AS ust_kategori,
	jb.[record_User.UniqueName] as kullanıcı,
	DATEADD(HOUR, 3, CAST(jb.record_ActivationDate as datetime)) AS kullanıcı_talep_onaya_sunulma_tarih,
	DATEADD(HOUR, 3, CAST(jb.record_Date as datetime)) AS kullanıcı_talep_onay_tarihi,
	'Talep Onay Tarihleri' as surec,
	NULL as tdf_process_id,
	NULL as doc_id
FROM {{ source('stg_scm_kpi', 'raw__scm_kpi_t_fact_aribaprocurementrequestsjobresult2') }} jb
	--LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tdf_header on tdf_header.talep_sap_id = jb.meta_InitialUniqueName
	where 1=1
		--and meta_InitialUniqueName = 'PR2691'
		and jb.record_Date is not null
UNION ALL
 
SELECT
	zt.docid as ID,
	requisitionnumber,
	--tdf_header.created_pr_id,
	NULL AS ikinci_pr_id,
	'İhale' AS ust_kategori,
	zt.owner_unique,
	cast(NULL AS datetime) as tarih,
	DATEADD(HOUR, 3, cast(CONCAT(zt.crdat,' ',zt.crzet) as datetime)) as doc_id_yaratılma_tarih,
	'İhale Yaratılma Tarihi' as asama,
	NULL as tdf_process_id,
	zt.docid as doc_id
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_509t') }} zarb 
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_511T') }} zt ON  zt.srid =zarb.srid
	--LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tdf_header on tdf_header.talep_sap_id= zarb.requisitionnumber

WHERE 1=1
		and zt.docid is not null
		and zt.docid != ''
		AND zt.docid  not in 
						(SELECT docid FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_603') }}  )
		and zarb.status != '04'
		and zarb.srid not in ( SELECT srid FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_602') }} )
		and zt.srid != ''
		and zt.status != '04'
	  --AND zarb.requisitionnumber = 'PR2691'

union all

select
	zt.docid as ID,
	requisitionnumber,
	NULL as ikinci_pr_id,
	'İhale' AS ust_kategori,
	zt.owner_unique AS ihale_yaratan,
	CAST(NULL AS datetime) as tarih,
	MIN(CAST(CONCAT(tf.created_date,' ',tf.created_time) AS datetime)) as ihale_bitis_yani_tdf_yaratılma,
	'İhale Bitiş Tarihi' as asama, -- Burasyı tdf yaratılma tarihi ile aynı yaptım -- tdf yaratılması ihale bitişi demek oldugu için.
	NULL as tdf_process_id,
	zt.docid as doc_id
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_509') }} zarb 
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_511') }} zt ON  zt.srid =zarb.srid
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }}  tf on tf.talep_sap_id = zarb.requisitionnumber
where 1=1
	    and zt.docid is not null
		and zt.docid != ''
		and zarb.status != '04'
		and zarb.srid not in ( SELECT srid FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_602') }} )
		AND zt.docid not in 
						(SELECT docid FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_603') }}  )
		and zt.status != '04'
GROUP BY 
	zt.docid,
	requisitionnumber,
	zt.owner_unique

union all 
select
	tf.tdf_id as ID,
	tf.talep_sap_id,
	tf.created_pr_id,
	'TDF' AS ust_kategori,
	tf.created_by,
	a.ihale_bitis_yani_tdf_yaratılma as tarih, --simdilik ihale bitisi girdim. ihale bitis = tdf_yaratılma
	TRY_CAST(CONCAT(tf.created_date,' ',tf.created_time) AS datetime) as process_time,
	'TDF Yaratılma' as asama,
	NULL as tdf_process_id,
	tf.doc_id as doc_id
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }}  tf 	
	left join 
	 			(
						select distinct
							zt.docid,
							MIN(CAST(CONCAT(tf.created_date,' ',tf.created_time) AS datetime)) as ihale_bitis_yani_tdf_yaratılma

						FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_509') }} zarb 
							LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_511') }} zt ON  zt.srid =zarb.srid 
							LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }}  tf on tf.talep_sap_id = zarb.requisitionnumber
						where 1=1
						 	and zt.docid is not null
							AND zt.docid  not in (SELECT docid FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_603') }}  )
							and zt.status != '04'
							and zarb.status != '04'
							and zarb.srid not in ( SELECT srid FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_602') }} )
							and CAST(CONCAT(tf.created_date,' ',tf.created_time) AS datetime) != '1900-01-01 00:00:00.000'
							GROUP BY 
								zt.docid
							) a on a.docid = tf.doc_id
	where 1=1 
			and tf.tdf_id is not null

union all
 
select
	tf.tdf_id as ID,
	tf.talep_sap_id,
	tf.created_pr_id,
	'TDF' AS ust_kategori,
	mail,
	TRY_CAST(CONCAT(tf.created_date,' ',tf.created_time) AS datetime) as tarih,
	TRY_CAST(CONCAT(l.process_date,' ',l.process_time) AS datetime) as process_time,
	'TDF Onaya Sunulma' as asama,
	process_id as tdf_process_id,
	tf.doc_id as doc_id
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_wf_log') }}  l 
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }}  tf ON tf.tdf_id = l.tdf_id
where 1=1

and l.approve_status = 'A'
and l.process_id = '0'
 
union all
 
select
	tf.tdf_id as ID,
	tf.talep_sap_id,
	tf.created_pr_id,
	'TDF' AS ust_kategori,
	l.mail,
	COALESCE(LAG(cast(CONCAT(l.process_date,' ',l.process_time) as datetime)) OVER (PARTITION BY l.tdf_id,l.process_id ORDER BY cast(CONCAT(l.process_date,' ',l.process_time) as datetime) ASC),min_onay_tarih) as tarih,
	cast(CONCAT(l.process_date,' ',l.process_time) as datetime) as process_time,
	'TDF Onay' as asama,
	l.process_id as tdf_process_id,
	tf.doc_id as doc_id
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_wf_log') }}  l
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tf ON tf.tdf_id = l.tdf_id
	LEFT JOIN
	(SELECT
	 MIN(cast(CONCAT(process_date,' ',process_time) as datetime)) AS min_onay_tarih,
	 tdf_id
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_wf_log') }}
	WHERE 1=1
		AND process_id = '0'
	GROUP BY  tdf_id ) m on m.tdf_id = l.tdf_id
where 1=1

and l.approve_status = 'A' --sadece onaylanmışlar gelir
and l.process_id is not null --burada process ıd yazmayan yada process_idsi 3 olanlar ne oluyo.
and l.process_id != ''
and l.process_id = '1'

union all
 
select
	tf.tdf_id as ID,
	tf.talep_sap_id,
	tf.created_pr_id,
	'TDF' AS ust_kategori,
	l.mail,
	coalesce(LAG(cast(CONCAT(l.process_date,' ',l.process_time) as datetime)) OVER (PARTITION BY l.tdf_id,l.process_id ORDER BY cast(CONCAT(l.process_date,' ',l.process_time) as datetime) ASC),min_onay_tarih) as tarih,
	cast(CONCAT(l.process_date,' ',l.process_time) as datetime) as process_time,
	'TDF Onay' as asama,
	l.process_id  as tdf_process_id,
	tf.doc_id as doc_id
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_wf_log') }}  l
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tf ON tf.tdf_id = l.tdf_id
	LEFT JOIN
	(SELECT
	 MIN(cast(CONCAT(process_date,' ',process_time) as datetime)) AS min_onay_tarih,
	 tdf_id
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_wf_log') }}
	WHERE 1=1
		AND process_id = '0'
	GROUP BY  tdf_id ) m on m.tdf_id = l.tdf_id
where 1=1

and l.approve_status = 'A' --sadece onaylanmışlar gelir
and l.process_id is not null --burada process ıd yazmayan yada process_idsi 3 olanlar ne oluyo.
and l.process_id != ''
and l.process_id = '2'

union all
 
select
	tf.tdf_id as ID,
	tf.talep_sap_id,
	tf.created_pr_id,
	'TDF' AS ust_kategori,
	l.mail,
	coalesce(LAG(cast(CONCAT(l.process_date,' ',l.process_time) as datetime)) OVER (PARTITION BY l.tdf_id,l.process_id ORDER BY cast(CONCAT(l.process_date,' ',l.process_time) as datetime) ASC),min_onay_tarih) as tarih,
	cast(CONCAT(l.process_date,' ',l.process_time) as datetime) as process_time,
	'TDF Onay' as asama,
	l.process_id  as tdf_process_id,
	tf.doc_id as doc_id
from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_wf_log') }}  l
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tf ON tf.tdf_id = l.tdf_id
	LEFT JOIN
	(SELECT
	 MIN(cast(CONCAT(process_date,' ',process_time) as datetime)) AS min_onay_tarih,
	 tdf_id
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_wf_log') }}
	WHERE 1=1
		AND process_id = '0'
	GROUP BY  tdf_id ) m on m.tdf_id = l.tdf_id
where 1=1

and l.approve_status = 'A' --sadece onaylanmışlar gelir
and l.process_id is not null --burada process ıd yazmayan yada process_idsi 3 olanlar ne oluyo.
and l.process_id != ''
and l.process_id = '3'

union all
 
SELECT
	jb.InitialUniqueName as ID,
	tdf_header.talep_sap_id ,
	tdf_header.created_pr_id ,
	'Sipariş' AS ust_kategori,
	jb.[Preparer_UniqueName] as kullanıcı,
	cast(NULL as datetime) as previous_process_date,
	DATEADD(HOUR, 3, CAST(jb.CreateDate AS DATETIME)) as process_date,
	'İkinci Talep Yaratılma' as surec,
	NULL as tdf_process_id,
	tdf_header.doc_id as doc_id
FROM cost_center_grouped_main jb
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tdf_header on tdf_header.created_pr_id= jb.InitialUniqueName

WHERE 1=1
	and jb.InitialUniqueName IN (SELECT created_pr_id FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }}  )
 
UNION ALL
 
SELECT
	tdf_header.created_pr_id as ID,
	tdf_header.talep_sap_id ,
	tdf_header.created_pr_id,
	'Sipariş' AS ust_kategori,
	jb.[Preparer_UniqueName] as creator,
	DATEADD(HOUR, 3, CAST(jb.CreateDate as datetime)) as previous_process_date,
	DATEADD(HOUR, 3, CAST(jb.SubmitDate as datetime)) as process_date,
	'İkinci Talep Onaya Sunulma' as surec,
	NULL as tdf_process_id,
	tdf_header.doc_id as doc_id
FROM cost_center_grouped_main jb
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }}  tdf_header on tdf_header.created_pr_id= jb.InitialUniqueName

WHERE 1=1
	and jb.InitialUniqueName IN (SELECT created_pr_id FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }}  )
	and jb.SubmitDate is not null
 
UNION ALL
 
SELECT
	tdf_header.created_pr_id as ID,
	tdf_header.talep_sap_id ,
	tdf_header.created_pr_id,
	'Sipariş' AS ust_kategori,
	jb.[record_User.UniqueName] as kullanıcı,
	DATEADD(HOUR, 3, CAST(jb.record_ActivationDate as datetime)) AS kullanıcı_talep_onaya_sunulma_tarih,
	DATEADD(HOUR, 3, CAST(jb.record_Date as datetime)) AS kullanıcı_talep_onay_tarihi,
	'İkinci Talep Onay Tarihleri' as surec,
	NULL as tdf_process_id,
	tdf_header.doc_id as doc_id
FROM {{ source('stg_scm_kpi', 'raw__scm_kpi_t_fact_aribaprocurementrequestsjobresult2') }} jb
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tdf_header on tdf_header.created_pr_id= jb.meta_InitialUniqueName
--where jb.meta_InitialUniqueName = 'PR3804' 
where 1=1
	and jb.record_Date is not null
UNION ALL 

	select

		cw.cowid as ID,
		tdf.talep_sap_id,
		tdf.created_pr_id,
		'Sözleşme' as ust_kategori,
		cw.created_by,
		CAST(NULL AS datetime) as previous_processing_time,
		CAST(concat(cw.aedat,' ',cw.aezet) AS datetime) as process_time,
		'Kontrat Yaratılma Tarihi' as asama,
	    NULL as tdf_process_id,
		tdf.doc_id
	from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_100') }}  cw
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }}  tdf on tdf.tdf_id = cw.tdf_id
	where 1=1
		and concat(cw.aedat,' ',cw.aezet) NOT IN ('0000-00-00 00:00:00')
	union all

	select

		t300.cowid as ID,
		tdf.talep_sap_id,
		tdf.created_pr_id,
		'Sözleşme' as ust_kategori,
		t303.owner_unique_name,
		CAST(CONCAT(t303.begin_date,' ',t303.begin_time) AS datetime) as start_time,
		CAST(CONCAT(t303.end_date,' ',t303.end_time) AS datetime) as end_time,
		t303.title as asama,
		NULL as tdf_process_id,
		tdf.doc_id
	from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_300') }} t300
		left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_303') }}  t303 on t300.cowid = t303.cowid
		left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_305') }} t305 on t300.cowid = t305.cowid
		left join {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ariba_zarb_t_100') }} cw on cw.cowid = t303.cowid
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zmm_t_tdf_header') }} tdf on tdf.tdf_id = cw.tdf_id
	WHERE 1=1

		and t303.status != 'InActive'
		and concat(t303.begin_date,' ',t303.begin_time) NOT IN ('0000-00-00 00:00:00')
		AND CONCAT(t303.end_date,' ',t303.end_time) NOT IN ('0000-00-00 00:00:00')

UNION ALL 

  SELECT DISTINCT
       eb.[ebeln] AS ID
      ,[zzariba_req_no] AS birinci_pr
	  ,NULL as ikinci_pr
	  ,'Sipariş' AS ust_kategori
	  ,'' as creator
	  ,CAST(NULL AS datetime) as previous_process_date
      ,dateadd(second,86399,cast(   cast(aedat as date)  as datetime )) as process_date 
	  ,'Katalogdan Sipariş Yaratılma Tarihi' as surec
	  ,NULL as tdf_id
	  ,NULL as doc_id
  FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_eban') }} eb
  LEFT JOIN  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_ekko') }} e ON e.ebeln = eb.[ebeln]
 -- where zzariba_req_no = 'PR10591'
 WHERE 1=1 
 AND aedat is not null
  AND aedat != ''
),
finalized as (
SELECT
a.ID,
a.birinci_pr_id,
n.Name,
a.ikinci_pr_id,
a.ust_kategori,
a.kullanıcı,
a.previous_process_date,
a.process_date,
a.asama,
a.tdf_process_id,
n.cost_center_description,
n.cost_center_code,
n.[UniqueName] as company,
n.billing_address,
a.doc_id
FROM all_data a
 	LEFT JOIN cost_center_grouped_main n on n.InitialUniqueName = a.birinci_pr_id
where 1=1
	and kullanıcı != 'sapariba.rpa@ronesans.com'
	and ID is not null
	and ID != ''
)
,final_cte as (
	select
		id = a.ID,
		first_pr_id = a.birinci_pr_id,
		pr_name = a.Name,
		second_pr_id = a.ikinci_pr_id,
		process_category = a.ust_kategori,
		[user] = a.kullanıcı,
		-- previous_process_date = a.previous_process_date,
			CASE 
				WHEN a.previous_process_date IS NOT NULL THEN a.previous_process_date
				WHEN LAG(a.process_date, 1) OVER (PARTITION BY a.birinci_pr_id ORDER BY a.process_date) = a.process_date
					THEN LAG(a.process_date, 2) OVER (PARTITION BY a.birinci_pr_id ORDER BY a.process_date)
				ELSE LAG(a.process_date, 1) OVER (PARTITION BY a.birinci_pr_id ORDER BY a.process_date)
			END AS previous_proces_date,
		process_date = a.process_date,
		tdf_approval_group = a.asama,
		a.tdf_process_id,
		a.cost_center_description,
		a.cost_center_code,
		a.company,
		a.billing_address,
		is_io =
			CASE 
				WHEN 
					cost_center_code LIKE 'HOLH04%'
					OR cost_center_code LIKE 'HOLA04%'
					OR cost_center_code LIKE 'HOLA0300%'
					OR cost_center_code LIKE 'HOLA0301%'
				THEN 'Yes'
				Else 'No'
			END,
		includes_tender = 
			case when a.birinci_pr_id IN (SELECT b.birinci_pr_id from finalized b where b.ust_kategori = 'Ihale' or b.ust_kategori = 'İhale Hazırlık Süreci (SR)' ) Then 'Yes' Else 'No' End,
		doc_id
	from finalized a
	where a.birinci_pr_id <> ''
)

select 
	rls_region   = kuc.RegionCode 
	,rls_group   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,rls_company = CONCAT(COALESCE(company ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,rls_businessarea = CONCAT('_',kuc.RegionCode)
	,final_cte.*
from final_cte
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} kuc ON final_cte.company = kuc.RobiKisaKod
