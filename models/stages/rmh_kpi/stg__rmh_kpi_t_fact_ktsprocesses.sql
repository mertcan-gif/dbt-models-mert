{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
  SELECT
	rls_region,
	[rls_group]   = CONCAT(COALESCE(comp.KyribaGrup,''),'_',COALESCE(comp.RegionCode,'')) collate database_default,
	[rls_company] = CONCAT(COALESCE(FRM.txtUserDataAreaId  ,''),'_'	,COALESCE(comp.RegionCode,''),'') collate database_default, 
	[rls_businessarea] = FRM.cmbExpCent,
	comp.KyribaGrup as [group],
	FRM.txtUserDataAreaId as company,
	'KTS' as process_type,
	LF.ID AS kts_processid,
	FRM.txtUserDataRegistry AS kts_sicilno,
	D.DOCUMENTID AS kts_doc_data_id,
	D.CREATEDATE AS [Oluşturma Tarihi],
	(
		SELECT MAX(FRQ.[RESPONSEDATE])
		FROM eba.eba.[dbo].[FLOWREQUESTS] AS FRQ
		WHERE ProcessID = FRM.txtProcessID
				AND FRQ.[RESPONSEDATE] IS NOT NULL
	) AS [last_action_date],
	FRM.txtUserData AS [Kimin Adına],
    (
        SELECT CAST(TEXT AS nvarchar(25)) AS TEXT
        FROM eba.eba.dbo.MLDICTIONARIES AS MLD
        WHERE (CATEGORY = N'Process.Z012BussCardReqProc')
              AND (WORD = N'Project.Workflow.Statuses.' + CONVERT(NVARCHAR, LF.STATUS))
              AND (LANGUAGE = N'Turkish')
    )  collate database_default AS Durum,
	D.CREATEDATE AS kts_creation_date,
	cast(txtPos as varchar(250))  as kts_reason
FROM eba.eba.dbo.E_Z012BussCardReqProc_Form AS FRM  	--SGF ana formu
    JOIN eba.eba.dbo.DOCUMENTS AS D  	--Tum formların tutuldugu tablo
        ON FRM.ID = D.ID  
    JOIN eba.eba.dbo.LIVEFLOWS AS LF  --Tum sureclerin tutuldu?u tablo
        ON D.OWNERPROCESSID = LF.ID  
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies')}} comp on comp.[RobiKisaKod] = FRM.txtUserDataAreaId collate database_default

WHERE (D.DELETED = 0)	--Form silinmis ise getirme
      AND (LF.DELETED = 0)	--Surec silinmis ise getirme
      AND (LF.FLOWSTEP > 1)	--Onaya sunulmamıs ise (Taslak ise) getirme