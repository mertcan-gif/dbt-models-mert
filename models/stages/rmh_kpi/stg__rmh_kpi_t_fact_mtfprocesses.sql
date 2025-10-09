{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
SELECT
	case when rls_region is null and FRM.txtUserDataAreaId = 'RIC' THEN 'TUR' ELSE rls_region END AS rls_region,
	[rls_group]   = CONCAT(COALESCE(comp.KyribaGrup,''),'_',COALESCE(comp.RegionCode,'')) collate database_default,
	[rls_company] = CONCAT(COALESCE(FRM.txtUserDataAreaId  ,''),'_'	,COALESCE(comp.RegionCode,''),'') collate database_default, 
	[rls_businessarea] = cmbCostCenter,
	comp.KyribaGrup as [group],
	FRM.txtUserDataAreaId as company,
	'MTF' as process_type,
	LF.ID AS mtf_processid,
	FRM.txtUserDataRegistry AS mtf_sicilno,
	D.DOCUMENTID AS mtf_doc_data_id,
	D.CREATEDATE AS [Oluşturma Tarihi],
	(
		SELECT MAX(FRQ.[RESPONSEDATE])
		FROM eba.eba.[dbo].[FLOWREQUESTS] AS FRQ
		WHERE ProcessID = FRM.txtProcessID
				AND FRQ.[RESPONSEDATE] IS NOT NULL
	) AS [last_action_date],
	FRM.txtUserData AS [Kimin Adına],
    cast((
        SELECT CAST(TEXT AS nvarchar(25)) AS TEXT
        FROM eba.eba.dbo.MLDICTIONARIES AS MLD
        WHERE (CATEGORY = N'Process.Z009MatReqPro')
              AND (WORD = N'Project.Workflow.Statuses.' + CONVERT(NVARCHAR, LF.STATUS))
              AND (LANGUAGE = N'Turkish')
    ) as nvarchar(250)) collate database_default AS Durum,
	CAST(D.CREATEDATE AS VARCHAR(250)) AS mtf_creation_date,
	CAST(txtAciklama AS VARCHAR(250)) as mtf_reason
FROM eba.eba.dbo.E_Z009MatReqPro_Form AS FRM
    JOIN eba.eba.dbo.DOCUMENTS AS D 	
        ON FRM.ID = D.ID  
    JOIN eba.eba.dbo.LIVEFLOWS AS LF 
        ON D.OWNERPROCESSID = LF.ID  
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies')}} comp on comp.[RobiKisaKod]  = FRM.txtUserDataAreaId  collate database_default
WHERE (D.DELETED = 0)	
      AND (LF.DELETED = 0)	
      AND (LF.FLOWSTEP > 1)	