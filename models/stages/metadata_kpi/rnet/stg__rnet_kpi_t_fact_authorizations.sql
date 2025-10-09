{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}


SELECT        
	FullYetki.EMAIL collate SQL_Latin1_General_CP1_CI_AS AS [user_id], 
	FullYetki.full_name,
	cast(menuparams.VALUE as nvarchar(max)) collate SQL_Latin1_General_CP1_CI_AS AS [report_id], 
	cast(structure.CAPTION as nvarchar(max)) collate SQL_Latin1_General_CP1_CI_AS AS [report_name], 
	N'Read' AS report_user_access_right, 
	NULL AS auth_start_date, 
	NULL AS auth_end_date,
	'NON-ERP' as segment
FROM            
	(
		SELECT  
			CONCAT(FIRSTNAME,' ',LASTNAME) full_name,      
			REPLACE(userRole.INCLUDENAME, N'webApplications.', N'') AS WebYetkiProfili, 
			roleUser.EMAIL
		FROM EBA.EBA.dbo.SMROLEGROUPCONTENT AS userRole WITH (NOLOCK)
			INNER JOIN EBA.EBA.dbo.OSUSERS AS roleUser WITH (NOLOCK) ON userRole.NAME = roleUser.ID AND userRole.TYPE = 11 AND roleUser.STATUS = 1 AND roleUser.TYPE = 0
		UNION ALL
		SELECT
			CONCAT(FIRSTNAME,' ',LASTNAME) full_name, 
			REPLACE(groupRole.INCLUDENAME, N'webApplications.', N'') AS WebYetkiProfili, 
			gpUsers.EMAIL
		FROM EBA.EBA.dbo.SMROLEGROUPCONTENT AS groupRole WITH (NOLOCK) 
			INNER JOIN EBA.EBA.dbo.OSGROUPCONTENT AS roleGroup WITH (NOLOCK) ON roleGroup.GROUPID = groupRole.NAME AND groupRole.TYPE = 8 
			INNER JOIN EBA.EBA.dbo.OSUSERS AS gpUsers WITH (NOLOCK) ON gpUsers.ID = roleGroup.USERID AND gpUsers.STATUS = 1 AND gpUsers.TYPE = 0
	) AS FullYetki 
	INNER JOIN EBA.EBA.dbo.MENUSTRUCTURE AS structure WITH (NOLOCK) ON structure.PROFILE = FullYetki.WebYetkiProfili
	INNER JOIN EBA.EBA.dbo.MENUSTRUCTUREPARAMS AS menuparams WITH (NOLOCK) ON menuparams.ID = structure.ID AND menuparams.PARAMETER = N'ARCHIVENAME'
