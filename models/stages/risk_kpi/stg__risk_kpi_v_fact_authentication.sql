{{
  config(
    materialized = 'table',tags = ['risk_kpi']
    )
}}
  Select distinct
  s.FullName as user_fullname,
  S.EMail as user_email,
  up.ObjectID as project_id,
  up.Code as project_code,
  up.Name as project_name
FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].[SYS_USERS] s
	LEFT JOIN (Select c.* , 1 as IsAdmin 
				FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].[PRJ_PROJECTS] c
				where 1=1
				 AND c.IsActive = '1'
				 AND c.IsDeleted != '1') up on s.IsAdmin = up.IsAdmin
where 1=1
 AND s.IsActive = '1'
 AND s.IsDeleted != '1'
 AND s.IsAdmin = '1'

UNION ALL

 Select distinct
  s.FullName as user_fullname,
  s.EMail as user_email,
  c.ObjectID as project_id,
  c.Code as project_code,
  c.Name as project_name
FROM PRODAPPSDB.RNS_RISK_PROD.[dbo].[SYS_USERS] s
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[SYS_USER_PROJECT_AUTHS] sy on s.ObjectID = sy.UserID
	LEFT JOIN PRODAPPSDB.RNS_RISK_PROD.[dbo].[PRJ_PROJECTS] c on sy.ProjectID = c.ObjectID
WHERE 1=1
 AND s.IsActive = '1'
 AND s.IsDeleted != '1'

 and s.IsAdmin != '1'
 and sy.AuthID = '3'
 and sy.Status = '1'
 AND sy.IsActive = '1'
 AND sy.IsDeleted != '1'
 AND c.IsActive = '1'
 AND c.IsDeleted != '1'