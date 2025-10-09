{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
with final_cte_request as (
SELECT  
	txtUserDataAreaID AS company,
	GSicil as gsicil,
	tr.docDataId as doc_data_id,
	fr.requestdate as request_date,
	fr.responsedate as response_date,
    LAG(fr.responsedate) OVER (PARTITION BY tr.docDataId  ORDER BY fr.responsedate) AS previous_response_date,
	CAST(CAST(DATEDIFF(SECOND, fr.requestdate, fr.responsedate) AS DECIMAL(18, 2)) / 86400 AS DECIMAL(18,2)) AS transaction_time_in_days,
	CAST(CAST(DATEDIFF(SECOND, LAG(fr.responsedate) OVER (PARTITION BY tr.docDataId  ORDER BY fr.responsedate), fr.responsedate) AS DECIMAL(18, 2)) / 86400 AS DECIMAL(18,2)) AS transaction_time_in_days_previous_response_taken,
	USERID as user_id,
	APPROVERUSERID as approver_user_id
FROM eba.eba.[dbo].[FLOWREQUESTS] fr
	LEFT JOIN eba.eba.[dbo].[Zvw008TravelReport] tr ON fr.PROCESSID = tr.ProcessID
where 1=1
	AND PROCESS = 'Z008BusinessTrip'
	and fr.status = '5'
	and tr.docDataId is not null
)
,final_cte_food as (
SELECT 
	CASE 
		WHEN CHARINDEX('/',FRM.cmbTravTask) > 0 
		THEN LTRIM(RTRIM(LEFT(FRM.cmbTravTask, CHARINDEX('/', FRM.cmbTravTask) - 1)))
		ELSE NULL
	END AS details,
	D.DOCUMENTID as document_id,
    txtUserDataAreaID as company,
	FRQ.PROCESS as process,
	FRQ.PROCESSID as process_id,
	FRQ.STATUS as status,
    FRM.txtUserDataId as gsicil,
	FRQ.FLOWOBJECT as flow_object,
	FRQ.REQUESTDATE as request_date,
	FRQ.RESPONSEDATE as response_date,
    LAG(RESPONSEDATE) OVER (PARTITION BY D.DOCUMENTID ORDER BY RESPONSEDATE) AS previous_response_date,
	CAST(CAST(DATEDIFF(SECOND, FRQ.REQUESTDATE, FRQ.RESPONSEDATE ) AS DECIMAL(18, 2)) / 86400 AS DECIMAL(18,2)) AS transaction_time_in_days,
	CAST(CAST(DATEDIFF(SECOND,  LAG(FRQ.RESPONSEDATE) OVER (PARTITION BY D.DOCUMENTID ORDER BY FRQ.RESPONSEDATE ), FRQ.RESPONSEDATE ) AS DECIMAL(18, 2)) / 86400 AS DECIMAL(18,2)) AS transaction_time_in_days_previous_response_taken,
	FRQ.USERID as user_id,
	FRQ.APPROVERUSERID as approver_user_id
FROM eba.eba.[dbo].[FLOWREQUESTS] AS FRQ
LEFT JOIN eba.eba.dbo.E_Z159PersonnelMealPayment_Form AS FRM  on FRM.txtProcessID = FRQ.ProcessID
INNER JOIN eba.eba.dbo.DOCUMENTS AS D
        ON FRM.ID = D.ID
WHERE	1=1
        AND FRQ.[RESPONSEDATE] IS NOT NULL
		and FRQ.PROCESS = 'Z159PersonnelMealPayment'
)
,requests as (
SELECT
	[rls_region]   = comp.RegionCode collate database_default,
	[rls_group]   = CONCAT(COALESCE(comp.KyribaGrup,''),'_',COALESCE(comp.RegionCode,'')) collate database_default,
	[rls_company] = CONCAT(COALESCE(company  ,''),'_'	,COALESCE(comp.RegionCode,''),'') collate database_default, 
	[rls_businessarea] = company,
	comp.KyribaGrup as [group],
	fc.*
FROM final_cte_request fc
	LEFT JOIN  {{ ref('dm__dimensions_t_dim_companies')}}  comp on comp.[KyribaKisaKod] = fc.company collate database_default
)
,food as (
SELECT
    [rls_region]   = comp.RegionCode collate database_default,
    [rls_group]   = CONCAT(COALESCE(comp.KyribaGrup,''),'_',COALESCE(comp.RegionCode,'')) collate database_default,
    [rls_company] = CONCAT(COALESCE(company  ,''),'_'	,COALESCE(comp.RegionCode,''),'') collate database_default, 
    [rls_businessarea] = company,
    comp.KyribaGrup as [group],
    fc.*
FROM final_cte_food fc
LEFT JOIN  {{ ref('dm__dimensions_t_dim_companies')}}  comp on comp.[KyribaKisaKod] = fc.company collate database_default
)

,last_cte as (
select 
       [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
	  , NULL as yms_details
      ,[company]
      ,[gsicil]
      ,[doc_data_id]
      ,[request_date]
      ,[response_date]
      ,[previous_response_date]
      ,[transaction_time_in_days_previous_response_taken]
      ,[transaction_time_in_days]
      ,[user_id]
      ,[approver_user_id]
from requests
UNION ALL
select 
	   [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
      ,[details] as yms_details
      ,[company]
	  ,[gsicil]
      ,[document_id] as doc_data_id
      --,[process]
      --,[process_id]
      --,[status]
      --,[flow_object]
      ,[request_date]
      ,[response_date]
      ,[previous_response_date]
      ,[transaction_time_in_days_previous_response_taken]
      ,[transaction_time_in_days]
      ,[user_id]
      ,[approver_user_id]
from food
)
select * 
from last_cte
where 1=1
      and not (yms_details is null and doc_data_id = '' )