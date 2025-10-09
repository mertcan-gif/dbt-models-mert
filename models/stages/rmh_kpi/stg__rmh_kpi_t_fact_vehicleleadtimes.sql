{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
WITH final_cte as (
SELECT  
	vr.txtProcessID AS process_id,
	NULL AS gsicil,
	txtUserDataRegistry as atf_sicil_no,
	vr.docDataID as atf_doc_data_id,
	MIN(vr.docDataCreateDate) as create_date,
	MAX(fr.RESPONSEDATE) as finish_date,
	userDataCreator as creator_name,
	txtReasonForRequest AS atf_reason,
	txtUserDataAreaID AS atf_user_data_area_id,
	fd.cmbExpenseCenter as atf_business_area,
	txtUserDataSubAreaID as user_data_sub_area_id,
	cmbDestinationCity as atf_city,
	vr.txtStartDateOfRequest as request_start_date,
	vr.txtEndDateOfRequest as request_end_date  
FROM eba.eba.[dbo].[FLOWREQUESTS] fr  
	LEFT JOIN eba.eba.[dbo].[E_Z206VehicleRequest_Form] vr ON fr.PROCESSID = vr.txtProcessID 
	LEFT JOIN  eba.eba.dbo.E_Z206VehicleRequest_Form_dgAccountAssignments fd on fd.FORMID = vr.ID 
where 1=1
	AND PROCESS = 'Z206VehicleRequest'
	AND RESPONSEDATE IS NOT NULL
    AND PROCESSID IN (
		SELECT DISTINCT PROCESSID
		FROM eba.eba.[dbo].[FLOWREQUESTS] fr
			LEFT JOIN eba.eba.[dbo].[E_Z206VehicleRequest_Form] vr ON fr.PROCESSID = vr.txtProcessID
		where 1=1
			AND PROCESS = 'Z206VehicleRequest'
			AND fr.REQUESTTYPE = 6 --Done olan statusu bul
			and LEN(vr.docDataID) >2
	)
GROUP BY
	vr.txtProcessID,
	txtUserDataRegistry,
	vr.docDataID,
	userDataCreator,
	txtReasonForRequest,
	txtUserDataAreaID,
	fd.cmbExpenseCenter,
	txtUserDataSubAreaID,
	cmbDestinationCity,
	vr.txtStartDateOfRequest,
	vr.txtEndDateOfRequest 
)
SELECT
	[rls_region]   = comp.RegionCode collate database_default,
	[rls_group]   = CONCAT(COALESCE(comp.KyribaGrup,''),'_',COALESCE(comp.RegionCode,'')) collate database_default,
	[rls_company] = CONCAT(COALESCE(atf_user_data_area_id  ,''),'_'	,COALESCE(comp.RegionCode,''),'') collate database_default, 
	[rls_businessarea] = atf_business_area,	
	comp.KyribaGrup as [group],
	atf_user_data_area_id as company,
	'ATF' as process_type,
	process_id,
	atf_sicil_no,
	atf_doc_data_id,
	create_date,
	finish_date,
	creator_name,
	CAST(NULL AS VARCHAR(20))  collate database_default as status,
	cast(atf_reason AS VARCHAR(200)) AS atf_reason,
	atf_city,
	request_start_date,
	request_end_date
FROM final_cte ff 
	LEFT JOIN  {{ ref('dm__dimensions_t_dim_companies')}}  comp on comp.[RobiKisaKod]  = ff.atf_user_data_area_id collate database_default