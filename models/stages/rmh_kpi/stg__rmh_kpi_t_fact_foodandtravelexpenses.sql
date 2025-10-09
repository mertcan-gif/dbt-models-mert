{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
WITH main_cte as (
SELECT
    (
        SELECT CAST(TEXT AS nvarchar(25)) AS TEXT
        FROM eba.eba.dbo.MLDICTIONARIES AS MLD
        WHERE (CATEGORY = N'Process.Z159PersonnelMealPayment')
              AND (WORD = N'Project.Workflow.Statuses.' + CONVERT(NVARCHAR, LF.STATUS))
              AND (LANGUAGE = N'Turkish')
    )  collate database_default AS Durum,
    (
        SELECT MAX(FRQ.[RESPONSEDATE])
        FROM eba.eba.[dbo].[FLOWREQUESTS] AS FRQ
        WHERE ProcessID = FRM.txtProcessID
              AND FRQ.[RESPONSEDATE] IS NOT NULL
    ) AS [last_action_date],
        (
        SELECT MAX(FRQ.docDataId)
        FROM eba.eba.[dbo].[Zvw008TravelReport] AS FRQ
        WHERE ProcessID = FRM.txtProcessID
              --AND FRQ.[RESPONSEDATE] IS NOT NULL
    ) AS [doc_data_id],
    LF.ID AS ProcessID,
    D.DOCUMENTID AS [Süreç No],
    D.CREATEDATE AS [Oluşturma Tarihi],
    FRM.cmbMoney_TEXT AS [Para Birimi],
    FRM.txtDescr AS [Genel Açıklama],
    FRM.txtTotMoney AS [Toplam Tutar],
    FRM.txtCreator AS [Süreç Başlatan],
    FRM.txtUserData AS [Kimin Adına],
    FRM.txtUserDataRegistry AS [Sicil No],
    FRM.txtUserDataAreaId as comp,
    FRM.txtUserDataAreaId + N'-' + FRM.txtUserDataArea AS Şirket,
    yms_overtime.cmbBusArea AS [İş Alanı],
    FRM.txtUserDataDept AS Departman,
    FRM.txtUserDataOrg AS Organizasyon,
    FRM.txtUserDataPos AS Pozisyon,
    FRM.txtUserManager AS Yönetici,
    FRM.cmbTravTask AS [SGF Detay],
    FRM.cmbTravTask_TEXT ,
    FRM.UserDataSubAreaCode AS [user_data_sub_area_code],
    FRM.txtCity AS [city],
    FRM.cmbExpenseType AS [expense_type],
    FRM.txtsector AS [sector],
    FORMAT(CAST(yms_overtime.txtShiftEnd AS DATETIME), 'HH:mm') as overtime_shift_end_time,
    FORMAT(CAST(yms_overtime.txtShiftStart AS DATETIME), 'HH:mm') as overtime_shift_start_time,
    FRM.txtTotMoney as yms_total_cost
FROM eba.eba.dbo.E_Z159PersonnelMealPayment_Form AS FRM WITH (NOLOCK)
    INNER JOIN eba.eba.dbo.DOCUMENTS AS D WITH (NOLOCK)
        ON FRM.ID = D.ID
    INNER JOIN eba.eba.dbo.LIVEFLOWS AS LF WITH (NOLOCK)
        ON D.OWNERPROCESSID = LF.ID
    LEFT JOIN eba.eba.dbo.E_Z159PersonnelMealPayment_Form_dtDet f on f.FORMID = FRM.ID
    LEFT JOIN eba.eba.dbo.E_Z159PersonnelMealPayment_Modal1 yms_overtime on yms_overtime.ID = f.DOCUMENTID
WHERE (D.DELETED = 0)
      AND (LF.DELETED = 0)
      AND (LF.FLOWSTEP > 1)
),
rls_added_food_previous as (
SELECT
    [rls_region]   = comp.RegionCode collate database_default,
    [rls_group]   = CONCAT(COALESCE(comp.KyribaGrup,''),'_',COALESCE(comp.RegionCode,'')) collate database_default,
    [rls_company] = CONCAT(COALESCE(comp  ,''),'_'	,COALESCE(comp.RegionCode,''),'') collate database_default, 
    [rls_businessarea] = [İş Alanı],
    comp.KyribaGrup as [group],
    fc.Durum as status,
    ProcessID as process_id,
    [doc_data_id],
    [last_action_date],
    [Süreç No] as process_no,
    [Oluşturma Tarihi] as create_date,
    [Para Birimi] as currency,
    [Genel Açıklama] as description,
    [Toplam Tutar] as total,
    [Süreç Başlatan] as creator,
    [Kimin Adına] as for_whom,
    [Sicil No] as sicil_no,
    comp as company,
    Şirket as company1,
    [İş Alanı] as business_area,
    Departman as deparment,
    Organizasyon as organisation,
    Pozisyon as position,
    Yönetici as manager,
    case when cmbTravTask_TEXT != '' then LEFT(cmbTravTask_TEXT, CHARINDEX(' /', cmbTravTask_TEXT) - 1)
    else null end as  details,
    overtime_shift_end_time,
    overtime_shift_start_time,
    yms_total_cost,
    expense_type,
    ROW_NUMBER() OVER (Partition by concat([SGF Detay],'_',[İş Alanı]) order by last_action_date DESC) as rn
from main_cte fc
LEFT JOIN  {{ ref('dm__dimensions_t_dim_companies')}}  comp on comp.[RobiKisaKod] = fc.comp collate database_default
)
,rls_added_food as (
SELECT
*
FROM  rls_added_food_previous
WHERE 1=1
    and rn= '1')

, sgf_cte as (
SELECT 
    tr.ProcessID, 
	txtUserDataAreaID,
	GSicil as gsicil,
	tr.docDataId ,
	MIN(tr.CREATEDATE) as sgf_start,
	MAX(fr.RESPONSEDATE) as sgf_end
FROM eba.eba.[dbo].[FLOWREQUESTS] fr
	LEFT JOIN eba.eba.[dbo].[Zvw008TravelReport] tr ON fr.PROCESSID = tr.ProcessID
    --LEFT JOIN dimensions.raw__dwh_t_dim_companymapping  comp on comp.[KyribaKisaKod] = tr.txtUserDataAreaID collate database_default
where 1=1
	AND PROCESS = 'Z008BusinessTrip'
	AND fr.RESPONSEDATE IS NOT NULL
	--AND tr.StatusID =3
GROUP BY tr.docDataId,GSicil ,txtUserDataAreaID, tr.ProcessID
)
,rls_added_sgf as (
SELECT 
	[rls_region]   = comp.RegionCode collate database_default,
	[rls_group]   = CONCAT(COALESCE(comp.KyribaGrup,''),'_',COALESCE(comp.RegionCode,'')) collate database_default,
	[rls_company] = CONCAT(COALESCE(fc.txtUserDataAreaID  ,''),'_'	,COALESCE(comp.RegionCode,''),'') collate database_default, 
	[rls_businessarea] = tr.BusinessArea,
	comp.KyribaGrup as [group],
	fc.txtUserDataAreaID as company,
	fc.PROCESSID,	
	fc.gsicil,
	fc.docDataId as doc_data_id,
	fc.sgf_start,
	fc.sgf_end,
	tr.txtUserDataName as user_name,
    tr.txtUserDataRegistry,
	tr.Status as status,
	tr.StatusID as status_id,
	tr.isReservationLate as is_reservation_late,
	tr.txtReservationLate as why_reservation_late, 
	tr.CREATEDATE as create_date,
	tr.txtStartDateOfTrip as start_date_of_trip,
	tr.txtEndDateOfTrip as end_date_of_trip,
	tr.txtNumberOfDays as number_of_days,
	tr.txtReasonOfTrip as reason_of_trip,
	tr.ProcessID as process_id,
	tr.txtUserDataAreaID as user_data_area_id,
    tr.BusinessArea as business_area,
    tr.txtUserDataSubAreaID as user_data_sub_ara_id,
    tr.cmbVarisSehri_TEXT as city,
    tr.AccountAssignment as account_assignment,
   -- ht.HotelName AS hotel_name,
   -- ht.HotelNo as hotel_no,
    tr.txtVehicleList_TR as vehicle_type,
    tr.DistributionRate as distribution_rate,
    tr.rlistTripTypeID as trip_type
FROM sgf_cte fc
	LEFT JOIN  {{ ref('dm__dimensions_t_dim_companies')}}  comp on comp.[RobiKisaKod] = fc.txtUserDataAreaID collate database_default
	LEFT JOIN EBA.EBA.dbo.[Zvw008TravelReport] tr on   tr.ProcessID = fc.PROCESSID --AND tr.StatusID =3
	LEFT JOIN  eba.eba.dbo.E_Z008BusinessTrip_Form bb on bb.txtProcessId= tr.ProcessID
),
last_cte as (
SELECT 
rls_region = cast(COALESCE(ras.rls_region,raf.rls_region) as varchar(250)),
rls_group = cast(COALESCE(ras.rls_group,raf.rls_group) as varchar(250)),
rls_company = cast(COALESCE(ras.rls_company,raf.rls_company) as varchar(250)),
rls_businessarea = COALESCE(ras.rls_businessarea,raf.rls_businessarea),
[group] = cast(COALESCE(ras.[group],raf.[group]) as varchar(250)),
company = LTRIM(RTRIM(COALESCE(ras.company,raf.company))),
'SGF' as process_type,
LTRIM(RTRIM(ras.PROCESSID)) as process_id,	
LTRIM(RTRIM(ras.txtUserDataRegistry)) as sicil_no,
LTRIM(RTRIM(ras.doc_data_id)) as code,
ras.sgf_start as process_start,
ras.sgf_end as process_end,
LTRIM(RTRIM(ras.user_name)) as process_user,
cast(LTRIM(RTRIM(ras.status)) as nvarchar(250)) collate database_default as process_status,
cast(LTRIM(RTRIM(ras.is_reservation_late)) as varchar(250)) as sgf_is_reservation_late,
cast(LTRIM(RTRIM(ras.why_reservation_late)) as varchar(250)) as sgf_reason_reservation_late, 
ras.create_date as process_creation_date,
ras.start_date_of_trip as sgf_start_date_of_trip,
ras.end_date_of_trip as sgf_end_date_of_trip,
LTRIM(RTRIM(ras.number_of_days)) as sgf_number_of_days,
cast(LTRIM(RTRIM(ras.reason_of_trip)) as varchar(250))  as process_goal,
cast(LTRIM(RTRIM(ras.city)) as varchar(250)) as sgf_city,
cast(LTRIM(RTRIM(ras.account_assignment)) as varchar(250)) as sgf_account_assignment,
--cast(LTRIM(RTRIM(ras.hotel_name)) as varchar(250)) as sgf_hotel_name,
--cast(LTRIM(RTRIM(ras.hotel_no)) as varchar(250)) as hotel_no,

cast(LTRIM(RTRIM(ras.vehicle_type)) as varchar(250)) as vehicle_type,

cast(LTRIM(RTRIM(raf.status)) as varchar(250)) as yms_status,
cast(LTRIM(RTRIM(raf.process_id)) as varchar(250)) as yms_process_id,
cast(LTRIM(RTRIM(raf.description)) as varchar(250)) as yms_description,
raf.create_date as yms_create_date,
raf.last_action_date as yms_last_action_date,
cast(LTRIM(RTRIM(raf.process_no)) as varchar(250)) as yms_process_no,
cast(LTRIM(RTRIM(raf.total)) as varchar(250)) as yms_total,
cast(LTRIM(RTRIM(raf.creator)) as varchar(250)) as yms_creator,
cast(LTRIM(RTRIM(raf.for_whom)) as varchar(250)) as yms_created_for_whom,
cast(LTRIM(RTRIM(raf.sicil_no)) as varchar(250)) as yms_sicil_no,
cast(LTRIM(RTRIM(raf.company)) as varchar(250)) as yms_company,
cast(LTRIM(RTRIM(raf.company1)) as varchar(250)) as yms_company_description,
cast(LTRIM(RTRIM(raf.business_area)) as varchar(250)) as yms_business_area,
cast(LTRIM(RTRIM(raf.deparment)) as varchar(250)) as yms_department,
cast(LTRIM(RTRIM(raf.organisation)) as varchar(250)) as yms_organisation,
cast(LTRIM(RTRIM(raf.position)) as varchar(250)) as yms_position,
cast(LTRIM(RTRIM(raf.manager)) as varchar(250)) as yms_manager,
cast(LTRIM(RTRIM(raf.details)) as varchar(250)) as yms_details,
cast(raf.overtime_shift_end_time as varchar(250)) as overtime_shift_end_time,
cast(raf.overtime_shift_start_time as varchar(250)) as overtime_shift_start_time,
raf.yms_total_cost,
raf.expense_type as yms_expense_type,
ras.trip_type,
ras.distribution_rate
FROM rls_added_sgf as ras
FULL OUTER JOIN rls_added_food as raf on raf.details =ras.doc_data_id
                                         and ras.rls_businessarea = raf.rls_businessarea
where 1=1
)
SELECT
       [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
      ,[company]
      ,[process_type]
      ,[process_id]
      ,[sicil_no]
      ,[code]
      ,[process_start]
      ,[process_end]
      ,[process_user]
      ,[process_status]
      ,[sgf_is_reservation_late]
      ,[sgf_reason_reservation_late]
      ,[process_creation_date]
      ,[sgf_start_date_of_trip]
      ,[sgf_end_date_of_trip]
      ,[sgf_number_of_days]
      ,[process_goal]
      ,[sgf_city]
      ,[sgf_account_assignment]
      ,[vehicle_type]
      ,[yms_status]
      ,[yms_process_id]
      ,[yms_description]
      ,[yms_create_date]
      ,[yms_last_action_date]
      ,[yms_process_no]
      ,[yms_total]
      ,[yms_creator]
      ,[yms_created_for_whom]
      ,[yms_sicil_no]
      ,[yms_company]
      ,[yms_company_description]
      ,[yms_business_area]
      ,[yms_department]
      ,[yms_organisation]
      ,[yms_position]
      ,[yms_manager]
      ,[yms_details]
      ,[overtime_shift_end_time]
      ,[overtime_shift_start_time]
      ,[yms_total_cost]
      ,[yms_expense_type]
      ,[trip_type]
      ,SUM(distribution_rate) AS distribution_rate
FROM last_cte
GROUP BY
       [rls_region]
      ,[rls_group]
      ,[rls_company]
      ,[rls_businessarea]
      ,[group]
      ,[company]
      ,[process_type]
      ,[process_id]
      ,[sicil_no]
      ,[code]
      ,[process_start]
      ,[process_end]
      ,[process_user]
      ,[process_status]
      ,[sgf_is_reservation_late]
      ,[sgf_reason_reservation_late]
      ,[process_creation_date]
      ,[sgf_start_date_of_trip]
      ,[sgf_end_date_of_trip]
      ,[sgf_number_of_days]
      ,[process_goal]
      ,[sgf_city]
      ,[sgf_account_assignment]
      ,[vehicle_type]
      ,[yms_status]
      ,[yms_process_id]
      ,[yms_description]
      ,[yms_create_date]
      ,[yms_last_action_date]
      ,[yms_process_no]
      ,[yms_total]
      ,[yms_creator]
      ,[yms_created_for_whom]
      ,[yms_sicil_no]
      ,[yms_company]
      ,[yms_company_description]
      ,[yms_business_area]
      ,[yms_department]
      ,[yms_organisation]
      ,[yms_position]
      ,[yms_manager]
      ,[yms_details]
      ,[overtime_shift_end_time]
      ,[overtime_shift_start_time]
      ,[yms_total_cost]
      ,[yms_expense_type]
      ,[trip_type]

