{{
  config(
    materialized = 'view',tags = ['eff_kpi']
    )
}}

SELECT DISTINCT
	rls_region,
	rls_group,
	rls_company,
	rls_businessarea = concat(frm.txtUserDataSubAreaID, '_', rls_region) COLLATE Turkish_CI_AS,
    (
        SELECT CAST(MLD.TEXT AS nvarchar(25)) AS TEXT
        FROM [EBA].[EBA].[dbo].MLDICTIONARIES AS MLD
        WHERE CATEGORY = N'Process.Z008BusinessTrip'
              AND WORD = N'Project.Workflow.Statuses.' + CONVERT(NVARCHAR, LF.STATUS)
              AND MLD.LANGUAGE = N'Turkish'
    ) [status],
	FRM.txtUserDataSubAreaID AS business_area_code,
	FRM.txtUserDataAreaID AS company_code,
    FRM.docDataId AS [process_number],
    FRM.txtUserData AS [personnel],
    FRM.txtUserDataPos AS [position],
    FRM.txtDestination AS [destination_country_city],
    FRM.txtStartDateOfTrip AS [travel_start_date],
    FRM.txtEndDateOfTrip AS [travel_end_date],
    FRM.txtNumberOfDays AS [count_of_travel_days],
    FRM.txtTripType_TR AS [travel_type],
    FRM.txtReasonOfTrip AS [reason_for_travel],
    FRM.txtUserDataSubArea AS [business_area],
    FRM.txtUserDataDept AS [department],
    FRM.txtUserDataOrg AS [organization],
    FRM.txtAdvanceAmount AS [advance_amount],
    FRM.cmbAdvanceCurrency_TEXT AS [currency]
FROM [EBA].[EBA].[dbo].E_Z008BusinessTrip_Form AS FRM WITH (NOLOCK)
    INNER JOIN [EBA].[EBA].[dbo].DOCUMENTS AS D WITH (NOLOCK)
        ON FRM.ID = D.ID
    INNER JOIN [EBA].[EBA].[dbo].LIVEFLOWS AS LF WITH (NOLOCK)
        ON D.OWNERPROCESSID = LF.ID
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} dim_comp ON frm.txtUserDataAreaID = RobiKisaKod COLLATE Turkish_CI_AS
WHERE D.DELETED = 0
      AND LF.DELETED = 0
      AND LF.FLOWSTEP > 1