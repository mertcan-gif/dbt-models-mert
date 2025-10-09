{{
  config(
    materialized = 'table',tags = ['infra_kpi']
    )
}}



WITH _raw_progress_payment AS (
  SELECT 
    rls_region = cm.RegionCode
    ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
    ,rls_company = cm.KyribaKisaKod + '_' + cm.RegionCode
    ,rls_businessarea = TRIM(business_area) + '_' + cm.RegionCode
    ,cm.KyribaGrup as [group]
    ,[company]
    ,[business_area]
    ,t01w.name1 AS business_area_name
    ,[progress_payment_no]
    ,CONVERT(DATE, REPLACE(REPLACE(progress_payment_date, CHAR(160), ''), ' ', ''), 104) AS progress_payment_date
    ,CAST([collection_date] AS DATE) AS collection_date
    ,TRY_CAST([amount_to_be_paid_by_ecas_eur] AS MONEY) AS amount_eur
    ,REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(eca_status)), CHAR(13), ''), CHAR(10), ''), CHAR(160), '') AS [status] 
	,TRIM([progress_payment - advance]) AS [progress_payment_advance]
	,flag = 'ECA'
  ,CAST(data_control_date AS date) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_progresspaymentcollection') }} ppc
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t01w on TRIM(ppc.business_area) = t01w.werks
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(ppc.company) = cm.KyribaKisaKod
)

,_raw_tax AS (
	SELECT 
    rls_region = cm.RegionCode
    ,rls_group = cm.KyribaGrup + '_' + cm.RegionCode
    ,rls_company = cm.KyribaKisaKod + '_' + cm.RegionCode
    ,rls_businessarea = TRIM(business_area) + '_' + cm.RegionCode
    ,cm.KyribaGrup as [group]
    ,[company]
    ,[business_area]
    ,t01w.name1 AS business_area_name
    ,[progress_payment_no]
    ,CONVERT(DATE, REPLACE(REPLACE(progress_payment_date, CHAR(160), ''), ' ', ''), 104) AS progress_payment_date
    ,CAST([collection_date] AS DATE) AS collection_date
    ,CAST([vat__received_from_administration] AS MONEY) AS vat_received_from_administration
    ,TRIM([administration_statu]) as administration_statu  
    ,TRIM([progress_payment - advance]) AS [progress_payment_advance]
	,flag = 'TAX'
  ,CAST(data_control_date AS date) AS data_control_date
  FROM {{ source('stg_sharepoint', 'raw__infra_kpi_t_fact_progresspaymentcollection') }} ppc
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t01w on TRIM(ppc.business_area) = t01w.werks
    LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} cm on TRIM(ppc.company) = cm.KyribaKisaKod
	)

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw_progress_payment

union all

SELECT	
	*
  ,rls_key = CONCAT(rls_businessarea, '-', rls_company, '-', rls_group)
FROM _raw_tax
