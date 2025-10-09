{{
  config(
    materialized = 'table',tags = ['nwc_kpi','rlsdimensions']
    )
}}

WITH DM_MONTHLY AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,kyriba_group = KyribaGrup
		,rbukrs
		,business_area_code
		,business_area
	FROM {{ ref('dm__nwc_kpi_t_fact_monthlyreport') }} 

)

,DM_PROJECTSTATUS AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,KyribaGrup
		,company
		,business_area_code
		,projects
	FROM {{ ref('dm__nwc_kpi_t_fact_projectstatus') }}

)

,DM_PROJECTPROGRESS AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,KyribaGrup
		,company
		,sap_business_area
		,project_name
	FROM {{ ref('dm__nwc_kpi_t_fact_projectprogress') }}

)

,DM_ADVANGEAGING AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,kyriba_group
		,company
		,business_area
		,business_area_description
	FROM {{ ref('dm__nwc_kpi_t_fact_advanceaging_cleareditems') }}

)

,DM_AR AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,kyriba_group
		,company
		,business_area
		,business_area_description
	FROM {{ ref('dm__nwc_kpi_t_fact_accountsreceivable_cleareditems') }}

)

,DM_AP AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,kyriba_group
		,company
		,business_area
		,business_area_description
	FROM {{ ref('dm__nwc_kpi_t_fact_accountspayable_cleareditems') }}

)

,DM_DUETODUEFROM AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,KyribaGrup
		,bukrs
		,BUSINESS_AREA
		,gsber
	FROM {{ ref('dm__nwc_kpi_t_fact_duetoduefrom') }}

)

,DM_PAYMENTPERFORMANCE AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,KyribaGrup
		,company
		,work_area  
		,work_area_name
	FROM {{ ref('dm__nwc_kpi_t_fact_paymentperformance') }}
)

,DM_STOCKAGING AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,kyriba_group
		,company
		,business_area
		,business_area_description
	FROM {{ ref('dm__nwc_kpi_t_fact_stockaging') }}

)

,DM_STOCKAGINGDEPOTS AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,KyribaGrup
		,[Şirket kodu]
		,[Üretim Yeri]
		,[Ad 1]
	FROM {{ ref('dm__nwc_kpi_t_fact_stockagingdepots') }}

)

,DM_BANKBALANCES AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,KyribaGrup
		,RBUKRS
		,business_area = ''
		,business_area_description = ''
	FROM {{ ref('dm__nwc_kpi_t_fact_s4mevduat') }}

)

,DM_DEPOSITCOCKPIT AS (

	SELECT DISTINCT
		rls_region
		,rls_group
		,rls_company
		,rls_businessarea
		,KyribaGrup
		,company
		,business_area = ''
		,business_area_description = ''
	FROM {{ ref('dm__nwc_kpi_t_fact_depositcockpit') }}

)

SELECT * FROM DM_MONTHLY
	UNION ALL
SELECT * FROM DM_PROJECTSTATUS
	UNION ALL
SELECT * FROM DM_PROJECTPROGRESS
	UNION ALL
SELECT * FROM DM_ADVANGEAGING
	UNION ALL
SELECT * FROM DM_AR
	UNION ALL
SELECT * FROM DM_AP
	UNION ALL
SELECT * FROM DM_DUETODUEFROM
	UNION ALL
SELECT * FROM DM_PAYMENTPERFORMANCE
	UNION ALL
SELECT * FROM DM_STOCKAGING
	UNION ALL
SELECT * FROM DM_STOCKAGINGDEPOTS
	UNION ALL
SELECT * FROM DM_BANKBALANCES
	UNION ALL
SELECT * FROM DM_DEPOSITCOCKPIT
