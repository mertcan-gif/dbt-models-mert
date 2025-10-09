
{{
  config(
    materialized = 'table',tags = ['nwc_kpi','balancesheet']
    )
}}

with adjusted_due_to_due_from as (

	select
		  rbukrs = [BUKRS]
		  ,business_area = BUSINESS_AREA
		  ,business_area_code = gsber
		  ,yearmonth = LEFT(starting_date_of_month,7)
		  ,bs_mapping = '' --[due_type]
		  ,general_ledger_account = ''
		  ,main_account_from_mapping = ''
		  ,main_account = CASE 
		  					  WHEN [due_type] = 'Due to Customers Under Construction Contracts' THEN '350' 
		  					  WHEN [due_type] = 'Due from Clients Under Construction Contracts' THEN '170'
						  END
		  ,vuk_account_name = ''
		  ,customer_vendor_code = ''
		  ,totalamount = [due]
		  ,currency
		  ,try_value
		  ,eur_value
		  ,bs_mapping_1 = CASE 
		  					  WHEN [due_type] = 'Due to Customers Under Construction Contracts' THEN 'Current Liabilities' 
		  					  WHEN [due_type] = 'Due from Clients Under Construction Contracts' THEN 'Current Assets'
						  END
		  ,bs_mapping_2 = [due_type]
		  
		  /** Furkan Eminsoy Bey'in ilettiği mapping dosyasındaki sıralamalar aşağıdaki gibidir **/
		  ,[rank] = CASE 
		  					  WHEN [due_type] = 'Due to Customers Under Construction Contracts' THEN '69'
		  					  WHEN [due_type] = 'Due from Clients Under Construction Contracts' THEN '14'
						  END
		  ,[source] = 'Due to Due From'
	from {{ ref('dm__nwc_kpi_t_fact_duetoduefrom') }}
	where [BUKRS] = 'REC'
),


UNIONIZED_DATA AS (
	select 
		company
		,business_area_description
		,business_area
		,posting_year_month = LEFT(posting_year_month,7)
		,bs_mapping
		,general_ledger_account
		,main_account = LEFT(general_ledger_account,3)
		,main_account_from_mapping = bm.main_account
		,bm.vuk_account_name
		,customer_vendor_code
		,running_total
		,currency
		,try_value
		,eur_value
		,bs_mapping_1 = CASE 
							WHEN [source] = 'SAP' THEN bm.bs_mapping_1
							WHEN [source] = 'CF' THEN 'Current Assets'
							ELSE NULL 
						END
		,bs_mapping_2 = CASE 
							WHEN [source] = 'SAP' THEN bm.bs_mapping_2
							WHEN [source] = 'CF' THEN 'Cash and Cash Equivalents'
							ELSE NULL 
						END
		,[rank] = CASE WHEN [source] = 'CF' THEN '1' ELSE bm.[rank] END
		,[source]
	from {{ ref('stg__nwc_kpi_t_fact_cumulativebalancesheetreport') }} cbs
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_balancesheetmapping') }} bm ON CAST(bm.account AS NVARCHAR) = cbs.bs_mapping

	union all

	select *
	from adjusted_due_to_due_from
)

select
	rls_region   = kuc.RegionCode 
	,rls_group   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,rls_company = CONCAT(COALESCE(company ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,rls_businessarea = CONCAT(COALESCE(business_area,''),'_',COALESCE(kuc.RegionCode,''))
	,company
	,business_area_description
	,business_area
	,posting_year_month = LEFT(posting_year_month,7)
	,bs_mapping
	,general_ledger_account
	,main_account = LEFT(general_ledger_account,3)
	,main_account_from_mapping
	,vuk_account_name
	,customer_vendor_code
	,customer_vendor_name = CASE WHEN kna1.kunnr IS NULL THEN lfa1.name1 ELSE kna1.name1 END
	,running_total
	,currency
	,try_value
	,eur_value
	,bs_mapping_1 = CASE WHEN [source] <> 'CF' AND (main_account >= 600 AND main_account <= 799) THEN 'Shareholders'' Equity' WHEN [source] <> 'CF' AND bs_mapping_1 IS NULL THEN 'Unmapped Accounts' ELSE bs_mapping_1 END
	,bs_mapping_2 = CASE WHEN [source] <> 'CF' AND (main_account >= 600 AND main_account <= 799) THEN 'Net Profit/(Loss) for the Period' WHEN [source] <> 'CF' AND bs_mapping_2 IS NULL THEN 'Unmapped Accounts' ELSE bs_mapping_2 END
	,[rank] = CASE WHEN [source] <> 'CF' AND (main_account >= 600 AND main_account <= 799) THEN '999' ELSE [rank] END
	,[source]
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
from UNIONIZED_DATA ud
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} kuc ON ud.company = kuc.RobiKisaKod
	LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_kna1') }} kna1 ON ud.customer_vendor_code = kna1.kunnr
	LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_lfa1') }} lfa1 ON ud.customer_vendor_code = lfa1.lifnr
where company IS NOT NULL
	and running_total <> 0


