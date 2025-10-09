
{{
  config(
    materialized = 'table',tags = ['monthlyreport_draft']
    )
}}

with adjusted_due_to_due_from as (

	select
		  rbukrs = [BUKRS]
		  ,business_area = BUSINESS_AREA
		  ,business_area_code = gsber
		  ,yearmonth = LEFT(starting_date_of_month,7)
		  ,nwc_mapping = [due_type]
		  ,totalamount = [due]
		  ,currency
		  ,try_value
		  ,eur_value
	from {{ ref('dm__nwc_kpi_t_fact_duetoduefrom') }}

	union all
	/* -1 ile çarpılmış hallerini Non-NWC PL'ye atan kısım*/
	select
		  rbukrs= [BUKRS]
		  ,business_area = BUSINESS_AREA
		  ,business_area_code = gsber
		  ,yearmonth = LEFT(starting_date_of_month,7)
		  ,nwc_mapping = 'Non-NWC PL'
		  ,totalamount = [due]*-1
		  ,currency
		  ,try_value
		  ,eur_value
	from {{ ref('dm__nwc_kpi_t_fact_duetoduefrom') }}

),


UNIONIZED_DATA AS (
	select 
		rbukrs
		,business_area
		,business_area_code
		,posting_year_month = LEFT(posting_year_month,7)
		,nwc_mapping
		,running_total
		,currency
		,try_value
		,eur_value
	from {{ ref('stg__nwc_kpi_t_fact_cumulativemonthlyreport') }}

	union all

	select *
	from {{ ref('stg__nwc_kpi_t_fact_monthlynwcadjusted') }}

	union all

	select *
	from {{ ref('stg__nwc_kpi_t_fact_monthlynwcbngroup') }}

	union all

	select *
	from {{ ref('stg__nwc_kpi_t_fact_monthlynwchkpgroup') }}

	union all

	select *
	from {{ ref('stg__nwc_kpi_t_fact_monthlynwcretgroup') }}

	union all

	select *
	from adjusted_due_to_due_from
)

select
	rls_region   = kuc.RegionCode 
	,rls_group   = CONCAT(COALESCE(kuc.KyribaGrup,''),'_',COALESCE(kuc.RegionCode,''))
	,rls_company = CONCAT(COALESCE(RBUKRS collate database_default ,''),'_'	,COALESCE(kuc.RegionCode,''),'')
	,rls_businessarea = CONCAT(COALESCE(business_area_code,''),'_',COALESCE(kuc.RegionCode,''))
	,company = rbukrs
	,business_area_description = business_area
	,business_area = business_area_code
	,posting_year_month = LEFT(posting_year_month,7)
	,nwc_mapping
	,running_total
	,currency
	,try_value
	,eur_value
	,kyriba_group = kuc.KyribaGrup
	,kyriba_company_code = kuc.KyribaKisaKod
from UNIONIZED_DATA ud
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} kuc ON ud.rbukrs = kuc.RobiKisaKod
where rbukrs IS NOT NULL
		and posting_year_month >= '2023-08'