{{
  config(
    materialized = 'table',tags = ['nwc_kpi','balancesheet']
    )
}}

WITH raw_data AS (
/****
hesap numarası 3lü değil de çok haneli olanları otomatik olarak buldurup IN sorgusunun içine yazıyorum, kalanları 3lü kod ile mapliyorum.
****/

	select 
		company = acdoca.rbukrs,
		business_area = rbusa,
		customer_vendor_code = CASE WHEN (lifnr IS NULL OR lifnr = '') THEN kunnr ELSE lifnr END,
		general_ledger_account = racct,
		bs_mapping = CASE
						WHEN LEFT (racct,3) IN (
										SELECT DISTINCT	LEFT(account,3) AS Hesap
										FROM "aws_stage"."sharepoint"."raw__nwc_kpi_t_dim_balancesheetmapping"
											WHERE LEN(account)>3
								) THEN racct
								ELSE LEFT(racct,3)
					END,
		posting_date = CAST(budat AS DATE),
		document_date = CAST(bldat AS DATE),
		amount_in_company_currency = case when tcurx.currdec = 3 THEN hsl/10 ELSE hsl END,
		zuonr
		
	from {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca 
		LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }} t001 ON acdoca.rbukrs = t001.bukrs
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON t001.waers = tcurx.currkey 
	where 1=1
		and rbukrs = 'REC'
		and rbukrs NOT IN (select RobiKisaKod from {{ source('stg_dimensions', 'raw__dwh_t_dim_companymapping') }} where KyribaGrup = 'RETGROUP')
		and (LEFT(racct,3) IN ('193','295','371') OR (LEFT(RIGHT(acdoca.fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(acdoca.fiscyearper,6),2) is null))
		and (acdoca.blart <> 'IA' OR acdoca.blart IS NULL)
		and (acdoca.afabe <> '03' OR acdoca.afabe IS NULL)
)

select 
	company
	,acd.business_area
	,business_area_description = t001w.name1 
	,customer_vendor_code
	,general_ledger_account
	{# ,COALESCE(
		case	
			when (acd.nwc_mapping = '1810102010' and zuonr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')) THEN 'Non-NWC Asset'
			when (acd.nwc_mapping = '3810401014' and zuonr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')) THEN 'Non-NWC Liability'
			when (left(acd.nwc_mapping,3) = '120' and (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')) and (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')
													or
												acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY'))
														) Then 'Non-NWC Asset'
			when (left(acd.nwc_mapping,3) = '320' and (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')) and (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')
													or
												acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY'))
														) Then 'Non-NWC Liability'																				
			when acd.nwc_mapping IN ('120') AND vl.business_area IS NOT NULL THEN 'Contract Receivables'
			when acd.nwc_mapping IN ('126') AND vl.business_area IS NOT NULL THEN 'Retention/Bonus Receivables'
			ELSE nwc.nwc_mapping END
		,'Non-NWC PL') AS nwc_mapping #}
	,bs_mapping
	,posting_year_month = EOMONTH(posting_date) 
	,total_amount_in_company_currency = sum(amount_in_company_currency)
	,[source] = 'SAP'
FROM raw_data acd 
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_balancesheetmapping') }} bsm ON bsm.account = acd.bs_mapping
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acd.business_area = t001w.WERKS
	{# LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtivendorlist') }} vl ON 
												acd.business_area = vl.business_area
												AND acd.customer_code = vl.s4_customer_code #}
WHERE 1=1
	AND (bsm.bs_mapping_1 <> N'Raporda olmamalı' OR bsm.bs_mapping_1 IS NULL)
GROUP BY 
	company 
	,acd.business_area
	,t001w.NAME1
	,customer_vendor_code
	,general_ledger_account
	{# ,COALESCE(
		case	
			when (acd.nwc_mapping = '1810102010' and zuonr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')) THEN 'Non-NWC Asset'
			when (acd.nwc_mapping = '3810401014' and zuonr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')) THEN 'Non-NWC Liability'
			when (left(acd.nwc_mapping,3) = '120' and (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')) and (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')
													or
												acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY'))
														) Then 'Non-NWC Asset'
			when (left(acd.nwc_mapping,3) = '320' and (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')) and (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY')
													or
												acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY'))
														) Then 'Non-NWC Liability'																				
			when acd.nwc_mapping IN ('120') AND vl.business_area IS NOT NULL THEN 'Contract Receivables'
			when acd.nwc_mapping IN ('126') AND vl.business_area IS NOT NULL THEN 'Retention/Bonus Receivables'
			ELSE nwc.nwc_mapping END
		,'Non-NWC PL') #}
	,bs_mapping
	,EOMONTH(posting_date)

{# UNION ALL

SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_monthlybonusreceivables') }} #}

UNION ALL

select 
	company
	,business_area
	,business_area_description 
	,CAST(account_code as nvarchar)
	,general_ledger_account
	,bs_mapping = ''
	,posting_year_month = EOMONTH(posting_date) 
	,total_amount_in_company_currency = sum(amount_in_tl)
	,[source] = 'CF'
FROM {{ ref('dm__nwc_kpi_t_fact_cashflow') }} cf 
WHERE 1=1
	AND company = 'REC'
	AND ([type] <> 'TEMETTÜ' OR [type] IS NULL)
	-- AND 
	-- 	 ([type] <> 'KAR'
	-- 		AND [type] <> N'KAR'
	-- 		AND [type] <> N'TEMETTÜ'
	-- 		AND [type] <> N'FAİZ NPV'
	-- 		AND [type] <> N'FAİZ REEL'
	-- 		AND [type] <> N'KDV DÜZELTME')
	-- 	 OR [type] IS NULL)
GROUP BY 
	company
	,business_area
	,business_area_description 
	,account_code
	,general_ledger_account
	,EOMONTH(posting_date)
