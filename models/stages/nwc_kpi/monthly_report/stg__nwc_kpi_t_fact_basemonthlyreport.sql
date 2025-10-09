{{
  config(
    materialized = 'table',tags = ['nwc_kpi','monthlyreport']
    )
}}

WITH raw_data AS (
/****
hesap numarası 3lü değilde çok haneli olanları otomatik olarak buldurup IN sorgusunun içine yazıyorum, kalanları 3lü kod ile mapliyorum.
****/

	select 
		acdoca.rbukrs,
		rbusa,
		kunnr,
		lifnr,
		case
			when LEFT(racct,3) IN (
						select DISTINCT	LEFT(hesap,3) AS Hesap
							from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_nwcaccountsmapping') }} 
							WHERE LEN(hesap)>3
			) THEN racct
			ELSE LEFT(racct,3)
		END AS nwc_mapping ,
		CAST(budat AS DATE) AS PostingDate,
		CAST(bldat AS DATE) AS DocumentDate,
		hsl_adjusted = case when tcurx.currdec = 3 THEN hsl/10 ELSE hsl END,
		zuonr
		
	from {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca 
		LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }} t001 ON acdoca.rbukrs = t001.bukrs
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON t001.waers = tcurx.currkey 
	where 1=1
		and rbukrs NOT IN (select RobiKisaKod from aws_stage.dimensions.raw__dwh_t_dim_companymapping where KyribaGrup = 'RETGROUP')
		and LEFT(racct,3) <> '193'
		and (LEFT(RIGHT(acdoca.fiscyearper,6),2) not in ('13','14','15','16','00') OR LEFT(RIGHT(acdoca.fiscyearper,6),2) is null)
		and (acdoca.blart <> 'IA' OR acdoca.blart IS NULL)

union all 

/***
193'le başlayan hesaplarda 14. ve 15. periyotlar da hesaplamaya dahil edildiğinden ayrı bir sorguda aşağıdaki gibi unionlanmıştır
***/

	select 
		acdoca.rbukrs,
		rbusa,
		kunnr,
		lifnr,
		case
			when LEFT(racct,3) IN (
						select DISTINCT	LEFT(hesap,3) AS Hesap
							from {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_nwcaccountsmapping') }} 
							WHERE LEN(hesap)>3
			) THEN racct
			ELSE LEFT(racct,3)
		END AS nwc_mapping ,
		CAST(budat AS DATE) AS PostingDate,
		CAST(bldat AS DATE) AS DocumentDate,
		hsl_adjusted = case when tcurx.currdec = 3 THEN hsl/10 ELSE hsl END,
		zuonr
		
	from {{ ref('stg__s4hana_t_sap_acdoca') }} acdoca 
		LEFT JOIN {{ ref('vw__s4hana_v_sap_ug_t001') }} t001 ON acdoca.rbukrs = t001.bukrs
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON t001.waers = tcurx.currkey 
	where 1=1
		and rbukrs NOT IN (select RobiKisaKod from aws_stage.dimensions.raw__dwh_t_dim_companymapping where KyribaGrup = 'RETGROUP')
		and LEFT(racct,3) = '193'
		and (LEFT(RIGHT(acdoca.fiscyearper,6),2) not in ('13','16','00') OR LEFT(RIGHT(acdoca.fiscyearper,6),2) is null)
		and (acdoca.blart <> 'IA' OR acdoca.blart IS NULL)
)

select 
	rbukrs
	,business_area_code = acd.rbusa
	,business_area = t001w.name1 
	,COALESCE(
		case	
			when (acd.nwc_mapping = '1810102010' and zuonr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) THEN 'Non-NWC Asset'
			when (acd.nwc_mapping = '3810401014' and zuonr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) THEN 'Non-NWC Liability'
			when (left(acd.nwc_mapping,3) = '120' and (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) and (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')
													or
												acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL'))
														) Then 'Non-NWC Asset'
			when (left(acd.nwc_mapping,3) = '320' and (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) and (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')
													or
												acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL'))
														) Then 'Non-NWC Liability'																				
			when acd.nwc_mapping IN ('120') AND vl.business_area IS NOT NULL THEN 'Contract Receivables'
			when acd.nwc_mapping IN ('126') AND vl.business_area IS NOT NULL THEN 'Retention/Bonus Receivables'
			ELSE nwc.nwc_mapping END
		,'Non-NWC PL') AS nwc_mapping
	,EOMONTH(PostingDate) posting_year_month
	,sum_hsl = CASE WHEN rbukrs = 'AKO' THEN sum(hsl_adjusted)/2 ELSE sum(hsl_adjusted) END
	,null as wsl
from raw_data acd 
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_nwcaccountsmapping') }} nwc ON nwc.hesap = acd.nwc_mapping
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_t001w') }} t001w ON acd.rbusa = t001w.WERKS
	LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtivendorlist') }} vl ON 
												acd.rbusa = vl.business_area
												AND acd.kunnr = vl.s4_customer_code
WHERE 1=1
	/** 
		Murat Seri Bey'in iletmiş olduğu; 
		"120 , 320 , 159, 340 , 136 , 336 hesaplarda ilgili şirketlerin kendi aralarındaki borç / alacak ilişkisinin raporda hariç tutulması"
		filtresidir.
	**/
	AND NOT (left(acd.nwc_mapping,3) IN ('120','320','159','340','136','336') 
				AND (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) 
				AND (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')
					 OR acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')))
GROUP BY 
	rbukrs 
	,acd.rbusa
	,t001w.NAME1
	,COALESCE(
		case	
			when (acd.nwc_mapping = '1810102010' and zuonr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) THEN 'Non-NWC Asset'
			when (acd.nwc_mapping = '3810401014' and zuonr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) THEN 'Non-NWC Liability'
			when (left(acd.nwc_mapping,3) = '120' and (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) and (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')
													or
												acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL'))
														) Then 'Non-NWC Asset'
			when (left(acd.nwc_mapping,3) = '320' and (acd.rbukrs in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')) and (acd.lifnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL')
													or
												acd.kunnr in ('TUA','AVA','RES','FIL','HOS','ALC','REN','REH','ASY','SEG','MAM','CKR','SBL'))
														) Then 'Non-NWC Liability'																				
			when acd.nwc_mapping IN ('120') AND vl.business_area IS NOT NULL THEN 'Contract Receivables'
			when acd.nwc_mapping IN ('126') AND vl.business_area IS NOT NULL THEN 'Retention/Bonus Receivables'
			ELSE nwc.nwc_mapping END
		,'Non-NWC PL')
	,EOMONTH(PostingDate)

UNION ALL

SELECT * FROM {{ ref('stg__nwc_kpi_t_fact_monthlybonusreceivables') }}