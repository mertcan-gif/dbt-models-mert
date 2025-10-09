{{
  config(
    materialized = 'table',tags = ['fi_kpi','rmore']
    )
}}

/*
	BLART kolonu UE olmayanlar için filtrelendiğinde SK olan belge türleri de gelmektedir.
	Ancak diğer tarif olan BLART'I S1, S2 ve S3 olan değerleri aldığımızda SK belge türleri gelmeyeceği için arada fark oluşturmaktadır.
*/

select 
	comp.rls_region
	,comp.[rls_group]  
	,comp.rls_company
	,rls_businessarea = CONCAT('_', comp.rls_region)
	,acdoca.rbukrs
	,acdoca.gjahr
	,acdoca.belnr
	,acdoca.rrcty
	,acdoca.bstat
	,acdoca.vorgn
	,acdoca.rwcur
	,acdoca.rtcur
	,acdoca.racct
	,account_descriptions.txt50 as racct_description
	,LEFT(acdoca.racct,1) as racct_first
	,LEFT(acdoca.racct,2) as racct_first_two
	,LEFT(acdoca.racct,3) as racct_first_three
	,acdoca.rcntr
	,acdoca.rbusa
	,acdoca.tsl
	,acdoca.wsl
	,acdoca.hsl
	,acdoca.ksl
	,acdoca.osl
	,acdoca.msl
	,acdoca.fiscyearper
	,acdoca.blart as blart
	,cast(acdoca.budat as date) as budat
	,cast(acdoca.bldat as date) as bldat
	,cast(bkpf.cpudt as date) as cpudt
	,cast(acdoca.netdt as date) as netdt
	,cast(acdoca.augdt as date) as augdt
	,acdoca.buzei
	,acdoca.sgtxt
	,acdoca.matnr
	,acdoca.lifnr
	,acdoca.kunnr
	,acdoca.augbl
	,acdoca.gkont
	,acdoca.aufnr
	,acdoca.psposid
	,acdoca.fipex
	,acdoca.fistl
	,acdoca.zuonr
	,acdoca.ebeln
	,acdoca.rfarea
	,acdoca.ebelp
	,acdoca.umskz
	,acdoca.drcrk
	,acdoca.koart
	,acdoca.fcsl
	,acdoca.docln
	,acdoca.mwskz
	,acdoca.gkoar
	,acdoca.afabe
	,acdoca.db_upload_timestamp
	,bkpf.xreversing
	,bkpf.xreversed
	,bkpf.stblg
	,bkpf.awkey
	,bkpf.usnam
	,bkpf.bktxt
	,bkpf.tcode
	,bkpf.xblnr
from {{ ref('stg__s4hana_t_sap_acdoca_full') }} acdoca
	LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acdoca.belnr = bkpf.belnr
										AND acdoca.rbukrs = bkpf.bukrs
										AND acdoca.gjahr = bkpf.gjahr
	LEFT JOIN {{ ref('dm__dimensions_t_dim_companies') }} comp ON acdoca.rbukrs = comp.RobiKisaKod
	LEFT JOIN ( 
		SELECT saknr,txt50
		FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_skat') }}
				where ktopl = 'RONS' AND spras = 'T'
  ) account_descriptions on acdoca.racct = account_descriptions.saknr
where 1=1
	and acdoca.rbukrs = 'HOL'
	and bkpf.xreversed = 0
	and bkpf.xreversing = 0



