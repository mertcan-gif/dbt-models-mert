{{
  config(
    materialized = 'table',tags = ['nwc_kpi_draft','balancesheet_draft']
    )
}}

/** Değerleme müşteri ve değerleme satıcı kolonları herhangi bir tabloda tutulmuyor.
	SAP'de belirli kurallar sonucu ulaşılabiliyor.
	SAP'de standart olarak değerleme ters kayıtları SGTXT kolonuna;
		"BELNR PERIOD değerleme Ters kayıt"
	formatında yansıtılıyor. 
	Buradaki BELNR text'ten çekilerek, ilgili şirket ve iş alanı filtresi eklendiğinde karşımıza çıkan kaydın LIFNR ve KUNNR kolonları
	değerleme satıcı ve değerleme müşteri değerlerini bize gösterir 
	
	Bu tablo, tüm acdoca tablosunu kendisi ile joinlemek yerine filtrelenmiş bir çıktıyı joinlemek için oluşturulmuştur**/

with valuation_records as (
select distinct
	counter_belnr = case when left(sgtxt,1) <> '0' then left(sgtxt,10) else right(left(sgtxt,10),9) end 
	,racct
	,rbukrs
	,rbusa
	,sgtxt
from {{ ref('stg__s4hana_t_sap_acdoca') }} 
													
where 1=1
	-- and RIGHT(racct,1) = '9'
	and RIGHT(sgtxt,9) = N'değerleme'
)

select
	a1.rbukrs
	,a1.rbusa
	,a1.gjahr
	,a1.belnr
	,a1.racct
	,a1.lifnr
	,a1.kunnr
from {{ ref('stg__s4hana_t_sap_acdoca') }} a1
	right join valuation_records vr ON vr.counter_belnr = a1.belnr and vr.rbukrs = a1.rbukrs and vr.rbusa = a1.rbusa										
where 1=1 
	and a1.koart = 'D'
	and a1.blart = 'UE'