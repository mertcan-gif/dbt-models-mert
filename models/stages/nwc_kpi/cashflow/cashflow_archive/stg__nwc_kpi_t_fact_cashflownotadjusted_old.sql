{{
  config(
    materialized = 'table',tags = ['nwc_kpi_old','cashflow_old']
    )
}}

WITH cf_base AS 
(
/* 
 1 => Anahesap 102 ve Belge Türü = 'SK'										| Dahil Et
 2 => Anahesap 102 ve Belge Türü = 'UE'										| Dahil Et
 3 => Anahesap 102 ve Karşıt Hesap 102 olan Belge Türü ('S1','S2') olanlar  | Dahil Et
 4 => Anahesap 102 ve Belge Türü = 'S1'										| Dahil Etme
 5 => Anahesap 102 ve Belge Türü = 'S2'										| Dahil Etme
 6 => Belge Türü ('S1','S2','S3') olmayanları								| Dahil Etme
 7 => Geri kalanları														| Dahil Et
 8 => GKONT'u 1029999999 olan hesaplar filtrelenmiştir
*/
	SELECT
		acdoca.gjahr,
		acdoca.racct,
		acdoca.rbusa,
		budat = CAST(acdoca.budat AS DATE),
		acdoca.belnr,
		acdoca.rbukrs,
		bldat = CAST(acdoca.bldat AS DATE),
		acdoca.buzei,
		acdoca.blart,
		acdoca.gkont,
		acdoca.sgtxt,
		bkpf.cpudt,
		bkpf.xreversing,
		bkpf.xreversed,
		bkpf.stblg,
		bkpf.usnam,
		Left(RACCT, 3) AS ledger_account,
		CASE WHEN acdoca.bldat =  '20221231' AND acdoca.blart = 'UE' THEN WSL ELSE -1*wsl END AS [amount_in_document_currency],
		acdoca.rwcur,
		acdoca.rtcur,
		CASE WHEN acdoca.bldat =  '20221231' AND acdoca.blart = 'UE' THEN HSL ELSE -1*hsl END AS [amount_try],
		CASE WHEN acdoca.bldat =  '20221231' AND acdoca.blart = 'UE' THEN OSL ELSE -1*osl END  AS [amount_usd],
		CASE WHEN acdoca.bldat =  '20221231' AND acdoca.blart = 'UE' THEN KSL ELSE -1*ksl END  AS [amount_eur],
		CASE WHEN acdoca.bldat =  '20221231' AND acdoca.blart = 'UE' THEN TSL ELSE -1*tsl END  AS [amount_teblig],
		CASE 
/*1*/			WHEN Left(racct, 3) + acdoca.blart = '102SK' THEN 'A'   
/*2*/			WHEN Left(racct, 3) + acdoca.blart = '102UE' THEN 'A'
/*3*/			WHEN Left(racct, 3) = '102' AND Left(gkont, 3) = '102' AND LEN(gkont) = 10 AND acdoca.blart IN ('S1','S2') THEN 'A'
/*4*/			WHEN Left(racct, 3) + acdoca.blart = '102S1' THEN 'B'
/*5*/			WHEN Left(racct, 3) + acdoca.blart = '102S2' THEN 'B'
				WHEN Left(racct, 3) = '102' AND acdoca.blart = 'C1' AND LEFT(gkont,3) = '100' THEN 'B'
				WHEN Left(racct, 3) = '102' AND acdoca.blart = 'C1' THEN 'A'
/*6*/			WHEN acdoca.blart NOT IN ('S1','S2','S3','SK','C1') THEN 'B'
				WHEN acdoca.BLART = 'S3' AND Left(gkont, 3) <> '102' THEN 'B'
/*7*/			ELSE 'A'
		END AS a,
		Left(acdoca.belnr, 2) AS belgn,
		IIf(LFA1.lifnr <> '',LFA1.lifnr,IIf(KNA1.kunnr <> '', KNA1.kunnr, '')) AS account_code,
		IIf(LFA1.name1 <> '', LFA1.name1, IIf(KNA1.name1 <> '', KNA1.name1, '')) AS account_name,
		IIf(
			acdoca.blart='S1',
			'NAKİT ÇIKIŞ', 
			IIf(acdoca.blart='S2', 
			'NAKİT GİRİŞ', 
				IIf(acdoca.blart='S3', 'VİRMAN', 
					IIf(acdoca.blart='SK', 'KUR FARKI', 'DİĞER')))) AS cf_type
	FROM {{ ref('stg__s4hana_t_sap_acdoca') }} AS acdoca
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_kna1') }} kna1 ON acdoca.kunnr = kna1.kunnr
			LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_lfa1') }} lfa1 ON acdoca.lifnr = lfa1.lifnr
			LEFT JOIN {{ ref('stg__s4hana_t_sap_bkpf') }} bkpf ON acdoca.rbukrs = bkpf.bukrs
									AND acdoca.belnr = bkpf.belnr
									AND acdoca.gjahr = bkpf.gjahr
	WHERE 1=1
			AND buzei <> '000'
			AND acdoca.blart IN ('SK','S1','S2','S3','UE','C1')
			-- AND	bkpf.xreversing = 0
			-- AND bkpf.xreversed = 0		 
			AND LEFT(acdoca.racct, 3) <> '900'	
			AND LEFT(acdoca.racct, 3) <> '901' 
			AND LEFT(acdoca.racct, 3) <> '899'
/*8*/		AND acdoca.GKONT <> '1029999999'
	
) 
,cf_adjusted1 AS (
	SELECT *,
			IIf(
			[financial_item] <> '',
			[financial_item],
			IIf(Left(RACCT, 3) = '102', '102000', [financial_item])
		) AS [mali kalem2]
	FROM cf_base		
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_mainaccounts') }} a_hesaplar ON cf_base.RACCT = a_hesaplar.main_account

)
,[cf_adjusted2] AS
(
	SELECT
		cf_adjusted1.gjahr,
		cf_adjusted1.racct,
		cf_adjusted1.rbusa,
		tgsbt.gtext,
		cf_adjusted1.budat,
		cf_adjusted1.belnr,
		cf_adjusted1.rbukrs,
		cf_adjusted1.bldat,
		cf_adjusted1.cpudt,
		cf_adjusted1.buzei,
		cf_adjusted1.blart,
		cf_adjusted1.gkont,
		cf_adjusted1.sgtxt,
		cf_adjusted1.ledger_account,
		[amount_in_document_currency] = 
                    CASE
						  WHEN tcurx.currdec = 3 THEN cf_adjusted1.[amount_in_document_currency]/10 
					  ELSE cf_adjusted1.[amount_in_document_currency] END,
		cf_adjusted1.rwcur,
		cf_adjusted1.rtcur,
		cf_adjusted1.[amount_try],
		cf_adjusted1.[amount_usd],
		cf_adjusted1.[amount_eur],
		cf_adjusted1.[amount_teblig],
		cf_type,
		cf_adjusted1.[financial_item] as commitment_item,
		fi.[group],
		fi.description_1,
		fi.description_2 as [description],
		account_code,
		account_name,
		xreversing,
		xreversed,
		stblg,
		usnam
	FROM cf_adjusted1
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tgsbt') }} tgsbt ON cf_adjusted1.RBUSA = tgsbt.GSBER
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_financialitems') }} fi 
                                                                            ON cf_adjusted1.[mali kalem2] = fi.financial_item
			                                                                AND cf_adjusted1.belgn = fi.document_1
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON cf_adjusted1.RWCUR = tcurx.CURRKEY
	WHERE 1=1
			AND cf_adjusted1.a <> 'B'
			AND tgsbt.spras = 'TR'
)		

,[cf_adjusted3] AS 
(
SELECT [cf_adjusted2].*,
	   CASE WHEN rec.[business_area] IS NULL THEN 'Gider' ELSE 'Gelir' END AS Tipi
FROM  [cf_adjusted2]
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtivendorlist') }} rec
			ON [cf_adjusted2].account_code = rec.[s4_customer_code]
			AND [cf_adjusted2].rbukrs = rec.company
			AND [cf_adjusted2].rbusa = rec.business_area
			
)

select *
from cf_adjusted3