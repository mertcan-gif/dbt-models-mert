{{
  config(
    materialized = 'table',tags = ['nwc_kpi','cashflow']
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
		Left(RACCT, 3) AS general_ledger,
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

			/** RET H067 özelinde ek koşul eklendi, RET CF özelindeki sorgu ve adjustment dosyası bağlantısı tamamlandığında buradan ek koşul silinecek **/
/*8*/		AND (acdoca.GKONT <> '1029999999' OR (acdoca.RBUSA = 'H067' AND acdoca.GKONT = '1029999999' AND acdoca.RACCT = '1029999999'))
	
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
		fiscal_year = cf_adjusted1.gjahr,
		general_ledger_account = cf_adjusted1.racct,
		business_area = cf_adjusted1.rbusa,
		business_area_description = tgsbt.gtext,
		posting_date = cf_adjusted1.budat,
		document_number = cf_adjusted1.belnr,
		company = cf_adjusted1.rbukrs,
		document_date = cf_adjusted1.bldat,
		entry_date = cf_adjusted1.cpudt,
		document_line_item = cf_adjusted1.buzei,
		document_type = cf_adjusted1.blart,
		offsetting_account_number = cf_adjusted1.gkont,
		item_text = cf_adjusted1.sgtxt,
		cf_adjusted1.general_ledger,
		[amount_in_document_currency] = 
                    CASE
						  WHEN tcurx.currdec = 3 THEN cf_adjusted1.[amount_in_document_currency]/10 
					  ELSE cf_adjusted1.[amount_in_document_currency] END,
		document_currency = cf_adjusted1.rwcur,
		amount_transaction_currency = cf_adjusted1.rtcur,
		amount_in_tl = cf_adjusted1.[amount_try],
		amount_in_usd = cf_adjusted1.[amount_usd],
		amount_in_eur = cf_adjusted1.[amount_eur],
		amount_notification = cf_adjusted1.[amount_teblig],
		category = cf_type,
		cf_adjusted1.[financial_item] as commitment_item,
		fi.[group],
		[first_definition] = fi.description_1,
		[definition] = fi.description_2,
		account_code,
		account_name,
		xreversing,
		xreversed,
		reverse_document_number = stblg,
		username = usnam
	FROM cf_adjusted1
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tgsbt') }} tgsbt ON cf_adjusted1.RBUSA = tgsbt.GSBER
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_financialitems') }} fi 
                                                                            ON cf_adjusted1.[mali kalem2] = fi.financial_item
			                                                                AND cf_adjusted1.belgn = fi.document_1
		LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tcurx') }} tcurx ON cf_adjusted1.rwcur = tcurx.CURRKEY
	WHERE 1=1
			AND cf_adjusted1.a <> 'B'
			AND tgsbt.spras = 'TR'
)		

,[cf_adjusted3] AS 
(
SELECT 
		[cf_adjusted2].fiscal_year
		,[cf_adjusted2].general_ledger_account
		,[cf_adjusted2].business_area
		,[cf_adjusted2].business_area_description
		,[cf_adjusted2].posting_date
		,[cf_adjusted2].document_number
		,[cf_adjusted2].company
		,[cf_adjusted2].document_date
		,[cf_adjusted2].entry_date
		,[cf_adjusted2].document_line_item
		,[cf_adjusted2].document_type
		,[cf_adjusted2].offsetting_account_number
		,[cf_adjusted2].item_text
		,[cf_adjusted2].general_ledger
		,[amount_in_document_currency] = CASE WHEN yeka.document_type IN ('S3','UE') THEN [cf_adjusted2].amount_in_document_currency * -1 ELSE [cf_adjusted2].amount_in_document_currency END 
		,[cf_adjusted2].document_currency
		,[cf_adjusted2].amount_transaction_currency
		,amount_in_tl = CASE WHEN yeka.document_type IN ('S3','UE') THEN [cf_adjusted2].amount_in_tl * -1 ELSE [cf_adjusted2].amount_in_tl END 
		,amount_in_usd = CASE WHEN yeka.document_type IN ('S3','UE') THEN [cf_adjusted2].amount_in_usd * -1 ELSE [cf_adjusted2].amount_in_usd END 
		,amount_in_eur = CASE WHEN yeka.document_type IN ('S3','UE') THEN [cf_adjusted2].amount_in_eur * -1 ELSE [cf_adjusted2].amount_in_eur END 
		,amount_notification = CASE WHEN yeka.document_type IN ('S3','UE') THEN [cf_adjusted2].amount_notification * -1 ELSE [cf_adjusted2].amount_notification END 
		,[cf_adjusted2].category
		,[cf_adjusted2].[commitment_item]
		,[cf_adjusted2].[group]
		,[cf_adjusted2].[first_definition]
		,[cf_adjusted2].[definition]
		,[cf_adjusted2].account_code
		,[cf_adjusted2].account_name
		,[cf_adjusted2].xreversing
		,[cf_adjusted2].xreversed
		,[cf_adjusted2].reverse_document_number
		,[cf_adjusted2].username
		,CASE -- Burada rtivendorlist excelinde yeni type kolonu açılıp "Gelir" denebilir. Farklı vendorlistler bir cte'de birleştirilerek tek joinle bağlanabilir.
			WHEN [cf_adjusted2].document_type='S3' AND LEFT([cf_adjusted2].document_number,2)='37' AND [cf_adjusted2].company='RET' AND [cf_adjusted2].business_area IN ('H067', 'H068') THEN 'Fonlama'
			WHEN [cf_adjusted2].company = 'RET' AND [cf_adjusted2].business_area IN ('H068','H067') AND [cf_adjusted2].commitment_item = '642100' THEN 'Faiz'
            WHEN rec.[business_area] IS NULL AND yeka.business_area IS NULL THEN 'Gider'
            WHEN rec.[business_area] IS NOT NULL AND [cf_adjusted2].document_type='S1' AND account_code='NOY' AND [cf_adjusted2].company='REC' AND [cf_adjusted2].business_area='R003' THEN 'Gider'
            WHEN rec.[business_area] IS NOT NULL THEN 'Gelir'
            WHEN yeka.[business_area] IS NOT NULL THEN yeka.[type]
        END AS [type]
FROM  [cf_adjusted2]
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_rtivendorlist') }} rec
			ON [cf_adjusted2].account_code = rec.[s4_customer_code]
			AND [cf_adjusted2].company = rec.company
			AND [cf_adjusted2].business_area = rec.business_area
		LEFT JOIN {{ source('stg_sharepoint', 'raw__nwc_kpi_t_dim_yekavendorlist') }} yeka
			-- ON [cf_adjusted2].account_code = yeka.[s4_customer_code]
			ON (CASE 
				WHEN yeka.document_type IN ('S3','UE') AND [cf_adjusted2].offsetting_account_number = yeka.[s4_customer_code] THEN 1 
				WHEN [cf_adjusted2].account_code = yeka.[s4_customer_code] THEN 1 
			ELSE 0 END = 1)
			AND [cf_adjusted2].company = yeka.company
			AND [cf_adjusted2].business_area = yeka.business_area
			AND [cf_adjusted2].document_type = yeka.document_type
			
)

select *
from cf_adjusted3