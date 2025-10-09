{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}

----- Tüm verileri içeren ancak sadece transaction detayında içeren tabloyu çekiyorum
select 
	   'S4HANA' AS id
	  ,CONVERT(DATETIME, TransactionDate, 104) AS creation_time
	  ,CASE 
	  	WHEN users.KullaniciAdi IS NOT NULL THEN lower(users.KullaniciEmail)
		ELSE CONCAT(lower(SapId),'@emailnotfound')
		END  AS [user_id] --Numandan Gelecek
	  ,'S4HANA' as [workspace_id]
	  ,[TransactionCode] as [report_id]
	  ,'S4HANA' AS [report_type]
	  ,'S4HANA' AS [consumption_method]
	  ,cast([TransactionCount] as  int) as transaction_amount
from {{ ref('stg__s4_kpi_t_fact_sapuserlogs') }} usg
	LEFT JOIN (select * from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tstct') }} where sprsl = 'T' ) tstct ON usg.[TransactionCode] = tstct.tcode
	LEFT JOIN (
		SELECT *
		FROM (
			SELECT *,ROW_NUMBER() OVER(PARTITION BY KullaniciAdi order by KullaniciAdi) _partition
			FROM (
				SELECT DISTINCT KullaniciEmail,KullaniciAdi
				FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_reportusages') }} usg
				where 1=1 
					and KullaniciBlokajDurumu = 0
					and KullaniciTipi = 'A'
					and LEFT(KullaniciAdi,2) <> 'D_'
					and KullaniciEmail <> ''
					AND KullaniciEmail NOT LIKE '%test%'
			) usr
		) usr2
		where _partition = 1
	
	) users on users.KullaniciAdi = usg.SapId
WHERE 1=1
	and (tstct.ttext IS NOT NULL or [TransactionCode] LIKE '%SNI%')
	and [TransactionCode] <> 'SESSION_MANAGER'
	AND TransactionCode <> 'Login'
	and LEFT(SapId,2) <> 'D_'
	and SapId NOT LIKE '%USER'
	and SapId NOT LIKE '%RFC%'
	and SapId NOT IN ('WS_EHO','DDIC','SAP_SYSTEM')
--	and CAST([TransactionCount] AS INT) <1000 -- günlük 1000'den fazla transaction var ise manuel değil otomatiktir öngörüsü ile incelenmiştir.
