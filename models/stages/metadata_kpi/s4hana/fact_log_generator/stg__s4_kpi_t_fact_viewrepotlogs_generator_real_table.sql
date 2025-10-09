{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}

----- ilk önce geçmiş verisinde eksiklikleri olan tabloyu çekiyorum AÇIKLAMA 1 (DEVAMI İÇİN AÇIKLAMA 2'Yİ OKUYUNUZ)
WITH RAW_CTE AS (
	SELECT 
			CASE 
				WHEN GecerlilikBitisTarihi = '' THEN 1
				WHEN CONVERT(DATE,GecerlilikBitisTarihi,104) > CONVERT(DATE,IslemTarihi,104) THEN 1
				WHEN CONVERT(DATE,GecerlilikBitisTarihi,104) <= CONVERT(DATE,IslemTarihi,104) THEN 0
			END activity_flag,
			usg.*
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_reportusages') }} usg
		LEFT JOIN (select * from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_tstct') }} where sprsl = 'T' ) tstct ON usg.IslemAdi = tstct.tcode
	where 1=1 
		and KullaniciBlokajDurumu = 0
		and KullaniciTipi = 'A'
		and LEFT(KullaniciAdi,2) <> 'D_'
		and KullaniciEmail <> ''
		AND KullaniciEmail NOT LIKE '%test%'
		and (tstct.ttext IS NOT NULL or IslemAdi LIKE '%SNI%')
		and IslemAdi <> 'SESSION_MANAGER'
	)
SELECT
	   'S4HANA' AS id
	  ,CONVERT(DATETIME, [IslemTarihi], 104) AS creation_time
	  ,lower(KullaniciEmail) AS [user_id]
	  ,'S4HANA' as [workspace_id]
	  ,[IslemAdi] as [report_id]
	  ,'S4HANA' AS [report_type]
	  ,'S4HANA' AS [consumption_method]
	  ,[IslemSayisi] as transaction_amount
  FROM RAW_CTE
  WHERE 1=1
	and activity_flag = 1
