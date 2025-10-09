{{
  config(
    materialized = 'table',tags = ['metadata_kpi']
    )
}}



WITH RAW_CTE AS (
	SELECT 
			CASE 
				WHEN GecerlilikBitisTarihi = '' THEN 1
				WHEN CONVERT(DATE,GecerlilikBitisTarihi,104) > CONVERT(DATE,IslemTarihi,104) THEN 1
				WHEN CONVERT(DATE,GecerlilikBitisTarihi,104) <= CONVERT(DATE,IslemTarihi,104) THEN 0
			END activity_flag,
			*
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_reportusages') }}
	where 1=1 
		and KullaniciBlokajDurumu = 0
		and KullaniciTipi = 'A'
		and LEFT(KullaniciAdi,2) <> 'D_'
		and KullaniciEmail <> ''
		AND KullaniciEmail NOT LIKE '%test%'
	)

SELECT DISTINCT
     email_address = lower([KullaniciEmail])
    ,'S4HANA' AS license_group
    ,'S4HANA' AS license_type
    ,'S4HANA' AS segment
    ,snapshot_date = convert(date,IslemTarihi,104)
FROM RAW_CTE
WHERE 1=1
	and activity_flag = 1