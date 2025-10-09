{{
  config(
    materialized = 'table',tags = ['hr_kpi']
    )
}}

/*
	çoklayan kişilerin username'i aşağıda bulunmaktadır.
	Bu durumun nedeni, PA0001 tablosundaki ENDDA kolonunda iki satırın da 9999-12-31 değerine sahip olmasıdır.
	SBALCI
	ZKAYIKCI
	MKENC
	MUKESKIN
	GOAYDIN
	IISERI
	BKORKUNC
	AKUS
*/

WITH BaseData AS (
    SELECT 
        adcp.id_code,
        usr21.bname AS username,
        usr02.ustyp AS user_type,
        usr02.class AS user_group,
		LEFT(pa01.ename, LEN(pa01.ename) - CHARINDEX(' ', REVERSE(pa01.ename))) AS first_name,
		RIGHT(pa01.ename, CHARINDEX(' ', REVERSE(pa01.ename)) - 1) AS last_name,
        pa01.ename AS full_name,
        pa01.bukrs AS company,
		pa01.gsber AS businessarea,
        adr6.smtp_addr AS mail,
        pa01.kostl,
        CAST(pa01.begda AS DATE) AS start_date, 
        CASE 
            WHEN pa01.endda = '9999-12-31' THEN CAST(GETDATE() AS DATE)
            ELSE CAST(pa01.endda AS DATE)
        END AS end_date
    FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_usr21') }} usr21
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_adcp') }} adcp ON usr21.persnumber = adcp.persnumber
    LEFT JOIN (SELECT DISTINCT persnumber, smtp_addr FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_adr6') }}) adr6 
        ON adr6.persnumber = adcp.persnumber
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_adrp') }} adrp ON usr21.persnumber = adrp.persnumber
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_usr02') }} usr02 ON usr21.bname = usr02.bname
    LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_pa0001') }} pa01 ON pa01.pernr = adcp.id_code
    WHERE adcp.id_code <> ''
        AND adcp.id_code <> N'Sicil Yok'
        AND usr02.ustyp = 'A'
        AND usr02.class <> 'TERMINATED'
        AND pa01.bukrs IS NOT NULL
        AND pa01.bukrs <> ''
        AND pa01.kostl IS NOT NULL
        AND pa01.kostl <> ''
)

,final AS (
	SELECT 
		b.id_code, 
		b.username, 
		b.user_type, 
		b.user_group,
		b.first_name,
		b.last_name, 
		b.full_name, 
		b.company, 
		b.businessarea,
		b.mail, 
		b.kostl, 
		DATEADD(DAY, v.number, b.start_date) AS date
	FROM BaseData b
	CROSS APPLY (
		SELECT number 
		FROM master.dbo.spt_values 
		WHERE type = 'P' 
			AND number BETWEEN 0 AND DATEDIFF(DAY, b.start_date, b.end_date)
			) v
	)
SELECT 
  	rls_region = 'NAN'
    ,rls_group = 'GR_0000_NAN'
    ,rls_company = 'CO_0000_NAN'
    ,rls_businessarea = 'BA_0000_NAN'
    ,f.id_code
	,f.username
	,f.user_type
	,f.user_group
	,f.first_name
	,f.last_name
	,f.full_name
	,f.company
	,f.mail
	,f.kostl as cost_center
	,f.date
FROM final f
WHERE 1=1
AND (date = '2025-01-31' 
		OR 
	 date = CAST(GETDATE() AS DATE))