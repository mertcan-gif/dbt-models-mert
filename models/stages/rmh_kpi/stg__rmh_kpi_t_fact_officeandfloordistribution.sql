{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
/**
47045616 (mavi yaka) sacreport tablosunda bulunmadığı için NULL gelmektedir.
47002237 sap_id'sine sahip kişi hem hrall da hem de sacreportta bulunmadığı için NULL gelmektedir.
**/
WITH hr_data AS (
	SELECT
		sac.sap_id
		,CONCAT(sac.first_name, ' ', sac.last_name) AS full_name
		,sac.[grup/baskanlik_en]
		,sac.is_birimi_en
		,sac.bordro_sirketi_kodu
		,sac.externalcode_picklistoption
		,sac.sirket_ise_giris_tarihi
		,sac.bitis_tarihi
		,ROW_NUMBER() OVER (PARTITION BY sap_id, CONCAT(sac.first_name, ' ', sac.last_name) ORDER BY sac.sirket_ise_giris_tarihi DESC) rn
	FROM {{ source('stg_hr_kpi', 'raw__hr_kpi_t_fact_sacreport') }} sac 
	)
 
,raw_data AS (
/**
	Filtredeki pernr, pa9608 tablosu oluşturulurken kaydırma yapıldığı için sap ve idari işler ekibinin (Hasan Kurt) bilgisi ile filtrelenmiştir.
	REFIKBELEN ismini REFIKB, REFIKBELEN112 ismini REFIK112 ile değiştirilmesi ise ofislerin masraf yerlerine göre ayarlamak içindir.
	Hasan Kurt'un alınan bilgiye göre pa9608 tablosunda bulunan REFIKBELEN ofisinin masrafları REFIKB ismi ile yer alıyor.
	REFIKBELEN112 diye girilen verilerin REFIK112 ile aynı olduğunu belirtti. Ofisin masrafı da REFIK112 olarak kaydedildiği için REFIK112 olarak değiştirildi. 
**/
	SELECT 
		pa.[pernr] AS sap_id
		,CAST([begda] AS date) AS start_date
		,CAST([endda] AS date) AS end_date
		,CASE
			WHEN TRIM([zzbina]) = 'REFIKBELEN' THEN 'REFIKB'
			WHEN TRIM([zzbina]) = 'REFIKBELEN112' THEN 'REFIK112'
			ELSE TRIM([zzbina])
		END AS office
		,[zzkat] AS floor
		,CASE 
			WHEN [zzservis] = '1' THEN 'Ankara Personnel Service'
			WHEN [zzservis] = '2' THEN 'Istanbul Personnel Service'
			ELSE [zzservis]
		END AS service
	FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_pa9608') }} pa
	WHERE pa.pernr <> '08519960' 
	)
 
,add_date_series AS (
/**
start_date 2024-01-01 
end_date 9999-12-31 
gibi olan olan değerler için end_date kısmını aylık olarak kırmak için oluşturulan cte'dir.
**/
    SELECT 
		sap_id,
		start_date,
		CASE
			WHEN end_date = '9999-12-31' THEN CAST(GETDATE() AS DATE)
			ELSE end_date
		END AS end_date,
		office,
		floor,
		service,
		DATEFROMPARTS(YEAR(start_date), MONTH(start_date), 1) AS current_start_date,
		EOMONTH(start_date) AS current_end_date
   	FROM raw_data
	UNION ALL
	SELECT
		sap_id,
		start_date,
		end_date,
		office,
		floor,
		service,
		DATEADD(MONTH, 1, current_start_date) AS current_start_date,
		EOMONTH(DATEADD(MONTH, 1, current_end_date)) AS CurrentEndDate
	FROM add_date_series
	WHERE DATEADD(MONTH, 1, current_start_date) <= end_date
)
 
SELECT
	[rls_region] = CASE 
					WHEN (SELECT TOP 1 custom_region FROM [hr_kpi].[raw__hr_kpi_t_dim_group] grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
					ELSE 'RUS' 
				 END
	,[rls_group] = CONCAT(
							(SELECT TOP 1 group_rls FROM [hr_kpi].[raw__hr_kpi_t_dim_group] grp WHERE grp.[group] = sac.[grup/baskanlik_en])
							,'_'
							,CASE 
								WHEN (SELECT TOP 1 custom_region FROM [hr_kpi].[raw__hr_kpi_t_dim_group] grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
								ELSE 'RUS' 
							END)
   ,[rls_company] = CONCAT(UPPER(sac.is_birimi_en),
                            '_',
                            CASE
                                WHEN (SELECT TOP 1 custom_region FROM [hr_kpi].[raw__hr_kpi_t_dim_group] grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
                                ELSE 'RUS'
                            END)
	,[rls_businessarea] = CONCAT(sac.externalcode_picklistoption,
	                            '_',
								CASE
									WHEN (SELECT TOP 1 custom_region FROM [hr_kpi].[raw__hr_kpi_t_dim_group] grp WHERE grp.[group] = sac.[grup/baskanlik_en]) = 'TR' THEN 'TUR'
									ELSE 'RUS'
								END)
	,ds.sap_id
    ,sac.full_name
	,sac.bordro_sirketi_kodu AS company
	,ds.start_date AS current_start_date
    ,current_end_date
    ,
	/*
        Piazza ofisin masrafları kat bazlı atıldığı için office isimlerini bu şekilde güncellenmesi gerekmektedir.
    */
	CASE
		WHEN ds.office = 'PIAZZA' THEN CONCAT(ds.office, '-', ds.floor)
		ELSE ds.office
	END AS office
    ,ds.floor
	,service
FROM add_date_series ds
LEFT JOIN (
			SELECT
				sap_id
				,full_name
				,[grup/baskanlik_en]
				,is_birimi_en
				,externalcode_picklistoption
				,bordro_sirketi_kodu
			FROM hr_data
			WHERE rn = 1) sac ON sac.sap_id = ds.sap_id 

