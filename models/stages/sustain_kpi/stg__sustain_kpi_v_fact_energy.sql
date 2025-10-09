{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}


select
	Yıl as year,
	Ay as month,
	[Grup Şirket] as company,
	[Proje / İşletme] as project,
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
	[Yakıt Türü] as fuel_type,
	case 
		when lower([Tüketim Birim]) = 'kWh'and LOWER([Yakıt Türü]) like '%gaz%' then (cast([Tüketim Miktar] as float)/10.64)*(0.036)
		when lower([Tüketim Birim]) = 'm3' and LOWER([Yakıt Türü])  like '%gaz%' then cast([Tüketim Miktar] as float)*(0.036)
		when lower([Tüketim Birim]) = 'lt' and LOWER([Yakıt Türü])  like '%dizel%' then cast([Tüketim Miktar] as float)*(0.036)
		when lower([Tüketim Birim]) = 'lt' and LOWER([Yakıt Türü])  like '%benzin%' then cast([Tüketim Miktar] as float)*(0.033)	
		when lower([Tüketim Birim]) = 'kg' and LOWER([Yakıt Türü])  like '%LPG%' then cast([Tüketim Miktar] as float)*(52)/1000	
		when lower([Tüketim Birim]) = 'kg' and LOWER([Yakıt Türü])  like '%LNG%' then cast([Tüketim Miktar] as float)*(52)/1000
		when lower([Tüketim Birim]) = 'm3' and LOWER([Yakıt Türü])  like '%LNG%' then cast([Tüketim Miktar] as float)*(52)	
		when lower([Tüketim Birim]) = 'kg' and LOWER([Yakıt Türü])  like '%Propan%' then cast([Tüketim Miktar] as float)*(0.0464)	
	end as consumption_amount,
	NULL as total_intensity_revenue,
	'1_1' as source
--	ciro as revenue
from {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_1_1') }}
--	left join {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }} t on t.sirket = [Grup Şirket] and cast(t.yil as varchar) = cast(Yıl AS varchar(max))
where 
	1=1
	--and Yıl != '2023'
UNION ALL

SELECT
	Yıl,
	Ay,
	[Grup Şirket],
	[Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
	[Yakıt Türü],
	CASE
		WHEN lower([Tüketim Birim]) = 'lt' AND LOWER([Yakıt Türü])  like '%dizel%' THEN TRY_CAST([Tüketim Miktar] as FLOAT)*(0.036)
		WHEN lower([Tüketim Birim]) = 'lt' AND LOWER([Yakıt Türü])  like '%benzin%' THEN TRY_CAST([Tüketim Miktar] as FLOAT)*(0.033)	
	END AS x,
	NULL as total_intensity_revenue,
	'1_2' as source

--	ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_1_2') }}
--	left join {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }} t on t.sirket = [Grup Şirket] and cast(t.yil as varchar) = cast(Yıl AS varchar(max))
where 
	1=1
	--and Yıl != '2023'
UNION ALL

select
	Yıl,
	Ay,
	[Grup Şirket],
	[Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
	'Elektrik' as [Yakıt Türü],
	(COALESCE([Yenilenebilir Enerji Miktar],0) + (COALESCE(TRY_CAST([Tüketim Miktarı] as FLOAT),0)))*0.0036,
	NULL as total_intensity_revenue,
	'2_1' as source
--	ciro
from {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_2_1') }}
--	left join {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }} t on t.sirket = [Grup Şirket] and cast(t.yil as varchar) = cast(Yıl AS varchar(max))
where 
	1=1
	--and Yıl != '2023'
--UNION ALL


--SELECT

--      [Yıl] as year
--      ,[Ay] as month
--	  ,[Grup Şirket] as company
--      ,[Proje / İşletme] as project
--    ,NULL as project_code_in_dimensions_table
--	,'Elektrik' as type
--	  ,case 
--		when [Atık Bertaraf Yöntemi] like '%Enerji%'  then cast([Faaliyet Verisi Değer] as float)
--		else 0
--		end as energy_activity_value,
--		NULL as total_intensity_revenue
----    ciro
--FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_water_consumption_2023') }}
----	left join {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }} t on t.sirket = [Grup Şirket] and cast(t.yil as varchar) = cast(Yıl AS varchar(max))
--where 1


UNION ALL


select 
	Yıl as year,
	Ay as month,
	[Grup Şirket] as company,
	[Proje / İşletme] as project,
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
	'Su' as fuel_type,
	case 
		when [Faaliyet Verisi Birim] like N'%m3%' and [Atık Türü] like N'%atıksu%' then cast([Faaliyet Verisi Değer] as float)
		when [Faaliyet Verisi Birim] like N'%kg%' and [Atık Türü] like N'%atıksu%' then cast([Faaliyet Verisi Değer] as float)*0.001
		when [Faaliyet Verisi Birim] like N'%ton%' and [Atık Türü] like N'%atıksu%' then cast([Faaliyet Verisi Değer] as float)* 1.01832416
		else 0
	end as activity_value,
--	t.ciro as revenue,
    2044.05 as total_intensity_revenue,
	'4_3' as source
from {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_4_3') }}
	--left join {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }} t on t.sirket = [Grup Şirket] and cast(t.yil as varchar) = cast(Yıl AS varchar(max))
where 1=1
 	--and Yıl != '2023'

--UNION ALL

--SELECT

--	[Yıl]
--	,[Ay]
--	,[Grup Şirket]
--	,[Proje / İşletme]
--    ,NULL as project_code_in_dimensions_table
--	,'Su' as [Yakıt Türü]
--	,case 
--		when [Atık Bertaraf Yöntemi] like '%Su%' and [Faaliyet Verisi Birim] like '%m3%' then cast([Faaliyet Verisi Değer] as float)
--		else 0
--	end as emisyon_faktor_deger_toplam
----	,ciro
--	,2819 as total_intensity_revenue
--FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_water_consumption_2023') }}
--	--left join {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }} t on t.sirket = [Grup Şirket] and cast(t.yil as varchar) = cast(Yıl AS varchar(max))
--where 1