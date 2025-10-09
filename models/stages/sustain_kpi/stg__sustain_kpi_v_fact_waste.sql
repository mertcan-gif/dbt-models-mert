{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

select 

 [Grup Şirket] as company
,[Yıl] as year
,[Ay] as month
,[Proje / İşletme] as project
,[Proje İşletme Kodu] as project_code_in_dimensions_table
,[Atık Türü] as waste_type
,[Atık Bertaraf Yöntemi] as waste_disposure_type
,(CASE WHEN  [Atık Bertaraf Yöntemi] like '%geri dön%'  and [Faaliyet Verisi Birim] like '%kg%'
		then cast([Faaliyet Verisi Değer] as float)/1000
	  when   [Atık Bertaraf Yöntemi] like '%geri dön%'  and [Faaliyet Verisi Birim] = 'ton'
	  then cast([Faaliyet Verisi Değer] as float)
		 ELSE 0 END) AS recycle_amount
,[Faaliyet Verisi Birim] as activity_value_unit
,[Faaliyet Verisi Değer] as nonfiltered_activity_value
,case 
	when [Faaliyet Verisi Birim] like '%kg%' then cast([Faaliyet Verisi Değer] as float)/1000
	when [Faaliyet Verisi Birim] = 'ton' then cast([Faaliyet Verisi Değer] as float)
end as activity_value
--ciro as revenue
from {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_4_3') }}
--	left join {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }} t on t.sirket = [Grup Şirket] and cast(t.yil as varchar) = cast(Yıl AS varchar(max))
where 1=1
 --and Yıl = '2023'

--UNION ALL

--SELECT
--	   [Grup Şirket]
--      ,[Yıl]
--      ,[Ay]
--      ,[Proje / İşletme]
--      ,NULL as project_code_in_dimensions_table
--	  ,'' 
--      ,[Atık Bertaraf Yöntemi]
--	  ,(CASE WHEN [Atık Bertaraf Yöntemi] like '%Geri%'  and [Faaliyet Verisi Birim] like '%kg%'
--				then try_cast([Faaliyet Verisi Değer] as float)/1000
--			  when   [Atık Bertaraf Yöntemi] like '%Geri%'  and [Faaliyet Verisi Birim] = 'ton'
--			  then try_cast([Faaliyet Verisi Değer] as float)
--				 ELSE 0 END) AS geri_donusum
--      ,[Faaliyet Verisi Değer]
--		,case 
--			when [Atık Bertaraf Yöntemi] like '%Toplam%' and  [Faaliyet Verisi Birim] like '%kg%' then cast([Faaliyet Verisi Değer] as float)/1000
--			when [Atık Bertaraf Yöntemi] like '%Toplam%' and  [Faaliyet Verisi Birim] = 'ton' then cast([Faaliyet Verisi Değer] as float)
--		else 0
--		end as emisyon_faktor_deger_toplam
----      ,ciro
--FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_water_consumption_2023') }}
----	left join {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_revenue') }} t on t.sirket = [Grup Şirket] and cast(t.yil as varchar) = cast(Yıl AS varchar(max))
