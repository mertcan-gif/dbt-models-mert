{{
  config(
    materialized = 'table',tags = ['nwc_kpi','stockaging']
    )
}}
WITH atıl_ambardan_yapılan_transferler as (
SELECT
	matnr,
	umwrk,
	werks,
	lgort,
	cpudt_mkpf,
	xauto,
	bwart,
	mblnr,
	menge
FROM 
{{ source('stg_s4_odata', 'raw__s4hana_t_sap_mseg') }} m
where 1=1
	and lgort like '97%'
	and bwart = '303'
	and xauto  = ''
)
, atıl_ambardan_yapılan_malzeme_yasi_tayini as (
SELECT
	a.mblnr,
	a.matnr,
	a.werks,
	a.lgort,
	a.umwrk,
YEAR(a.cpudt_mkpf) as yıl,
MONTH(a.cpudt_mkpf) as ay
FROM  {{ source('stg_s4_odata', 'raw__s4hana_t_sap_mseg') }} a
where 1=1
	 AND a.bwart IN ('907','908')
	 AND a.lgort like '97%'
	 AND ( a.matnr IN 
						(
						SELECT
						matnr
						FROM atıl_ambardan_yapılan_transferler
						where 1=1
						) 
	 AND a.werks IN 
						(
						SELECT
						werks
						FROM atıl_ambardan_yapılan_transferler
						where 1=1
						) )
)
, atıl_ambardan_yapılan_malzeme_yasi_final as (
SELECT
	CASE
	 WHEN a.Stock_Age != '01' THEN '+90 Alım Teşviki'
	 else 'Teşvik yok' end as stok_yasi_tesvigi,
	 a.matnr
FROM (select
		Stock_Age,
		mblnr,
		matnr
		from {{ source('stg_s4_odata', 'raw__s4hana_t_sap_zlp_mm_atil_0001') }}
		where 1=1 
			and  Stock_Age != '01' --sadece alım teşviği olan ürünler getirilir.
		)
		a
where mblnr IN (SELECT mblnr from atıl_ambardan_yapılan_malzeme_yasi_tayini)
)
, atıl_ambar_transferi_yapılan_malzemenin_giriş_tarihindeki_kayar_ortalama_maliyeti_ve_uygulaması as (
SELECT
	t.umwrk as 'uretim_yeri',
	'alim_tesviki' as 'tip',
	mb.verpr,
	t.matnr,
	t.lgort,
	t0.lgobe as warehouse_name,
	cast(mb.verpr As float)*CAST(a.menge AS float) AS 'miktar'
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_mbewh') }} mb
	RIGHT JOIN atıl_ambardan_yapılan_transferler as a on a.matnr =  REPLACE(LTRIM(REPLACE(mb.matnr, '0', ' ')), ' ', '0') and mb.bwkey=a.werks
	RIGHT JOIN atıl_ambardan_yapılan_malzeme_yasi_tayini AS t on t.yıl= mb.lfgja and t.ay= mb.lfmon and t.matnr =  REPLACE(LTRIM(REPLACE(mb.matnr, '0', ' ')), ' ', '0') and  mb.bwkey=t.werks
	LEFT JOIN aws_stage.[s4_odata].[raw__s4hana_t_sap_t001l] as t0 on concat(t0.werks,'_',t0.lgort) = CONCAT(t.werks,'_',t.lgort)
WHERE 1=1
UNION ALL
SELECT
	a.werks as 'uretim_yeri',
	'stok_maliyeti' as 'tip',
	mb.verpr,
	a.matnr,
    a.lgort,
	t0.lgobe as warehouse_name,
	cast(mb.verpr As float)*CAST(a.menge AS float) as 'miktar'
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_mbewh') }} mb
	RIGHT JOIN atıl_ambardan_yapılan_transferler as a on a.matnr =  REPLACE(LTRIM(REPLACE(mb.matnr, '0', ' ')), ' ', '0') and mb.bwkey=a.werks 
	RIGHT JOIN atıl_ambardan_yapılan_malzeme_yasi_tayini AS t on t.yıl= mb.lfgja and t.ay= mb.lfmon and t.matnr =  REPLACE(LTRIM(REPLACE(mb.matnr, '0', ' ')), ' ', '0') and  mb.bwkey=t.werks
	LEFT JOIN aws_stage.[s4_odata].[raw__s4hana_t_sap_t001l] as t0 on concat(t0.werks,'_',t0.lgort) = CONCAT(a.werks,'_',a.lgort)
)
SELECT
f.uretim_yeri as businessarea,
m.maktx as material_name,
f.tip as type,
f.verpr as item_unit_cost,
f.miktar as total_cost,
f.warehouse_name,
f.matnr,
f.lgort
FROM atıl_ambar_transferi_yapılan_malzemenin_giriş_tarihindeki_kayar_ortalama_maliyeti_ve_uygulaması f
	LEFT JOIN {{ source('stg_s4_odata', 'raw__s4hana_t_sap_makt') }} m on REPLACE(LTRIM(REPLACE(m.matnr, '0', ' ')), ' ', '0') = f.matnr and m.spras= 'T'
