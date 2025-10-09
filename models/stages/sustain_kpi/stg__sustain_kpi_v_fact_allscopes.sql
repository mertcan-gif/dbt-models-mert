{{
  config(
    materialized = 'table',tags = ['sustain_kpi']
    )
}}

SELECT
    [Yıl] as year,
    [Ay] as month,
    [Grup Şirket] as company,
    [Proje / İşletme] as project,
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    cast([CO2e (ton)] as float) as  [co2e],
    '1' as scope_type,
    '1_1' as source
--    ciro as revenue
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_1_1') }}

UNION ALL

SELECT

    Yıl,
    Ay,
    [Grup Şirket],
    [Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    cast([CO2e (ton)] as float) [co2e],
    '1' as scope_type,
    '1_2' as source
--    ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_1_2') }}


UNION ALL

SELECT

    m.yil,
    m.ay,
    m.grup,
    m.proje,
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    try_cast(m.[co2e] as float) as [co2e],
    '1' as scope_type,
    '1_4' as source

--    ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_1_4') }} m


union all

SELECT
       [Yıl]
      ,[Ay]
      ,[Grup Şirket]
      ,[Proje / İşletme]
      ,[Proje İşletme Kodu] as project_code_in_dimensions_table
      ,try_cast([Emisyon Miktarları CO2 (ton)] as decimal(20,15)) as co2e
      ,'2' as scope_type
      ,'2_1' as source
--      ,ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_2_1') }}

UNION ALL

SELECT 
    [Yıl],
    [Ay],
    [Grup Şirket],
    [Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    try_cast([total_emisyon_kg CO2e] as float) /1000 as  co2e
    ,'3' as scope_type
    ,'3_1' as source

  --  ,ciro
  FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_3_1') }}

UNION ALL

SELECT  
    CASE WHEN cast([Yıl] AS varchar(max)) = '2024(Ekim-Kasım-Aralık)' THEN 2024 ELSE cast([Yıl] AS int) end as [Yıl],
    [Ay],
    [Grup Şirket],
    [Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    try_cast([total_emisyon_kg CO2e] as float)/1000 as  co2e
    ,'3' as scope_type
    ,'3_2' as source
--    ,ciro
  FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_3_2') }}
where 1=1


UNION ALL

SELECT  
    CASE WHEN cast([Yıl] AS varchar(max)) = '2024(Ekim-Kasım-Aralık)' THEN 2024 ELSE cast([Yıl] AS int) end as [Yıl],
    [Ay],
    [Grup Şirket],
    [Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    try_cast([total_emisyon_kg CO2e] as float)/1000 as  co2e
    ,'3' as scope_type
    ,'3_3' as source

--    ,ciro
  FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_3_3') }}
where 1=1


union all


SELECT  
    CASE WHEN cast([sene] AS varchar(max)) = '2024(Ekim-Kasım-Aralık)' THEN 2024 ELSE cast([sene] AS int) end as [Yıl],
    [Ay],
    m.[sirket],
    [proje],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    try_cast([emisyon_co2e] as float)/1000 as  co2e
    ,'3' as scope_type
    ,'3_4' as source

--    ,ciro
  FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_3_4') }} m
where 1=1

union all


SELECT  
    CASE WHEN cast([Yıl] AS varchar(max)) = '2024(Ekim-Kasım-Aralık)' THEN 2024 ELSE cast([Yıl] AS int) end as [Yıl],
    [Ay],
    [Grup Şirket],
    [Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    (coalesce(try_cast([emisyon_co2e_kg] as float),0)/1000+
    coalesce(try_cast([total_emisyon_kg CO2e] as float),0)/1000) as summed_value,
    '3' as scope_type,
    '3_5' as source

--    ,ciro
  FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_3_5') }}
where 1=1

union all

SELECT 
      [Yıl]
	  ,Ay as ay
	  ,[Grup Şirket] as şirket
    ,[Proje İşletme]
    ,[Proje İşletme Kodu] as project_code_in_dimensions_table
    ,try_cast([Emisyon Miktarları (ton)] as float)
	  ,'3' as scope_type
    ,'4_1_1' as source

--	  ,ciro
  FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_4_1_1') }}
where 1=1


union all

SELECT 
      [Yıl]
	  ,Ay as ay
	  ,m.[Grup Şirket] as şirket
    ,[Proje İşletme]
    ,[Proje İşletme Kodu] as project_code_in_dimensions_table
    ,try_cast([emisyon] as float)
	  ,'3' as scope_type
    ,'4_1_2' as source

--	  ,ciro
  FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_4_1_2') }} m
where 1=1

union all

SELECT

    CASE 
        WHEN cast([Yıl] AS varchar(max)) = '2024(Ekim-Kasım-Aralık)' THEN 2024
        WHEN cast([Yıl] AS varchar(max)) = '2024/9' THEN 2024
        WHEN cast([Yıl] AS varchar(max)) = '2024/7' THEN 2024
        WHEN cast([Yıl] AS varchar(max)) = '2024/8' THEN 2024
        ELSE cast([Yıl] AS int) end as [Yıl],
    Ay AS ay,
    [Grup Şirket],
    [Proje İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    try_cast([Emisyon Miktarları] as decimal(20,15)),
    '3' as scope_type,
    '4_2' as source

--    ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_4_2') }}

union all

SELECT distinct 

    CASE 
        WHEN cast([Yıl] AS varchar(max)) = '2024(Q1-Q2-Q3)' THEN 2024
        WHEN cast([Yıl] AS varchar(max)) = '2024-Q1' THEN 2024
        WHEN cast([Yıl] AS varchar(max)) = '2024-Q2' THEN 2024
        WHEN cast([Yıl] AS varchar(max)) = '2024-Q3' THEN 2024
        WHEN len([Yıl])<4  THEN 2024
        ELSE cast([Yıl] AS int) end as [Yıl],
    Ay AS ay,
    [Grup Şirket],
    [Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    try_cast([Emisyon Miktarları (ton)] as decimal(20,15)) as emisyon,
    '3' as scope_type,
    '4_3' as source

--    ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_4_3') }}

union all

SELECT
    m.[yil],
    ay,
    m.[sirket],
    [proje],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    emisyon,
    '3' as scope_type,
    '4_5' as source

--    ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_4_5') }} m

union all

select
    k.year,
    k.month,
    k.company,
    k.project,
    NULL as project_code_in_dimensions_table,
    k.co2e_ton,
    '3' as scope_type,
    '5_4' as source

--    t.ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_5_4') }} k

union all 

select
    k.Yıl,
    k.Ay,
    k.sirket,
    k.[Proje / İşletme],
    [Proje İşletme Kodu] as project_code_in_dimensions_table,
    k.co2e_ton,
    '3' as scope_type,
    '5_2' as source

--    t.ciro
FROM {{ source('stg_sharepoint', 'raw__sustain_kpi_t_fact_emission_5_2') }} k
