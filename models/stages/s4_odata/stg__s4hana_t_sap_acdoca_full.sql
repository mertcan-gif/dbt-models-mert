{{
  config(
    materialized = 'table',tags = ['s4_odata','acdoca_full']
    )
}}

WITH concatted_data AS (
  SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_acdoca_before_2023_buzei') }}
  UNION ALL
  SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_acdoca_before_2023_buzei_null') }}
  UNION ALL
  SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_acdoca_buzei') }}
  UNION ALL
  SELECT * FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_acdoca_buzei_null') }} 


)

,data_with_row_num AS (
  SELECT 
      *
      ,row_number() over(partition by rbukrs, gjahr, belnr, docln order by rbukrs) RN
  from concatted_data
  where 1=1
)

  SELECT 
       [rbukrs]
      ,[gjahr]
      ,[belnr]
      ,[rrcty]
      ,[bstat]
      ,[vorgn]
      ,[rwcur]
      ,[rtcur]
      ,[racct]
      ,[rcntr]
      ,[rbusa]
      ,cast([tsl] as money) as [tsl]
      ,cast([wsl] as money) as [wsl]
      ,cast([hsl] as money) as [hsl]
      ,cast([ksl] as money) as [ksl]
      ,cast([osl] as money) as [osl]
      ,cast([msl] as money) as [msl]
      ,[fiscyearper]
      ,[budat]
      ,[bldat]
      ,[blart]
      ,[buzei]
      ,[sgtxt]
      ,[matnr]
      ,[lifnr]
      ,[kunnr]
      ,[augbl]
      ,[gkont]
      ,[aufnr]
      ,[psposid] 
      ,[fipex] 
      ,[fistl] 
      ,[netdt] 
      ,[augdt]
      ,[zuonr]
      ,[ebeln]
      ,[rfarea]
      ,[ebelp]
      ,[umskz]
      ,[drcrk]
      ,[koart]
      ,CAST([fcsl] as money) as [fcsl]
      ,[docln]
      ,[mwskz]
      ,[gkoar]
      ,[afabe]
      ,[db_upload_timestamp]
  from data_with_row_num
  where 1=1
    and RN = 1