
{{
  config(
    materialized = 'table',tags = ['s4mevduat_draft','fi_kpi_draft']
    )
}}


/* 
2025-02-19: dm__nwc_kpi_t_fact_s4mevduat Tablosunu 3 farklı tablodan besliyoruz. Bu tablolarin hesap codelarindan da raporda ivf flagi yakiyoruz.
Eğer buradaki hesap kodlari, s4mevduatfreerestricted icinde olmazsa yeni eklenen hesaplar rapora yansimiyor. Dolayisiyla bu 3 kaynagi da buraya ekliyoruz
ilk kaynak sap'den gelen s4mevduat
ikincisi tr'deki yuvam hesaplari
ucuncusu rnetten gelen sap disi sirketlerin kodlari

*/ 


SELECT DISTINCT
	account_number = general_ledger_account
	,free_restricted 
	,bank_name
FROM {{ ref('stg__nwc_kpi_t_fact_s4bankbalances') }} 

UNION 

SELECT 
      DISTINCT 
      account_number=[RACCT]
      ,free_restricted=[SERBEST]
      ,bank_name=[BANKATANIMI]
  FROM {{ ref('stg__nwc_kpi_t_fact_yuvammevduat') }}

UNION 

SELECT 
      DISTINCT 
      account_number=[RACCT]
      ,free_restricted=[SERBEST]
      ,bank_name=[BANKATANIMI]
  FROM {{ ref('stg__nwc_kpi_t_fact_ivfmevduat') }}

UNION 

SELECT DISTINCT 
	account_number= RACCT COLLATE Latin1_General_CI_AS
	,free_restricted_flag=SERBEST COLLATE Latin1_General_CI_AS
	,bank_name=BANKATANIMI COLLATE Latin1_General_CI_AS
	FROM {{ ref('stg__nwc_kpi_t_fact_rnetbankbalances') }}
	where [TARIH]>='2024-01-01'

