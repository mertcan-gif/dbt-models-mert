{{
  config(
    materialized = 'table',tags = ['s4_odata']
    ,post_hook = [
      "CREATE NONCLUSTERED INDEX idx_rbukrs ON {{ this }} (bukrs)",
      "CREATE NONCLUSTERED INDEX idx_gjahr ON {{ this }} (gjahr)",
      "CREATE NONCLUSTERED INDEX idx_belnr ON {{ this }} (belnr)"
    ]
    )
}}

SELECT 
	     [bukrs]
      ,[belnr]
      ,[gjahr]
      ,[blart]
      ,[cpudt]
      ,[usnam]
      ,[tcode]
      ,[xblnr]
      ,[stblg]
      ,[bktxt]
      ,[bstat]
      ,[awkey]
      ,[xreversing] = 
				case when xreversing = 'False' then 0
				else 1
				end
      ,[xreversed] = 
				case when xreversed = 'False' then 0
				else 1
				end
      ,[db_upload_timestamp]
FROM {{ source('stg_s4_odata', 'raw__s4hana_t_sap_bkpf') }}
