{{
  config(
    materialized = 'table',tags = ['gyg_kpi']
    )
}}


SELECT  
      CAST([Zyear] AS INT) as [year]
      ,CAST([Zmonx] AS INT) as [month]
      ,[Bukrs] AS [company]
      ,[Fipex] AS [financial_center_code]
      ,CAST([Znorm] AS INT) AS [norm]
      ,CAST(replace([Actual],'*','') AS INT) AS [actual]	
	  ,REPLACE([Gygvers],'Q','V') as [version]
  FROM {{ source('stg_gyg_kpi', 'raw__gyg_kpi_t_sap_hrpermanentstaff') }}
