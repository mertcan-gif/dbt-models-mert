{{ config( materialized = 'table', tags = ['metadata_kpi'] ) }}


WITH raw_data as (
    SELECT 
        CAST(MSP.VALUE AS NVARCHAR(MAX)) COLLATE SQL_Latin1_General_CP1_CI_AS  AS id
        ,CAST(MS.CAPTION AS NVARCHAR(MAX)) COLLATE SQL_Latin1_General_CP1_CI_AS AS report_name
        ,N'RNet' COLLATE SQL_Latin1_General_CP1_CI_AS AS sub_segment
        ,N'NON-ERP' COLLATE SQL_Latin1_General_CP1_CI_AS   AS segment
    FROM EBA.EBA.dbo.MENUSTRUCTUREPARAMS AS MSP WITH (NOLOCK)
        INNER JOIN EBA.EBA.dbo.MENUSTRUCTURE AS MS WITH (NOLOCK) ON MS.ID = MSP.ID
    WHERE (MSP.PARAMETER = N'ARCHIVENAME')
),
partition_data as (
	SELECT *
        ,ROW_NUMBER() OVER(PARTITION BY id Order by id) RN
	FROM raw_data
)

SELECT 
    id
    ,report_name
    ,sub_segment
    ,segment
FROM partition_data
WHERE RN = 1

