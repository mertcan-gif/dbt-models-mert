{{
  config(
    materialized = 'table',tags = ['rmh_kpi']
    )
}}
SELECT 
    [start_time],
    [completion_time],
    [email],
    [name],
    [office_name],
    [QuestionNumber] as question_number,
    CASE 
        WHEN [QuestionResponse] IN (N'Çok iyi', N'Çok İyi', N'Çok memnunum') THEN 5
        WHEN [QuestionResponse] IN (N'İyi', 'Memnunum') THEN 4
        WHEN [QuestionResponse] IN (N'Orta' , N'Kararsızım') THEN 3
        WHEN [QuestionResponse] IN (N'Kötü', N'Memnun Değilim') THEN 2
        WHEN [QuestionResponse] IN (N'Çok kötü', N'Hiç Memnun Değilim') THEN 1
        ELSE [QuestionResponse]
    END AS question_response
FROM 
    (
    SELECT 
        [start_time],
        [completion_time],
        [email],
        [name],
        [office_name],
        CAST([answer_question_4] AS NVARCHAR(255)) [answer_question_4],
        CAST([answer_question_5] AS NVARCHAR(255)) [answer_question_5]
    FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_shuttlesurveyresult') }}
	WHERE 1=1
		AND answer_question_1 IS NOT NULL
    ) AS SourceTable
UNPIVOT
(
    QuestionResponse FOR QuestionNumber IN (
        [answer_question_4], 
        [answer_question_5]
    )
) AS UnpivotedTable



