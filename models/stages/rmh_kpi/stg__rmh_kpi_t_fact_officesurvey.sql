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
    [QuestionNumber],
    CASE 
		WHEN [QuestionResponse] = N'Çok iyi' THEN 5
		WHEN [QuestionResponse] = N'İyi' THEN 4
		WHEN [QuestionResponse] = N'Orta' THEN 3
		WHEN [QuestionResponse] = N'Kötü' THEN 2
		WHEN [QuestionResponse] = N'Çok kötü' THEN 1
		ELSE [QuestionResponse]
	END AS [question_response]
FROM 
    (SELECT 
		  [start_time],
          [completion_time],
          [email],
          [name],
          [office_name],
          CAST([answer_question_1] AS NVARCHAR(255)) [answer_question_1] ,
          CAST([answer_question_2] AS NVARCHAR(255)) [answer_question_2] ,
          CAST([answer_question_3] AS NVARCHAR(255)) [answer_question_3] ,
          CAST([answer_question_4] AS NVARCHAR(255)) [answer_question_4] ,
          CAST([answer_question_5] AS NVARCHAR(255)) [answer_question_5] ,
          CAST([answer_question_6] AS NVARCHAR(255)) [answer_question_6] ,
          CAST([answer_question_7] AS NVARCHAR(255)) [answer_question_7] ,
          CAST([answer_question_8] AS NVARCHAR(255)) [answer_question_8] ,
          CAST([answer_question_9] AS NVARCHAR(255)) [answer_question_9] 
     FROM {{ source('stg_sharepoint', 'raw__rmh_kpi_t_fact_officesurveyresult') }}
    ) AS SourceTable
UNPIVOT
(
    QuestionResponse FOR QuestionNumber IN (
        [answer_question_1], 
        [answer_question_2], 
        [answer_question_3], 
        [answer_question_4], 
        [answer_question_5], 
        [answer_question_6], 
        [answer_question_7], 
        [answer_question_8], 
        [answer_question_9]
    )
) AS UnpivotedTable 



