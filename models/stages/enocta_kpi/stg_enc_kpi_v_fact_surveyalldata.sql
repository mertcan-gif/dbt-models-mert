
{{
  config(
    materialized = 'table',tags = ['enc_kpi']
    )
}}	
SELECT DISTINCT 
 ff.[SurveyCode] 
,ff.[SurveyName] 
,ff.[SurveyDescription]
,ff.[SurveyStartDate]
,ff.[SurveyEndDate]
,ff.[CompulsoryStatus]
,ff.[SurveyType]
,ff.[AnswerUpdateStatus]
,ff.[Version]
,ff.[VersionSurveyCode]
,ff.[AnswerGetType]
,ff.[CreatedBy]
,ff.[Foreword]
,ff.[Lastword]
,ff.[UserCode]
,ff.[Name]
,ff.[LastName]
,ff.[EvaluatedUserCode]
,ff.[EvaluatedName]
,ff.[EvaluatedLastName]
,ff.[UserClass]
,ff.[ContentEvaluationScore]
,ff.[InstructorEvaluationScore]
,ff.[SurveyAnswerDate]
,ff.[ID]
,ff.[NAME_1]
,aa.name as course_name
,ff.[INTEGRATION_CODE]
,ff.[db_upload_timestamp]
,ss.[UserCode] as user_code_survey_detail
,ss.[Name] as  user_name_survey_detail
,ss.[LastName] as user_lastname_survey_detail
,ss.[EvaluatedUserCode] as evlaluated_user_code_survey_detail 
,ss.[EvaluatedName] as evaluated_user_name_survey_detail 
,ss.[EvaluatedLastName] as evaluated_user_lastname_survey_detail 
,ss.[UserClass] as user_class_survey_detail
,ss.[ContentEvaluationScore] as content_evaluation_survey_detail
,ss.[InstructorEvaluationScore] as instructor_evaluation_survey_detail
,ss.[SurveyAnswerDate] as survey_answer_date_survey_detail
,ss.[ACTIVITY_CODE]
,ss.[ACTIVITY_INTEGRATION_CODE]
,ss.[UNIT_CODE]
,ss.[UNIT_INTEGRATION_CODE]
,ss.[SessionCode]
,ss.[SessionStartDate]
,ss.[SessionEndDate]
,ss.[SessionFacilityCode]
,ss.[SessionFacilityName]
,ss.[SessionHallCode]
,ss.[SessionHallName]
,ss.[INSTRUCTOR_CODE]
,ss.[INSTRUCTOR_NAME]
,ss.[INSTRUCTOR_TYPE]
,ss.[QuestionId]
,ss.[QuestionText]
,ss.[IsRequired]
,ss.[QuestionGroupId]
,ss.[SurveyQuestionType]
,ss.[AnswerId]
,ss.[AnswerText]
,ss.[db_upload_timestamp] as upload_timestamp_survey_detail
FROM {{ source('stg_enc_kpi', 'raw_enocta_kpi_t_fact_surveybase') }} ff
LEFT JOIN {{ source('stg_enc_kpi', 'raw_enocta_kpi_t_fact_surveydetails') }} ss on ss.ACTIVITY_INTEGRATION_CODE = ff.INTEGRATION_CODE
																				and ss.AnswerId is not null
																				and ss.UserCode = ff.UserCode
LEFT JOIN (select distinct *
from  {{ source('stg_enc_kpi', 'raw_enocta_kpi_t_fact_surveycoursenames') }}
where id is not null) aa ON aa.ID = ss.UNIT_CODE

where 1=1