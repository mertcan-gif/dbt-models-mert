{{
  config(
    materialized = 'view',tags = ['planner_kpi']
    )
}}

SELECT 
	RLS = plans.team_group
	,ReportingDate = tasks.reporting_date
	,TaskId = tasks.id
	,TaskName = tasks.title
	,AssignedUser = COALESCE(tam3.user_full_name,'No User Assigned')
	,TaskDescription =	COALESCE(tasks.description, 'No description...')
	,TaskBucket = buckets.name
	,Priority = 
		CASE 
			WHEN tasks.priority = 1 THEN 'Urgent'
			WHEN tasks.priority = 3 THEN 'Important'
			WHEN tasks.priority = 5 THEN 'Medium'
			WHEN tasks.priority = 9 THEN 'Low'
		END
	,SortPriority = 
		CASE 
			WHEN tasks.priority = 1 THEN 9
			WHEN tasks.priority = 3 THEN 8
			WHEN tasks.priority = 5 THEN 7
			WHEN tasks.priority = 9 THEN 6
		END

	,Progress =	
		CASE 
			WHEN tasks.percent_complete = 0 THEN 'Not Started'
			WHEN tasks.percent_complete = 50 THEN 'In Progress'
			WHEN tasks.percent_complete = 100 THEN 'Completed'
		END	
	,CreatedDateTime = created_date_time
	,StartDateTime = start_date_time
	,DueDateTime = due_date_time
	,CompletedDateTime = completed_date_time
FROM (
	SELECT * FROM {{ source('stg_planner_kpi', 'raw__planner_kpi_t_fact_taskshistorical') }}
	UNION ALL 
	SELECT * FROM {{ source('stg_planner_kpi', 'raw__planner_kpi_t_fact_tasks') }} ) tasks
	LEFT JOIN {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_plans') }} plans ON tasks.plan_id = plans.id
	LEFT JOIN {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_bucketplanmapping') }}  buckets ON tasks.bucket_id = buckets.id
	LEFT JOIN (
				SELECT 
					[task_id],
					[user_full_name]	
				FROM (
					SELECT
						[task_id],
						u.[user_full_name],
						ROW_NUMBER() OVER(PARTITION BY task_id ORDER BY tam.assigned_date_time DESC) last_assignee_flag
					FROM {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_taskassigneemapping') }}  tam
						LEFT JOIN {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_users') }}  u ON tam.assignee_id = u.[user_id]
						) tam2
				 WHERE tam2.last_assignee_flag = 1
	) tam3 ON tasks.id = tam3.task_id