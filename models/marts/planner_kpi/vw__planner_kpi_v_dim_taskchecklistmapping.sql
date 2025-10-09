{{
  config(
    materialized = 'view',tags = ['planner_kpi']
    )
}}

/******************* GRAPH API **************************/

SELECT 
	RLS = p.team_group,
	TaskId = task_id,
	ChecklistItem=title, 
	ChecklistItemStatus=is_checked ,
	OrderHint = ROW_NUMBER() OVER(PARTITION BY task_id ORDER BY order_hint COLLATE Latin1_General_BIN DESC)
FROM {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_taskchecklistmapping') }} tcm
	LEFT JOIN {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_plans') }} p ON tcm.plan_id = p.id