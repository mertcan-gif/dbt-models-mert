{{
  config(
    materialized = 'view',tags = ['planner_kpi']
    )
}}

/******************* GRAPH API **************************/

SELECT
	tlm.task_id AS TaskId,
	pc.value AS [Label]
FROM {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_tasklabelmapping') }} tlm
	LEFT JOIN {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_planconfigurations') }} pc ON tlm.plan_id = pc.plan_id AND pc.category = tlm.category_label
