{{
  config(
    materialized = 'view',tags = ['planner_kpi']
    )
}}

/******************* GRAPH API **************************/

SELECT 
	*
FROM {{ source('stg_planner_kpi', 'raw__planner_kpi_t_dim_rlstable') }} rls
