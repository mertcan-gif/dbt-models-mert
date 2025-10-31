I want to create an md file that creates a relationship table for data sources:

dm__hr_kpi_t_dim_employees
dm__hr_kpi_t_dim_employee_assigned_items
dm__hr_kpi_t_dim_employee_education
dm__hr_kpi_t_dim_employee_language
dm__hr_kpi_t_dim_personnelleave
dm__hr_kpi_t_fact_employee_experience

which have schema
Target (Data Mart),	Target (Column), Source (Entity), Source Field

which will use the information in /models folder. 

analyze all files under the folder to create the required schema.