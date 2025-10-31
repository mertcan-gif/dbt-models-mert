import graphviz

# --- Data: Detailed Data Lineage for Key KPIs ---
# This data is a simplified representation of the dependencies found in your dbt project.
lineage_data = {
    "Absenteeism Rate": {
        "mart": "dm__hr_kpi_t_dim_personnelleave",
        "staging": "stg__hr_kpi_v_dim_usedremainingleave",
        "sources": ["raw__hr_kpi_t_sf_employee_time", "raw__hr_kpi_t_sf_timeaccountdetails"]
    },
    "High Performer Turnover Rate": {
        "mart": "dm__hr_kpi_t_dim_employees",
        "staging": "stg__hr_kpi_t_dim_employees_union_raw",
        "sources": ["raw__hr_kpi_t_sf_employees", "raw__hr_kpi_t_sf_newsf_employees"]
    },
    "Bad Hires (%)": {
        "mart": "dm__hr_kpi_t_dim_employees",
        "staging": "stg__hr_kpi_t_dim_employees_union_raw",
        "sources": ["raw__hr_kpi_t_sf_employees", "raw__hr_kpi_t_sf_newsf_employees"]
    },
    "Employee Experience (in days)": {
        "mart": "dm__hr_kpi_t_fact_employee_experience",
        "staging": "stg__hr_kpi_t_dim_sf_rls", # Experience data is directly from source in this case
        "sources": ["raw__hr_kpi_t_sf_newsf_outsideworkexperience", "raw__hr_kpi_t_sf_newsf_insideworkexperience"]
    }
}


# --- Visualization Code ---

# Create a new directed graph
dot = graphviz.Digraph('Data_Lineage', comment='Data Lineage from Source to KPI')
dot.attr(rankdir='LR', splines='ortho', concentrate='true', label='Data Lineage from Source to KPI', fontsize='20')

# Define styles for different node types
kpi_style = {'shape': 'doubleoctagon', 'style': 'filled', 'fillcolor': '#a7c7e7', 'fontname': 'Helvetica', 'fontsize': '12'}
mart_style = {'shape': 'box', 'style': 'filled', 'fillcolor': '#c1e1c1', 'fontname': 'Helvetica', 'fontsize': '12'}
staging_style = {'shape': 'ellipse', 'style': 'filled', 'fillcolor': '#fdfd96', 'fontname': 'Helvetica', 'fontsize': '10'}
source_style = {'shape': 'cylinder', 'style': 'filled', 'fillcolor': '#ffb347', 'fontname': 'Helvetica', 'fontsize': '10'}

# Use sets to keep track of nodes already added to avoid duplicates
added_kpis = set()
added_marts = set()
added_staging = set()
added_sources = set()

for kpi, details in lineage_data.items():
    # Add KPI node
    if kpi not in added_kpis:
        dot.node(kpi, **kpi_style)
        added_kpis.add(kpi)

    # Add Data Mart node
    mart = details["mart"]
    if mart not in added_marts:
        dot.node(mart, **mart_style)
        added_marts.add(mart)
    dot.edge(mart, kpi)

    # Add Staging node
    staging = details["staging"]
    if staging not in added_staging:
        dot.node(staging, **staging_style)
        added_staging.add(staging)
    dot.edge(staging, mart)

    # Add Source nodes and edges
    for source in details["sources"]:
        if source not in added_sources:
            dot.node(source, **source_style)
            added_sources.add(source)
        dot.edge(source, staging)

# --- Render and Save the Graph ---
output_filename = 'data_lineage_visualization.gv'
dot.render(output_filename, view=True, format='png')

print(f"Data lineage visualization saved as {output_filename}.png")