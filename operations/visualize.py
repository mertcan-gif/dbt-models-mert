import graphviz

# --- Data: KPI formulations and their sources ---
kpi_data = [
    {
        "kpi": "Absenteeism Rate",
        "variables": ["Total Leave Days", "Total Employees"],
        "sources": {
            "Total Leave Days": "dm__hr_kpi_t_dim_personnelleave",
            "Total Employees": "dm__hr_kpi_t_dim_employees"
        }
    },
    {
        "kpi": "HR Data Completeness",
        "variables": ["Complete Records", "Total Records"],
        "sources": {
            "Complete Records": "dm__hr_kpi_t_dim_employees",
            "Total Records": "dm__hr_kpi_t_dim_employees"
        }
    },
    {
        "kpi": "High Performers (%)",
        "variables": ["High Performers", "Total Employees"],
        "sources": {
            "High Performers": "dm__hr_kpi_t_dim_employees",
            "Total Employees": "dm__hr_kpi_t_dim_employees"
        }
    },
    {
        "kpi": "Female Employee % in Management",
        "variables": ["Female Managers", "Total Managers"],
        "sources": {
            "Female Managers": "dm__hr_kpi_t_dim_employees",
            "Total Managers": "dm__hr_kpi_t_dim_employees"
        }
    },
    {
        "kpi": "Average Voluntary Turnover Rate",
        "variables": ["Voluntary Separations", "Avg. Headcount"],
        "sources": {
            "Voluntary Separations": "dm__hr_kpi_t_dim_employees",
            "Avg. Headcount": "dm__hr_kpi_t_dim_employees"
        }
    },
    {
        "kpi": "Bad Hires (%)",
        "variables": ["Involuntary Separations (<6 months)", "Total Hires"],
        "sources": {
            "Involuntary Separations (<6 months)": "dm__hr_kpi_t_dim_employees",
            "Total Hires": "dm__hr_kpi_t_dim_employees"
        }
    },
    {
        "kpi": "High Performer Turnover Rate",
        "variables": ["High Performers Who Left", "Total Separated Employees"],
        "sources": {
            "High Performers Who Left": "dm__hr_kpi_t_dim_employees",
            "Total Separated Employees": "dm__hr_kpi_t_dim_employees"
        }
    }
]

# --- Visualization Code ---

# Create a new directed graph
dot = graphviz.Digraph('KPI_Flow', comment='KPI Formulation and Sources')
dot.attr(rankdir='RL', splines='ortho', concentrate='true')

# Define styles for different node types
kpi_style = {'shape': 'box', 'style': 'filled', 'fillcolor': '#a7c7e7'}
variable_style = {'shape': 'ellipse', 'style': 'filled', 'fillcolor': '#c1e1c1'}
source_style = {'shape': 'cylinder', 'style': 'filled', 'fillcolor': '#fdfd96'}

# Use a set to keep track of sources already added to avoid duplicates
added_sources = set()

# Add nodes and edges to the graph
with dot.subgraph(name='cluster_kpis') as kpi_cluster:
    kpi_cluster.attr(label='Key Performance Indicators (KPIs)', style='filled', color='lightgrey')
    for item in kpi_data:
        kpi_name = item["kpi"]
        
        # Add KPI node
        kpi_cluster.node(kpi_name, **kpi_style)

        with dot.subgraph(name=f'cluster_{kpi_name}') as var_cluster:
            var_cluster.attr(label='Variables', style='filled', color='lightgrey')
            for var in item["variables"]:
                # Create a unique ID for the variable to handle cases where the same variable name is used in different KPIs
                var_id = f"{kpi_name}_{var}"
                
                # Add Variable node
                var_cluster.node(var_id, label=var, **variable_style)
                
                # Add edge from Variable to KPI
                dot.edge(var_id, kpi_name)

                source_dm = item["sources"].get(var)
                if source_dm:
                    if source_dm not in added_sources:
                        dot.node(source_dm, **source_style)
                        added_sources.add(source_dm)
                    
                    # Add edge from Source Data Mart to Variable
                    dot.edge(source_dm, var_id)

# --- Render and Save the Graph ---
output_filename = 'kpi_visualization.gv'
dot.render(output_filename, view=True, format='png')

print(f"Visualization saved as {output_filename}.png")