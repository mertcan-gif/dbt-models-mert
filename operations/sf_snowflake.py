# sf_pyodata_snowflake.py

import requests
import pandas as pd
import yaml
import pyodata
from snowflake.connector.pandas_tools import write_pandas # <-- NEW IMPORT
import snowflake.connector # <-- NEW IMPORT
import ronesans_helper.config as cfg

# --- OLD SQL SERVER FUNCTIONS (TO BE REMOLED) ---
# NonAppServerConnector, sqlcol, and load_to_sql are no longer needed.

# --- NEW SNOWFLAKE LOADING FUNCTION ---
def load_to_snowflake(df: pd.DataFrame, table_name: str, schema: str, database: str, warehouse: str):
    """
    Connects to Snowflake and loads a pandas DataFrame into a specified table.
    This function replaces the old load_to_sql function.
    It uses Snowflake's optimized `write_pandas` for efficient loading.
    """
    try:
        # NOTE: Store these credentials securely, e.g., in Prefect Blocks or environment variables
        conn = snowflake.connector.connect(
            user=cfg.dict_all_server['SNOWFLAKE']['username'],
            password=cfg.dict_all_server['SNOWFLAKE']['password'],
            account=cfg.dict_all_server['SNOWFLAKE']['account'],
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        print(f"Successfully connected to Snowflake. Preparing to write to {database}.{schema}.{table_name}")

        # Use write_pandas to load the data. `overwrite=True` mimics the `if_exists='replace'` behavior.
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name.upper(), # Snowflake table names are typically uppercase
            auto_create_table=True, # Creates the table if it doesn't exist
            overwrite=True # Drops and recreates the table
        )
        print(f"Snowflake load complete. Success: {success}, Rows: {nrows}")

    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Snowflake connection closed.")

# --- EXISTING FUNCTIONS (get_config, recursive_iteration, etc.) ---
# These functions remain the same as in your original script.
# get_config, get_odata_entity_results, pick_list_finder, etc.

# --- MODIFIED DATA RETRIEVAL FUNCTION ---
def get_data_from_odata(username, password, table, file_path, service_url):
    """
    This is the original data retrieval function with data quality fixes integrated.
    """
    auth_info = (username, password)
    session = requests.Session()
    session.auth = auth_info
    _client = pyodata.Client(service_url, session)

    ############ CREATE CONFIG ############ 
    _table = table
    _file_path = file_path
    config = get_config(_file_path, _table)

    ############ CREATE PYODATA ############ 
    entity_instance = get_odata_entity_results(
        _client, config['entity_set_name'],
        config['select_parameters'],
        config['filter_parameters'],
        config['expand_parameters'],
        config['from_date_parameter']
    )

    ############ CREATE DATAFRAME ############ 
    df = turn_odata_entity_to_dataframe(entity_instance, config['columns'])
    df = rename_columns(df, config['columns'])
    
    # --- NEW DATA QUALITY FIXES ---

    # 1. FIX "Year 1753" ISSUE:
    # Convert any 'Not a Time' (NaT) values in datetime columns to None.
    # Snowflake will correctly interpret None as NULL, preventing default minimum dates.
    for col in df.select_dtypes(include=['datetime64[ns, UTC]']).columns:
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
    print("Cleaned NaT values from datetime columns.")

    # 2. FIX "Null Rows" ISSUE:
    # Define a list of essential columns that cannot be null for a row to be valid.
    # IMPORTANT: You must define which columns are considered primary keys.
    # For example, for an employee table, this might be ['user_id_en'].
    primary_key_columns = [value for key, value in config['columns'].items() if "userId" in key or "externalCode" in key]
    
    if primary_key_columns:
        initial_rows = len(df)
        df.dropna(subset=primary_key_columns, inplace=True)
        final_rows = len(df)
        print(f"Removed {initial_rows - final_rows} rows with null primary keys.")
    else:
        print("No primary key columns defined for null row check.")
    
    # --- END OF DATA QUALITY FIXES ---
    
    df = df.astype({col: 'str' for col in df.select_dtypes(include=['object']).columns})
    return df

# Helper functions like get_config, get_odata_entity_results, turn_odata_entity_to_dataframe, etc.
# should be included here as they were in the original file.