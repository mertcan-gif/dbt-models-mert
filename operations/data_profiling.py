import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

def run_data_profiling():
    """
    Connects to the database, loads the employee data, and performs
    a series of data quality and profiling checks.
    """
    # --- 1. Database Connection ---
    print("--- Starting Data Profiling ---")
    server = '172.22.74.254'
    database = 'aws_stage'
    username = 'extmertcan.coskun'
    password = 'id3bGWpkLeDea4EAE4W9'
    table_name = 'raw__hr_kpi_t_sf_newsf_employees'
    schema_name = 'sf_odata'

    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=yes;'
    )

    cnxn = None  # Initialize connection to None
    try:
        cnxn = pyodbc.connect(conn_str)
        print("Connection to database established successfully!")

        # --- 2. Load Full Data from Database ---
        query = f"SELECT * FROM [{schema_name}].[{table_name}]"
        print("Loading full dataset from the database... This may take some time.")
        
        df = pd.read_sql(query, cnxn)
        print("Data loaded successfully.")
        print(f"{len(df)} rows fetched.\n")

        print("--- Basic DataFrame Info ---")
        df.info()
        print("\n" + "="*80 + "\n")

        # --- 3. Date Field Analysis ---
        print("--- Analyzing Date Ranges ---")
        date_cols = ['start_date', 'job_start_date', 'job_end_date', 'end_date', 'date_of_birth', 
                     'initial_hire_date', 'seniority_base_date', 'db_upload_timestamp']
        date_summary = {}

        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                min_date = df[col].min()
                max_date = df[col].max()
                date_summary[col] = {'Minimum Date': min_date, 'Maximum Date': max_date}

        summary_df = pd.DataFrame.from_dict(date_summary, orient='index')
        print(summary_df)
        print("\n" + "="*80 + "\n")

        # --- 4. Categorical Data Analysis ---
        print("--- Analyzing Categorical Value Distributions ---")
        categorical_cols_to_analyze = ['employee_status_en', 'workplace_en', 'payroll_company', 'gender']
        
        for col in categorical_cols_to_analyze:
            if col in df.columns:
                print(f"\n--- Value Counts for '{col}' ---")
                print(df[col].value_counts(dropna=False).head(15))
        print("\n" + "="*80 + "\n")

        # --- 5. Numeric Data Profiling ---
        print("--- Descriptive Statistics for Numeric Columns ---")
        numeric_df = df.select_dtypes(include=np.number)
        
        if not numeric_df.empty:
            print(numeric_df.describe().T)
        else:
            print("No numeric columns found in the DataFrame.")
        print("\n" + "="*80 + "\n")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if cnxn:
            cnxn.close()
            print("Database connection closed.")

# Run the main function
if __name__ == "__main__":
    run_data_profiling()
