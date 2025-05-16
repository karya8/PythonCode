from __future__ import annotations

import os
import shutil
from datetime import datetime

try:
    import yaml
except ImportError:
    print("Error: The 'pyyaml' module is not installed. Please install it using 'pip install pyyaml'")
    #  You might want to add a sys.exit(1) here if you want the DAG to fail immediately.
    #  Without it, the DAG might continue to run and fail later when it actually tries to use yaml.
    #  For now, I'll let it proceed, but the user will see the error.

try:
    from airflow import DAG
    from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
    from airflow.operators.python import PythonOperator
    from airflow.models.baseoperator import chain
except ImportError:
    print("Error: Apache Airflow is not installed. Please install it using 'pip install apache-airflow'")
    #  Similar to the yaml import error, you might want to add a sys.exit(1) here.
    #  Without it, the DAG might continue to run and fail later.


# --- Configuration File ---
CONFIG_FILE = 'config.yaml'  # Moved to the top for clarity

# Load configuration
try:
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    print(f"Error: Configuration file '{CONFIG_FILE}' not found.  Please create it.")
    #  Similar to the yaml import error, you might want to sys.exit(1) here.
    config = {}  # Provide an empty dict to avoid later errors, but the DAG will likely fail.

# BIGQUERY_CONN_ID = config.get('bigquery_conn_id', 'google_cloud_default') # Removed
PROJECT_ID = config.get('bigquery_project')  # Added
DATASET_NAME = config.get('bigquery_dataset')  # Added
SQL_FILES_PATH = config.get('sql_files_path')
ARCHIVE_PATH = config.get('archive_path')
SQL_STATEMENT_DELIMITER = config.get('sql_statement_delimiter', ';\n')
FILE_ORDERING = config.get('file_ordering')  # New parameter for explicit file order

if not PROJECT_ID:
    print("Error: 'bigquery_project' is not defined in the configuration file.")
    # Consider adding sys.exit(1) here

if not DATASET_NAME:
    print("Error: 'bigquery_dataset' is not defined in the configuration file.")
    # Consider adding sys.exit(1) here


def list_and_sort_sql_files(sql_files_path, file_ordering=None):
    """
    Lists SQL files in the specified directory and sorts them.

    Args:
        sql_files_path (str): The path to the directory containing the SQL files.
        file_ordering (list, optional): A list specifying the order of the SQL files.
            If None, the files are sorted alphabetically. Defaults to None.

    Returns:
        list: A sorted list of SQL file names.
    """
    sql_files = [f for f in os.listdir(sql_files_path) if f.endswith('.sql')]
    if file_ordering:
        # Validate that all files in file_ordering exist
        missing_files = [f for f in file_ordering if f not in sql_files]
        if missing_files:
            raise ValueError(
                f"The following files specified in file_ordering are missing: {missing_files}"
            )
        # Use the order from config, and if there are files not in the list, append them in sorted order.
        ordered_files = [f for f in file_ordering if f in sql_files]
        remaining_files = sorted([f for f in sql_files if f not in file_ordering])
        return ordered_files + remaining_files

    else:
        sql_files.sort()  # Sort alphabetically if no explicit order is provided.
        return sql_files



def execute_sql_file(sql_file_name, sql_files_path, archive_path, project_id, dataset_name, delimiter):
    """
    Executes the SQL statements in a given file and moves the file to the archive directory.

    Args:
        sql_file_name (str): The name of the SQL file to execute.
        sql_files_path (str): The path to the directory containing the SQL files.
        archive_path (str): The path to the directory where the SQL files should be archived.
        project_id (str): The BigQuery project ID.  Added
        dataset_name (str): The BigQuery dataset name. Added
        delimiter (str): The delimiter used to separate SQL statements within the file.
    """
    sql_file_path = os.path.join(sql_files_path, sql_file_name)
    archive_file_path = os.path.join(archive_path, sql_file_name)

    # Check if the file has already been archived
    if os.path.exists(archive_file_path):
        print(f"SQL file '{sql_file_name}' already processed and archived. Skipping.")
        return  # Early return to skip execution

    try:
        with open(sql_file_path, 'r') as f:
            sql_content = f.read()
        sql_statements = [stmt.strip() for stmt in sql_content.split(delimiter) if stmt.strip()]

        for i, sql in enumerate(sql_statements):
            execute_query_operator = BigQueryExecuteQueryOperator(  # Corrected variable name
                task_id=f'execute_{sql_file_name.replace(".", "_")}_statement_{i}',
                sql=sql,
                # gcp_conn_id=bq_conn_id, # Removed
                project_id=project_id,  # Added
                dataset_id=dataset_name,  # Added
                use_legacy_sql=False,
            )
            execute_query_operator.execute(context={})  # Execute the operator

        # Move the SQL file to the archive directory after successful execution
        os.makedirs(archive_path, exist_ok=True)
        shutil.move(sql_file_path, archive_file_path)
        print(f"Successfully executed and archived '{sql_file_name}'.")

    except Exception as e:
        print(f"Error executing SQL file '{sql_file_name}': {e}")
        raise  # Re-raise the exception to mark the task as failed in Airflow



with DAG(
    dag_id='bigquery_sql_executor',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Run on demand
    catchup=False,  # Do not backfill
    tags=['bigquery', 'sql', 'dynamic'],
) as dag:
    get_sql_files = PythonOperator(
        task_id='get_sql_files',
        python_callable=list_and_sort_sql_files,
        op_kwargs={'sql_files_path': SQL_FILES_PATH, 'file_ordering': FILE_ORDERING},
        provide_context=True,  # Add this if you need to access Airflow context.
    )

    # Use a list to hold the execute tasks
    execute_tasks = []
    for sql_file in list_and_sort_sql_files(SQL_FILES_PATH, FILE_ORDERING):
        execute_task = PythonOperator(
            task_id=f'execute_sql_{sql_file.replace(".", "_")}',
            python_callable=execute_sql_file,
            op_kwargs={
                'sql_file_name': sql_file,
                'sql_files_path': SQL_FILES_PATH,
                'archive_path': ARCHIVE_PATH,
                'project_id': PROJECT_ID, # Added
                'dataset_name': DATASET_NAME, # Added
                'delimiter': SQL_STATEMENT_DELIMITER,
            },
            provide_context=True,  # Add this if you need to access Airflow context.
        )
        execute_tasks.append(execute_task)

    # Use chain to set dependencies
    if execute_tasks:
      chain(get_sql_files, *execute_tasks)

---------------------------yaml file ------------------------

bigquery_project: 'your-gcp-project-id'  # Replace with your GCP project ID
bigquery_dataset: 'your_dataset_name'  # Replace with your BigQuery dataset name
sql_files_path: '/path/to/your/sql_files'
archive_path: '/path/to/your/sql_archive'
sql_statement_delimiter: ";\n"
file_ordering:  # Optional:  Specify the order of the files.
  - '01_create_tables.sql'
  - '02_insert_data.sql'
  - '03_alter_table.sql'
  - '04_views.sql'
