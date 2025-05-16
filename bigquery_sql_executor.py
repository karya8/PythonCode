from __future__ import annotations

import os
import shutil
from datetime import datetime

import yaml
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator

# --- Configuration File ---
CONFIG_FILE = 'config.yaml'

# Load configuration
with open(CONFIG_FILE, 'r') as f:
    config = yaml.safe_load(f)

BIGQUERY_CONN_ID = config.get('bigquery_conn_id', 'google_cloud_default')
SQL_FILES_PATH = config.get('sql_files_path')
ARCHIVE_PATH = config.get('archive_path')
SQL_STATEMENT_DELIMITER = config.get('sql_statement_delimiter', ';\n')

with DAG(
    dag_id='bigquery_sql_executor',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigquery', 'sql', 'dynamic'],
) as dag:
    def list_and_sort_sql_files(sql_files_path):
        sql_files = [f for f in os.listdir(sql_files_path) if f.endswith('.sql')]
        sql_files.sort()
        return sql_files

    def execute_sql_file(sql_file_name, sql_files_path, archive_path, bq_conn_id, delimiter):
        sql_file_path = os.path.join(sql_files_path, sql_file_name)
        archive_file_path = os.path.join(archive_path, sql_file_name)

        # Check if the file has already been archived
        if os.path.exists(archive_file_path):
            print(f"SQL file '{sql_file_name}' already processed and archived. Skipping.")
            return

        try:
            with open(sql_file_path, 'r') as f:
                sql_content = f.read()
            sql_statements = [stmt.strip() for stmt in sql_content.split(delimiter) if stmt.strip()]

            for i, sql in enumerate(sql_statements):
                execute_query = BigQueryExecuteQueryOperator(
                    task_id=f'execute_{sql_file_name.replace(".", "_")}_statement_{i}',
                    sql=sql,
                    gcp_conn_id=bq_conn_id,
                    use_legacy_sql=False,
                )
                execute_query.execute(context={})  # Execute immediately within the PythonOperator

            # Move the SQL file to the archive directory after successful execution
            os.makedirs(archive_path, exist_ok=True)
            shutil.move(sql_file_path, archive_file_path)
            print(f"Successfully executed and archived '{sql_file_name}'.")

        except Exception as e:
            print(f"Error executing SQL file '{sql_file_name}': {e}")
            raise

    get_sql_files = PythonOperator(
        task_id='get_sql_files',
        python_callable=list_and_sort_sql_files,
        op_kwargs={'sql_files_path': SQL_FILES_PATH},
    )

    for sql_file in list_and_sort_sql_files(SQL_FILES_PATH):
        execute_task = PythonOperator(
            task_id=f'execute_sql_{sql_file.replace(".", "_")}',
            python_callable=execute_sql_file,
            op_kwargs={
                'sql_file_name': sql_file,
                'sql_files_path': SQL_FILES_PATH,
                'archive_path': ARCHIVE_PATH,
                'bq_conn_id': BIGQUERY_CONN_ID,
                'delimiter': SQL_STATEMENT_DELIMITER,
            },
        )
        get_sql_files >> execute_task
