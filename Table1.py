from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryCreateEmptyTableOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import yaml
import os
from google.cloud import bigquery
import json

def parse_yaml_table_definition(yaml_file_path):
    """Parses YAML table definition and returns table details."""
    with open(yaml_file_path, 'r') as file:
        table_details = yaml.safe_load(file)
    return table_details

def create_table_tasks(dag, config_file_path):
    """Creates Airflow tasks to create BigQuery tables. Reads all configs from file"""

    with open(config_file_path, 'r') as file:
        dag_config = json.load(file)

    project_id = dag_config.get("project_id")
    dataset_id = dag_config.get("dataset_id")
    config_folder = dag_config.get("config_folder")
    table_prefix = dag_config.get("table_prefix")

    tasks = []
    for filename in os.listdir(config_folder):
        if filename.endswith('.yaml') and filename.startswith(table_prefix):
            file_path = os.path.join(config_folder, filename)
            table_definition = parse_yaml_table_definition(file_path)

            table_id = table_definition.get('table_id')
            table_type = table_definition.get('table_type')
            table_reference_config = table_definition.get("tableReference")
            schema_config = table_definition.get("schema")
            time_partitioning_config = table_definition.get("timePartitioning")
            external_data_config = table_definition.get("externalDataConfiguration")

            table_resource = {
                "tableReference": {
                    "projectId": table_reference_config.get("projectId", project_id) if table_reference_config else project_id,
                    "datasetId": table_reference_config.get("datasetId", dataset_id) if table_reference_config else dataset_id,
                    "tableId": table_id
                }
            }
            if schema_config:
                table_resource["schema"] = schema_config
            if time_partitioning_config:
                table_resource["timePartitioning"] = time_partitioning_config
            if external_data_config and table_type == 'external':
                table_resource["externalDataConfiguration"] = external_data_config

            if table_type == 'native':
                if "schema" not in table_resource:
                    raise ValueError(f"schema must be defined for native table {table_id} in {filename}")
                create_table_task = BigQueryCreateEmptyTableOperator(
                    task_id=f'create_{table_id}_native',
                    table_resource=table_resource,
                    if_exists='ignore',
                    dag=dag,
                )
            elif table_type == 'external':
                if "externalDataConfiguration" not in table_resource:
                    raise ValueError(f"externalDataConfiguration must be defined for external table {table_id} in {filename}")
                create_table_task = BigQueryCreateExternalTableOperator(
                    task_id=f'create_{table_id}_external',
                    table_resource=table_resource,
                    if_exists='ignore',
                    dag=dag,
                )
            else:
                raise ValueError(f"Invalid table type: {table_type} in {filename}")

            tasks.append(create_table_task)

    return tasks

# Retrieve configuration from config JSON file.
config_file_path = "/path/to/your/dag_config.json"  # Replace with your actual config file path

with DAG(
    dag_id='bigquery_table_creation_separate_attrs',
    schedule_interval=None,  # Run manually
    start_date=days_ago(1),
    catchup=False,
    tags=['bigquery', 'table_creation', 'separate_attrs'],
) as dag:
    table_tasks = create_table_tasks(dag, config_file_path)

    end_task = DummyOperator(
        task_id='all_tables_created',
        dag=dag,
    )

    if table_tasks:
        for task in table_tasks:
            task >> end_task
