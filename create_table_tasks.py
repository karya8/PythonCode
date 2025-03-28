from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryCreateEmptyTableOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import yaml
import os
from google.cloud import bigquery

def parse_yaml_table_definition(yaml_file_path):
    """Parses YAML table definition and returns table details."""
    with open(yaml_file_path, 'r') as file:
        table_details = yaml.safe_load(file)
    return table_details

def create_table_tasks(dag, project_id, dataset_id, config_folder, table_prefix):
    """Creates Airflow tasks to create BigQuery tables, checking existence inline."""
    tasks = []
    for filename in os.listdir(config_folder):
        if filename.endswith('.yaml') and filename.startswith(table_prefix):
            file_path = os.path.join(config_folder, filename)
            table_definition = parse_yaml_table_definition(file_path)

            table_id = table_definition.get('table_id')
            table_type = table_definition.get('table_type')
            partitioning_field = table_definition.get("partitioning_field")
            time_partitioning = table_definition.get("time_partitioning")

            client = bigquery.Client(project=project_id)
            table_ref = client.dataset(dataset_id).table(table_id)

            try:
                client.get_table(table_ref) #check if table exists
                print(f"Table {table_id} already exists. Skipping creation.")
            except Exception as e:
                print(f"Table {table_id} does not exist. Creating...")
                if table_type == 'native':
                    create_table_task = BigQueryCreateEmptyTableOperator(
                        task_id=f'create_{table_id}_native',
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table_id,
                        schema_fields=table_definition.get('schema_fields'),
                        time_partitioning=time_partitioning,
                        partitioning_field=partitioning_field,
                        if_exists='ignore',
                        dag=dag,
                    )
                elif table_type == 'external':
                    create_table_task = BigQueryCreateExternalTableOperator(
                        task_id=f'create_{table_id}_external',
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_resource={
                            'tableReference': {'tableId': table_id},
                            'externalDataConfiguration': table_definition.get('external_data_configuration')
                        },
                        time_partitioning=time_partitioning,
                        partitioning_field=partitioning_field,
                        if_exists='ignore',
                        dag=dag,
                    )
                else:
                    raise ValueError(f"Invalid table type: {table_type} in {filename}")

                tasks.append(create_table_task)

    return tasks

# Retrieve configuration from Airflow Variables.
dag_config = Variable.get("dag_config", default_var='{"project_id": "your-project-id", "dataset_id": "your_dataset", "config_folder": "/path/to/your/yaml/files", "table_prefix": "table_"}')
dag_config = json.loads(dag_config)
project_id = dag_config.get("project_id")
dataset_id = dag_config.get("dataset_id")
config_folder = dag_config.get("config_folder")
table_prefix = dag_config.get("table_prefix")

with DAG(
    dag_id='bigquery_table_creation',
    schedule_interval=None,  # Run manually
    start_date=days_ago(1),
    catchup=False,
    tags=['bigquery', 'table_creation'],
) as dag:
    table_tasks = create_table_tasks(dag, project_id, dataset_id, config_folder, table_prefix)
