DAG_TEMPLATE = """
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import os
import sys

# Dynamically add the dags folder to sys.path
dag_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if dag_folder not in sys.path:
    sys.path.append(dag_folder)

from {module_name} import process_data_url, get_credentials, get_token, get_scope_id, download_and_upload_zip, insert_record, delete_record_if_exists, update_or_insert_success_record, get_timestamps_by_status, get_last_successful_timestamp


with DAG(
    dag_id='{dag_id}',
    default_args={default_args},
    schedule_interval='{schedule_interval}',
    catchup={catchup},
    tags={tags},
    max_active_runs={max_active_runs}
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    process_task = PythonOperator(
        task_id='process_{data_url_name}',
        python_callable=process_data_url,
        op_kwargs={{
            'config': {config},
            'data_url_config': {data_url_config},
            'current_time': datetime.utcnow()
        }},
    )
    start >> process_task >> end
"""

@provide_session
def create_dags_from_config(config_file="config.json", session=None):
    """Loads config and creates DAG files."""
    try:
        with open(config_file, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config file: {e}")
        return

    dags_folder = conf.get('core', 'dags_folder')
    existing_dag_files = [
        f for f in os.listdir(dags_folder)
        if f.startswith("dynamic_dag_") and f.endswith(".py")
    ]
    processed_dags = set()

    # Get the module name (assuming this script is in the dags folder)
    module_name = os.path.splitext(os.path.basename(__file__))[0]


    for data_url_config in config['data_urls']:
        dag_id = f"dynamic_dag_{data_url_config['name']}"
        dag_file_name = f"{dag_id}.py"
        dag_file_path = os.path.join(dags_folder, dag_file_name)
        processed_dags.add(dag_file_name)

        default_args = {
            'owner': config.get('owner', 'airflow'),
            'depends_on_past': config.get('depends_on_past', False),
            'start_date': datetime.fromisoformat(config.get('start_date', '2023-11-01T00:00:00')),
            'email': config.get('email', []),
            'email_on_failure': config.get('email_on_failure', False),
            'email_on_retry': config.get('email_on_retry', False),
            'retries': config.get('retries', 1),
            'retry_delay': timedelta(minutes=config.get('retry_delay_minutes', 5)),
        }

        # Create the DAG file content from the template
        dag_file_content = DAG_TEMPLATE.format(
            dag_id=dag_id,
            default_args=repr(default_args),  # Use repr to format as a Python dictionary
            schedule_interval=repr(config.get('schedule_interval', '@hourly')),
            catchup=config.get('catchup', False),
            tags=config.get('tags', ["data_pipeline"]),
            max_active_runs=config.get('max_active_runs', 1),
            data_url_name=data_url_config["name"],
            config=repr(config),  # Format the config as a string
            data_url_config=repr(data_url_config), # Format the data_url_config
            module_name=module_name,
        )

        # Write the DAG file
        try:
            with open(dag_file_path, "w") as f:
                f.write(dag_file_content)
            print(f"Created DAG file: {dag_file_path}")
        except Exception as e:
            print(f"Error writing DAG file: {e}")
            #  Don't re-raise, continue to the next DAG

    # Clean up old DAG files
    for old_dag_file in existing_dag_files:
        if old_dag_file not in processed_dags:
            try:
                os.remove(os.path.join(dags_folder, old_dag_file))
                print(f"Deleted old DAG file: {old_dag_file}")
            except Exception as e:
                print(f"Error deleting old DAG file {old_dag_file}: {e}")

# --- Main DAG to Trigger Dynamic DAG Creation ---

dag_creator_dag_id = 'dag_creator'

with DAG(
    dag_id=dag_creator_dag_id,
    schedule_interval=None,  # Or '@once', or a specific schedule
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dag_management'],
) as dag_creator:
    create_dags_task = PythonOperator(
        task_id='create_dynamic_dags',
        python_callable=create_dags_from_config,
        op_kwargs={'config_file': os.path.join(conf.get('core', 'dags_folder'), 'config.json')}
    )
