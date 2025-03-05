import json
import os
import zipfile
import requests
from datetime import datetime, timedelta
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.secret_manager.hooks.secret_manager import SecretsManagerHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import DagModel, Variable
from airflow.utils.db import create_session
from airflow.utils.state import State

def get_credentials(environment, config):
    """Retrieves credentials (same as before)."""
    if environment == 'production':
        try:
            secret_manager_hook = SecretsManagerHook()
            client_id = secret_manager_hook.get_secret(secret_id=config['api_credentials']['secret_manager_client_id'])
            client_secret = secret_manager_hook.get_secret(secret_id=config['api_credentials']['secret_manager_client_secret'])
            return client_id, client_secret
        except Exception as e:
            raise Exception(f"Error retrieving secrets from Secret Manager: {e}")
    elif environment == 'staging':
        return config['api_credentials']['client_id'], config['api_credentials']['client_secret']
    else:
        raise ValueError("Invalid environment.")


def get_token(client_id, client_secret, auth_url):
    """Fetches an access token (same as before)."""
    try:
        response = requests.post(
            auth_url,
            data={"grant_type": "client_credentials"},
            auth=(client_id, client_secret),
            timeout=30
        )
        response.raise_for_status()
        return response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error getting token: {e}")
    except (KeyError, ValueError) as e:
        raise Exception(f"Error parsing token response: {e}, Response: {response.text}")

def get_scope_id(token, scope_url, headers=None):
    """Fetches the scope ID (same as before)."""
    _headers = {"Authorization": f"Bearer {token}"}
    if headers:
        _headers.update(headers)
    try:
        response = requests.get(scope_url, headers=_headers, timeout=30)
        response.raise_for_status()
        return response.json()["scope_id"]
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error getting scope ID: {e}")
    except (KeyError, ValueError) as e:
        raise Exception(f"Error parsing scope ID response: {e}, Response: {response.text}")

def download_and_upload_zip(token, scope_id, data_url, timestamp, bucket_name, destination_blob_prefix, bq_table, **kwargs):
    """Downloads, uploads, and logs to BigQuery.

    (Same as before, but uses f-strings for clarity)
    """
    formatted_timestamp = timestamp.strftime("%Y-%m-%d-%H-%M-%S")
    full_data_url = f"{data_url}?timestamp={formatted_timestamp}"
    destination_blob_name = f"{destination_blob_prefix}/{formatted_timestamp}.zip"

    _headers = {"Authorization": f"Bearer {token}", "X-Scope-Id": str(scope_id)}

    try:
        response = requests.get(full_data_url, headers=_headers, stream=True, timeout=60)
        response.raise_for_status()

        zip_data = BytesIO(response.content)
        if not zipfile.is_zipfile(zip_data):
            raise Exception("Response is not a valid zip file.")
        zip_data.seek(0)

        gcs_hook = GCSHook()
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=destination_blob_name,
            data=zip_data.read(),
            mime_type='application/zip',
        )
        print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")

        insert_row_bq(bq_table, data_url, timestamp, "SUCCESS", "")

    except Exception as e:
        error_message = str(e)
        print(f"Error processing {full_data_url}: {error_message}")
        insert_row_bq(bq_table, data_url, timestamp, "FAILURE", error_message)
        raise


def insert_row_bq(bq_table, data_url, timestamp, status, error_message):
    """Inserts a row into BigQuery (same as before)."""
    insert_job = BigQueryInsertJobOperator(
        task_id=f"insert_bq_log_{timestamp.strftime('%Y%m%d%H%M%S')}",
        configuration={
            "load": {
                "sourceUris": [],
                "table": bq_table,
                "schemaUpdateOptions": [],
                "writeDisposition": "WRITE_APPEND",
                "schema": {
                    "fields": [
                        {"name": "data_url", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
                        {"name": "status", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "error_message", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "load_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    ],
                },
                "load_task_config": {
                    "ignoreUnknownValues": False,
                }
            }
        },
        rows=[
            {
                "data_url": data_url,
                "timestamp": timestamp.isoformat(),
                "status": status,
                "error_message": error_message,
                "load_ts": datetime.utcnow().isoformat(),
            }
        ],
    )
    insert_job.execute(context={})


def process_data_url(data_url, scope_id, token, bucket_name, destination_blob_prefix, bq_table, dag_run):
    """Processes a *single* data URL, handling timestamps and retries.

    This function is now specific to a single data URL.
    """
    last_successful_time = dag_run.get_previous_execution_date(state=State.SUCCESS)
    if last_successful_time is None:
        last_successful_time = dag_run.start_date - timedelta(hours=2)

    current_time = dag_run.execution_date
    five_min_interval = timedelta(minutes=5)

    failed_timestamps = get_failed_timestamps(bq_table, data_url, last_successful_time, current_time)

    current_timestamp = last_successful_time + five_min_interval
    while current_timestamp <= current_time:
        if current_timestamp not in failed_timestamps:
            failed_timestamps.append(current_timestamp)
        current_timestamp += five_min_interval
    for timestamp in failed_timestamps:
        download_and_upload_zip(
            token=token,
            scope_id=scope_id,
            data_url=data_url,
            timestamp=timestamp,
            bucket_name=bucket_name,
            destination_blob_prefix=destination_blob_prefix,
            bq_table=bq_table,
        )



def get_failed_timestamps(bq_table, data_url, start_time, end_time):
    """Retrieves failed timestamps (same as before)."""
    from google.cloud import bigquery

    client = bigquery.Client()
    query = f"""
        SELECT timestamp
        FROM `{bq_table}`
        WHERE data_url = @data_url
        AND status = 'FAILURE'
        AND timestamp >= @start_time
        AND timestamp <= @end_time
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", start_time),
            bigquery.ScalarQueryParameter("end_time", "TIMESTAMP", end_time),
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    return [row.timestamp for row in results]



def create_dag(dag_config):
    """Creates a DAG dynamically for a *single* data URL."""

    dag_id = dag_config['dag_id']
    environment = dag_config['environment']
    data_url = dag_config['data_url']  # Single data URL

    with create_session() as session:
        dag_exists = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
    if dag_exists:
        print(f"DAG '{dag_id}' already exists.  Skipping creation.")
        return None

    default_args = dag_config.get('default_args', {})
    schedule_interval = dag_config.get('schedule_interval', '@hourly')

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=datetime.fromisoformat(dag_config.get('start_date', '2023-10-26')),
        catchup=dag_config.get('catchup', False),
        tags=dag_config.get('tags', []),
    )

    with dag:
        start = DummyOperator(task_id="start")
        end = DummyOperator(task_id="end")

        client_id, client_secret = get_credentials(environment, dag_config)

        get_token_task = PythonOperator(
            task_id="get_token",
            python_callable=get_token,
            op_kwargs={
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_url": dag_config['api_endpoints']['auth_url'],
            },
        )

        get_scope_id_task = PythonOperator(
            task_id="get_scope_id",
            python_callable=get_scope_id,
            op_kwargs={
                "scope_url": dag_config['api_endpoints']['scope_url'],
            },
            provide_context=True,
            op_kwargs_from_xcom={'token': "{{ ti.xcom_pull(task_ids='get_token') }}"},
        )
        process_data_task = PythonOperator(
            task_id="process_data_url",  # Singular, as it's for one URL
            python_callable=process_data_url,
            op_kwargs={
                "data_url": data_url,  # Pass the single data URL
                "bucket_name": dag_config['gcs_bucket'],
                "destination_blob_prefix": dag_config['destination_blob_name'],
                "bq_table": dag_config['bigquery_table'],
            },
            provide_context=True,
            op_kwargs_from_xcom={
                'token': "{{ ti.xcom_pull(task_ids='get_token') }}",
                'scope_id': "{{ ti.xcom_pull(task_ids='get_scope_id') }}"
            },
        )

        start >> get_token_task >> get_scope_id_task >> process_data_task >> end

    return dag


def create_dags_from_config(config_folder):
    """Creates DAGs from *multiple* config files, one DAG per config."""
    for filename in os.listdir(config_folder):
        if filename.endswith(".json"):
            config_path = os.path.join(config_folder, filename)
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)
                # Iterate through data_url_configs, creating a DAG for each
                for data_url_config in config['data_url_configs']:
                    # Create a unique DAG ID based on the data URL
                    dag_id = f"{config['base_dag_id']}_{data_url_config['name']}"

                    # Combine base config with data URL specific config
                    dag_config = {
                        "dag_id": dag_id,
                        "environment": config['environment'],
                        "schedule_interval": config.get('schedule_interval', '@hourly'),
                        "start_date": config.get('start_date', '2023-10-26'),
                        "catchup": config.get('catchup', False),
                        "tags": config.get('tags', []) + [data_url_config['name']], # Add data URL name as tag
                        "default_args": config.get('default_args', {}),
                        "api_credentials": config['api_credentials'],
                        "api_endpoints": config['api_endpoints'],
                        "data_url": data_url_config['url'],  # The specific data URL
                        "gcs_bucket": config['gcs_bucket'],
                        "destination_blob_name": data_url_config.get('destination_blob_name', config['destination_blob_name']),
                        "bigquery_table": config['bigquery_table'],
                    }
                    dag = create_dag(dag_config)
                    if dag:
                        globals()[dag.dag_id] = dag
                        print(f"DAG '{dag.dag_id}' created successfully from {filename}.")

            except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
                print(f"Error processing config file {filename}: {e}")
            except Exception as e:
                print(f"Unexpected error creating DAG from {filename}: {e}")

if __name__ == "__main__":
    config_folder = "config"
    create_dags_from_config(config_folder)