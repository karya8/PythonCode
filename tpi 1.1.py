import json
import os
import zipfile
import requests
from datetime import datetime, timedelta
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryExecuteQueryOperator  # Import BQ operator
from airflow.models import DagModel, DagBag
from airflow.utils.db import create_session
from airflow.utils.state import State
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import storage, secretmanager


def get_credentials(environment, config):
    """Retrieves credentials (using Secret Manager)."""
    if environment == 'production':
        try:
            client = secretmanager.SecretManagerServiceClient()
            client_id_secret_name = f"projects/{config['api_credentials']['project_id']}/secrets/{config['api_credentials']['secret_manager_client_id']}/versions/latest"
            client_secret_secret_name = f"projects/{config['api_credentials']['project_id']}/secrets/{config['api_credentials']['secret_manager_client_secret']}/versions/latest"

            client_id_response = client.access_secret_version(request={"name": client_id_secret_name})
            client_secret_response = client.access_secret_version(request={"name": client_secret_secret_name})

            client_id = client_id_response.payload.data.decode("UTF-8")
            client_secret = client_secret_response.payload.data.decode("UTF-8")

            return (client_id, client_secret), config['api_credentials']['project_id']

        except Exception as e:
            raise Exception(f"Error retrieving secrets from Secret Manager: {e}")
    elif environment == 'staging':
        return (config['api_credentials']['client_id'],
                config['api_credentials']['client_secret']), config['api_credentials']['project_id']
    else:
        raise ValueError("Invalid environment.")


def get_token(credentials, auth_url):
    """Fetches an access token."""
    try:
        if isinstance(credentials, tuple):
            client_id, client_secret = credentials
            response = requests.post(
                auth_url,
                data={"grant_type": "client_credentials"},
                auth=(client_id, client_secret),
                timeout=30
            )
            response.raise_for_status()
            token = response.json()["access_token"]
        else:
            raise TypeError("Invalid credentials type.")

        return token

    except requests.exceptions.RequestException as e:
        raise Exception(f"Error getting token: {e}")
    except (KeyError, ValueError) as e:
        raise Exception(f"Error parsing token response: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred getting the token: {e}")

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

def download_and_upload_zip(credentials, project_id, scope_id, data_url, timestamp, bucket_name, destination_blob_prefix, bq_table, **kwargs):
    """Downloads, uploads, logs, and deletes failed records."""
    formatted_timestamp = timestamp.strftime("%Y-%m-%d-%H-%M-%S")
    full_data_url = f"{data_url}?timestamp={formatted_timestamp}"
    destination_blob_name = f"{destination_blob_prefix}/{formatted_timestamp}.zip"

    token = get_token(credentials, kwargs['auth_url'])
    _headers = {"Authorization": f"Bearer {token}", "X-Scope-Id": str(scope_id)}

    try:
        response = requests.get(full_data_url, headers=_headers, stream=True, timeout=60)
        response.raise_for_status()

        zip_data = BytesIO(response.content)
        if not zipfile.is_zipfile(zip_data):
            raise Exception("Response is not a valid zip file.")
        zip_data.seek(0)

        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_file(zip_data, content_type='application/zip')

        print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")
        #insert_row_bq(bq_table, data_url, timestamp, "SUCCESS", "", credentials, project_id) # No longer log success here

        # Delete the failed record (if it exists) after successful upload.
        delete_failed_record(bq_table, data_url, timestamp, credentials, project_id)


    except Exception as e:
        error_message = str(e)
        print(f"Error processing {full_data_url}: {error_message}")
        insert_failed_record(bq_table, data_url, timestamp, error_message, credentials, project_id) # Log only failures
        raise




def insert_failed_record(bq_table, data_url, timestamp, error_message, credentials, project_id):
    """Inserts a failure record into BigQuery."""
    insert_job = BigQueryInsertJobOperator(
        task_id=f"insert_bq_failed_{timestamp.strftime('%Y%m%d%H%M%S')}",
        project_id=project_id,
        configuration={
            "load": {
                "sourceUris": [],
                "table": bq_table,
                "schemaUpdateOptions": [],
                "writeDisposition": "WRITE_APPEND",  # Append failures
                "schema": {
                    "fields": [
                        {"name": "data_url", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
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
                "error_message": error_message,
                "load_ts": datetime.utcnow().isoformat(),
            }
        ],
    )
    insert_job.execute(context={})


def delete_failed_record(bq_table, data_url, timestamp, credentials, project_id):
    """Deletes a record from BigQuery if status is FAILURE."""
    from google.cloud import bigquery

    if isinstance(credentials, tuple):
      client = bigquery.Client(project=project_id)
    else:
      client = bigquery.Client(credentials=credentials, project=project_id)

    delete_query = f"""
        DELETE FROM `{bq_table}`
        WHERE data_url = @data_url
        AND timestamp = @timestamp
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("data_url", "STRING", data_url),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
        ]
    )
    try:
      query_job = client.query(delete_query, job_config=job_config)
      query_job.result()  # Wait for the query to complete
      print(f"Deleted failed record for {data_url} at {timestamp}")
    except Exception as e:
       print(f"Failed to delete the record from bigquery table:{e}")



def process_data_url(credentials, project_id, scope_id, data_url, bucket_name, destination_blob_prefix, bq_table, dag_run, auth_url):
    """Processes a single data URL."""
    last_successful_time = dag_run.get_previous_execution_date(state=State.SUCCESS)
    if last_successful_time is None:
        last_successful_time = dag_run.start_date - timedelta(hours=2)

    current_time = dag_run.execution_date
    five_min_interval = timedelta(minutes=5)

    failed_timestamps = get_failed_timestamps(bq_table, data_url, last_successful_time, current_time, credentials, project_id)

    current_timestamp = last_successful_time + five_min_interval
    while current_timestamp <= current_time:
        if current_timestamp not in failed_timestamps: #check if existing timestamps are already present or not.
            failed_timestamps.append(current_timestamp)
        current_timestamp += five_min_interval

    for timestamp in failed_timestamps:
        download_and_upload_zip(
            credentials=credentials,
            project_id=project_id,
            scope_id=scope_id,
            data_url=data_url,
            timestamp=timestamp,
            bucket_name=bucket_name,
            destination_blob_prefix=destination_blob_prefix,
            bq_table=bq_table,
            auth_url=auth_url
        )



def get_failed_timestamps(bq_table, data_url, start_time, end_time, credentials, project_id):
    """Retrieves failed timestamps."""
    from google.cloud import bigquery
    if isinstance(credentials, tuple):
      client = bigquery.Client(project=project_id)
    else:
      client = bigquery.Client(credentials=credentials, project=project_id)

    query = f"""
        SELECT timestamp
        FROM `{bq_table}`
        WHERE data_url = @data_url
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



def create_data_dag(dag_config):
    """Creates a DAG for a single URL."""
    dag_id = dag_config['dag_id']
    environment = dag_config['environment']
    data_url = dag_config['data_url']

    with create_session() as session:
        dag_exists = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
    if dag_exists:
        print(f"DAG '{dag_id}' already exists. Skipping creation.")
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

        credentials, project_id = get_credentials(environment, dag_config)
        auth_url = dag_config['api_endpoints']['auth_url']

        get_scope_id_task = PythonOperator(
            task_id="get_scope_id",
            python_callable=get_scope_id,
            op_kwargs={
                "scope_url": dag_config['api_endpoints']['scope_url'],
                "token": get_token(credentials, auth_url)
            },
        )

        process_data_task = PythonOperator(
            task_id="process_data_url",
            python_callable=process_data_url,
            op_kwargs={
                "credentials": credentials,
                "project_id": project_id,
                "data_url": data_url,
                "bucket_name": dag_config['gcs_bucket'],
                "destination_blob_prefix": dag_config['destination_blob_name'],
                "bq_table": dag_config['bigquery_table'],
                "auth_url": auth_url,
            },
            provide_context=True,
            op_kwargs_from_xcom={'scope_id': "{{ ti.xcom_pull(task_ids='get_scope_id') }}"},
        )

        start >> get_scope_id_task >> process_data_task >> end

    return dag



def create_and_deploy_dags(config_folder, **kwargs):
    """Creates and deploys DAGs from JSON configs."""
    dagbag = DagBag(include_examples=False)

    for filename in os.listdir(config_folder):
        if filename.endswith(".json"):
            config_path = os.path.join(config_folder, filename)
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)

                for data_url_config in config['data_url_configs']:
                    dag_id = f"{config['base_dag_id']}_{data_url_config['name']}"
                    dag_config = {
                        "dag_id": dag_id,
                        "environment": config['environment'],
                        "schedule_interval": config.get('schedule_interval', '@hourly'),
                        "start_date": config.get('start_date', '2023-10-26'),
                        "catchup": config.get('catchup', False),
                        "tags": config.get('tags', []) + [data_url_config['name']],
                        "default_args": config.get('default_args', {}),
                        "api_credentials": config['api_credentials'],
                        "api_endpoints": config['api_endpoints'],
                        "data_url": data_url_config['url'],
                        "gcs_bucket": config['gcs_bucket'],
                        "destination_blob_name": data_url_config.get('destination_blob_name', config['destination_blob_name']),
                        "bigquery_table": config['bigquery_table'],
                    }

                    with create_session() as session:
                        dag_exists = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
                    if not dag_exists:
                        dag = create_data_dag(dag_config)
                        if dag:
                            dagbag.dags[dag_id] = dag
                            globals()[dag_id] = dag
                            print(f"DAG '{dag_id}' created successfully from {filename}.")

                            trigger_dag = TriggerDagRunOperator(
                                task_id=f"trigger_{dag_id}",
                                trigger_dag_id=dag_id,
                                wait_for_completion=False,
                                execution_date="{{ execution_date }}",
                                reset_dag_run=True,
                            )
                            trigger_dag.execute(context=kwargs)
                    else:
                        print(f"DAG '{dag_id}' already exists. Skipping.")

            except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
                print(f"Error processing config file {filename}: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")



# --- Main DAG that creates and triggers the data DAGs ---
with DAG(
    dag_id="dag_creator",
    schedule_interval=None,
    start_date=datetime(2023, 11, 16),
    catchup=False,
    tags=['dag_management'],
) as dag_creator:

    create_dags_task = PythonOperator(
        task_id="create_and_deploy_dags",
        python_callable=create_and_deploy_dags,
        op_kwargs={"config_folder": "config"},
        provide_context=True,
    )