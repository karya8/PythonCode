# master_dag_file.py

import os
import zipfile
import csv
import shutil
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Master DAG Configuration (loaded from JSON)
MASTER_CONFIG_FILE = "master_config.json"

def load_master_config():
    """Loads the master DAG configuration from the JSON file."""
    with open(MASTER_CONFIG_FILE, "r") as f:
        return json.load(f)

master_config = load_master_config()

STORAGE_BUCKET = master_config.get("storage_bucket")
DAGS_BUCKET_PATH = master_config.get("dags_bucket_path", "dags")
MASTER_DAG_FILE_NAME = master_config.get("master_dag_file_name", "master_dag_file.py")
CONFIG_DIR = master_config.get("config_dir","config")

def download_and_unzip(zip_file_path, processing_folder):
    """Downloads a zip file from the storage bucket and unzips it."""
    local_zip_path = os.path.join(processing_folder, os.path.basename(zip_file_path))

    print(f"Simulating download of {zip_file_path} to {local_zip_path}")

    os.makedirs(processing_folder, exist_ok=True)
    with open(local_zip_path, 'w') as f:
        f.write("Simulated zip content")

    with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
        zip_ref.extractall(processing_folder)

    return local_zip_path

def process_csv(csv_file_name, header_file_name, processing_folder, zip_file_path):
    """Reads the first line of the CSV and compares it with a header file."""
    csv_file_path = os.path.join(processing_folder, csv_file_name)
    header_file_path = os.path.join(CONFIG_DIR, header_file_name)

    try:
        with open(csv_file_path, "r") as csvfile:
            reader = csv.reader(csvfile)
            first_row = next(reader)

        with open(header_file_path, "r") as headerfile:
            header_row = next(csv.reader(headerfile))

        if first_row == header_row:
            os.makedirs("success", exist_ok=True)
            shutil.move(csv_file_path, os.path.join("success", csv_file_name))
            shutil.move(zip_file_path, os.path.join("success", os.path.basename(zip_file_path)))
            print(f"Header match! Moved {csv_file_name} and zip to success.")
        else:
            os.makedirs("error", exist_ok=True)
            os.remove(csv_file_path)
            shutil.move(zip_file_path, os.path.join("error", os.path.basename(zip_file_path)))
            print(f"Header mismatch! Moved zip to error and deleted {csv_file_name}.")

    except FileNotFoundError as e:
        print(f"File not found: {e}")
        os.makedirs("error", exist_ok=True)
        shutil.move(zip_file_path, os.path.join("error", os.path.basename(zip_file_path)))
    except Exception as e:
        print(f"An error occurred: {e}")
        os.makedirs("error", exist_ok=True)
        shutil.move(zip_file_path, os.path.join("error", os.path.basename(zip_file_path)))

def create_dynamic_dag_file_and_upload(dag_id, dag_config):
    """Creates a Python file for a dynamic DAG and uploads it to storage."""

    dag_code = f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from {MASTER_DAG_FILE_NAME.replace('.py', '')} import download_and_unzip, process_csv
from airflow.models import Variable

STORAGE_BUCKET = Variable.get("storage_bucket")
PROCESSING_FOLDER = "processing"

with DAG(
    dag_id="{dag_id}",
    schedule_interval="{dag_config.get('schedule_interval')}",
    start_date=datetime({dag_config.get('start_date', '2024, 1, 1')}),
    catchup=False,
) as dag:

    def download(**context):
        return download_and_unzip("{dag_config['zip_file_path']}", PROCESSING_FOLDER)

    def process(**context):
        zip_file_path = context['ti'].xcom_pull(task_ids='download_zip')
        process_csv("{dag_config['csv_file_name']}", "{dag_config['header_file_name']}", PROCESSING_FOLDER, zip_file_path)

    download_task = PythonOperator(
        task_id="download_zip",
        python_callable=download,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id="process",
        python_callable=process,
        provide_context=True,
    )

    download_task >> process_task
"""

    print(f"Simulating upload of {dag_id}.py to {STORAGE_BUCKET}/{DAGS_BUCKET_PATH}")
    print(dag_code)

# Master DAG to create dynamic DAG files and upload them
with DAG(
    dag_id="master_dag_upload_dags",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as master_dag:

    def generate_and_upload_dags(**context):
        """Reads configuration files, creates DAG files, and uploads them."""
        for config_file in os.listdir(CONFIG_DIR):
            if config_file.endswith(".json"):
                config_path = os.path.join(CONFIG_DIR, config_file)
                with open(config_path, "r") as f:
                    dag_config = json.load(f)
                dag_id = dag_config["dag_id"]
                create_dynamic_dag_file_and_upload(dag_id, dag_config)

    create_and_upload_task = PythonOperator(
        task_id="generate_and_upload_dags",
        python_callable=generate_and_upload_dags,
        provide_context=True,
    )

    create_and_upload_task
