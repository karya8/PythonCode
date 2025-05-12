from __future__ import annotations

import datetime
import logging
import os
from typing import Any

import pendulum
import yaml
import json
import shutil

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

BQ_TABLE = "{{ dag_run.conf.bigquery_table }}"
BQ_DATASET = "{{ dag_run.conf.bigquery_dataset }}"
BQ_PROJECT = "{{ dag_run.conf.bigquery_project }}"


def _round_down_to_nearest_five(minute: int) -> int:
    """Rounds a minute value down to the nearest multiple of 5."""
    return minute - (minute % 5)


@task
def read_main_config(main_config_file: str) -> dict[str, Any]:
    """Reads the main configuration from a JSON file."""
    config_file_path = os.path.join(os.path.dirname(__file__), main_config_file)
    with open(config_file_path, "r") as f:
        config_data = json.load(f)
    return config_data


@task
def read_load_requests_file(config_base_path: str, load_requests_file: str) -> list[dict[str, Any]]:
    """Reads the YAML file containing a list of load requests."""
    config_file_path = os.path.join(os.path.dirname(__file__), config_base_path, load_requests_file)
    with open(config_file_path, "r") as f:
        load_requests_data = yaml.safe_load(f)
    return load_requests_data.get("load_requests", [])


@task
def generate_timestamps(load_request: dict[str, Any]) -> list[str]:
    """Generates 5-minute timestamp intervals."""
    start_ts_str = load_request["start_timestamp"]
    end_ts_str = load_request["end_timestamp"]

    start_ts = pendulum.parse(start_ts_str)
    end_ts = pendulum.parse(end_ts_str)

    timestamps = []
    current_ts = start_ts.replace(minute=_round_down_to_nearest_five(start_ts.minute), second=0, microsecond=0)

    while current_ts <= end_ts:
        timestamps.append(current_ts.to_iso8601_string())
        current_ts = current_ts.add(minutes=5)

    return timestamps


@task
def process_timestamps_bq(schema_name: str, timestamps: list[str], load_strategy: str, bq_project: str, bq_dataset: str, bq_table: str):
    """Inserts or updates timestamps in BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=bq_project)
    table_id = f"{bq_project}.{bq_dataset}.{bq_table}"

    for ts in timestamps:
        query = f"""
            SELECT timestamp
            FROM `{table_id}`
            WHERE schema_name = '{schema_name}' AND timestamp = '{ts}'
        """
        query_job = client.query(query)
        results = query_job.result()

        if results.total_rows == 0:
            insert_query = f"""
                INSERT INTO `{table_id}` (schema_name, timestamp, status, retry)
                VALUES ('{schema_name}', '{ts}', 'load', 0)
            """
            insert_job = client.query(insert_query)
            insert_job.result()
            logging.info(f"Inserted timestamp: {ts} for schema: {schema_name}")
        elif load_strategy.lower() == "overwrite":
            update_query = f"""
                INSERT INTO `{table_id}` (schema_name, timestamp, status, retry)
                VALUES ('{schema_name}', '{ts}', 'load', COALESCE((SELECT retry FROM `{table_id}` WHERE schema_name = '{schema_name}' AND timestamp = '{ts}'), 0) + 1)
            """
            update_job = client.query(update_query)
            update_job.result()
            logging.info(f"Overwrote timestamp: {ts} for schema: {schema_name}")
        else:
            logging.info(f"Skipping timestamp: {ts} for schema: {schema_name} as it exists and load strategy is not overwrite.")


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def archive_load_requests_file(config_base_path: str, load_requests_file: str, archive_base_path: str):
    """Moves the processed load requests file to the specified archive folder."""
    source_path = os.path.join(os.path.dirname(__file__), config_base_path, load_requests_file)
    archive_folder = os.path.join(os.path.dirname(__file__), archive_base_path)
    destination_path = os.path.join(archive_folder, load_requests_file)
    os.makedirs(archive_folder, exist_ok=True)
    shutil.move(source_path, destination_path)
    logging.info(f"Moved load requests file '{load_requests_file}' to '{archive_folder}'")


@dag(
    dag_id="{{ dag_run.conf.dag_name }}",
    schedule="30 16 * * *",  # Runs daily at 12:30 PM EDT (16:30 UTC)
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["timestamp_processing"],
    default_args={
        "retries": None,  # Will be read from config
        "retry_delay": None,  # Will be read from config
    },
)
def timestamp_processing_dag():
    main_config_task = read_main_config(main_config_file="config/main_config.json")
    load_requests = read_load_requests_file(
        config_base_path=main_config_task["config_folder"],
        load_requests_file=main_config_task["load_requests_file"],
    )

    # Set default_args here, after reading the config
    dag_kwargs = {
        "default_args": {
            "retries": main_config_task.get("retries"),
            "retry_delay": datetime.timedelta(minutes=main_config_task.get("retry_delay_minutes")) if main_config_task.get("retry_delay_minutes") else None,
        }
    }
    dag.update_args(dag_kwargs)

    process_tasks = []
    for index, load_request in enumerate(load_requests):
        schema_name = load_request["schema_name"]
        generate_ts = generate_timestamps.override(task_id=f"generate_ts_{schema_name}")(load_request=load_request)
        process_bq = process_timestamps_bq.override(task_id=f"process_bq_{schema_name}")(
            schema_name=schema_name,
            timestamps=generate_ts,
            load_strategy=load_request["load_strategy"],
            bq_project=main_config_task.get("bigquery_project"),
            bq_dataset=main_config_task.get("bigquery_dataset"),
            bq_table=main_config_task.get("bigquery_table"),
        )
        process_tasks.append(process_bq)

        # Set dependencies to run sequentially
        if index > 0:
            generate_ts.set_upstream(process_tasks[index - 1])
        else:
            generate_ts.set_upstream(read_load_requests_file)

    archive_task = archive_load_requests_file.override(task_id="archive_load_requests")(
        config_base_path=main_config_task["config_folder"],
        load_requests_file=main_config_task["load_requests_file"],
        archive_base_path=main_config_task["archive_folder"],
    )
    archive_task.set_upstream(process_tasks[-1]) if process_tasks else archive_task.set_upstream(read_load_requests_file)


timestamp_processing_dag()



config json file
{
  "config_folder": "config_files",
  "archive_folder": "processed_config_files",
  "load_requests_file": "load_requests.yaml",
  "bigquery_project": "your-gcp-project-id",
  "bigquery_dataset": "your_dataset_name",
  "bigquery_table": "your_table_name",
  "retries": 3,
  "retry_delay_minutes": 5
}

load_requests:
  - schema_name: schema_a
    start_timestamp: "2025-05-12T09:00:00"
    end_timestamp: "2025-05-12T10:00:00"
    load_strategy: overwrite
    comments: "Initial load for schema A"
  - schema_name: schema_b
    start_timestamp: "2025-05-12T10:05:00"
    end_timestamp: "2025-05-12T10:15:00"
    load_strategy: append
    comments: "Adding more data for schema B"
  - schema_name: schema_c
    start_timestamp: "2025-05-12T11:00:00"
    end_timestamp: "2025-05-12T11:30:00"
    load_strategy: overwrite
    comments: "Another load for schema C"
