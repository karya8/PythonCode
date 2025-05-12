from __future__ import annotations

import datetime
import logging
import os
from typing import Any

import pendulum
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
def read_main_config(main_config_file: str) -> dict[str, str]:
    """Reads the main configuration from a JSON file."""
    config_file_path = os.path.join(os.path.dirname(__file__), main_config_file)
    with open(config_file_path, "r") as f:
        config_data = json.load(f)
    return {"config_folder": config_data.get("config_folder", "config"),
            "archive_folder": config_data.get("archive_folder", "archive")}


@task
def read_load_requests(config_base_path: str) -> list[str]:
    """Reads all load request files from the specified config folder."""
    config_folder = os.path.join(os.path.dirname(__file__), config_base_path)
    load_request_files = [
        f for f in os.listdir(config_folder) if f.endswith((".yaml", ".yml", ".json"))
    ]
    return [os.path.join(config_folder, lrf) for lrf in load_request_files]


@task
def read_load_request(load_request_file_path: str) -> dict[str, Any]:
    """Reads a single load request file."""
    with open(load_request_file_path, "r") as f:
        config_data = json.load(f) if load_request_file_path.endswith(".json") else yaml.safe_load(f)
    return config_data


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
def process_timestamps_bq(schema_name: str, timestamps: list[str], load_strategy: str):
    """Inserts or updates timestamps in BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

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
def archive_load_request(load_request_file_path: str, archive_base_path: str):
    """Moves the processed load request file to the specified archive folder."""
    load_request_file_name = os.path.basename(load_request_file_path)
    archive_folder = os.path.join(os.path.dirname(__file__), archive_base_path)
    destination_path = os.path.join(archive_folder, load_request_file_name)
    os.makedirs(archive_folder, exist_ok=True)
    shutil.move(load_request_file_path, destination_path)
    logging.info(f"Moved load request file '{load_request_file_name}' to '{archive_folder}'")


@dag(
    dag_id="{{ dag_run.conf.dag_name }}",
    schedule="30 16 * * *",  # Runs daily at 12:30 PM EDT (16:30 UTC)
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["timestamp_processing"],
    default_args={
        "retries": "{{ dag_run.conf.retries | int }}",
        "retry_delay": datetime.timedelta(minutes=5),
    },
)
def timestamp_processing_dag():
    main_config_task = read_main_config(main_config_file="config/main_config.json")
    load_request_files = read_load_requests(config_base_path=main_config_task["config_folder"])

    process_tasks = []
    for load_request_file_path in load_request_files:
        load_request = read_load_request.override(task_id=f"read_{os.path.basename(load_request_file_path).replace('.', '_')}")(load_request_file_path=load_request_file_path)
        generated_timestamps = generate_timestamps.override(task_id=f"generate_ts_{os.path.basename(load_request_file_path).replace('.', '_')}")(load_request=load_request)
        process_bq = process_timestamps_bq.override(task_id=f"process_bq_{load_request['schema_name']}")(
            schema_name=load_request["schema_name"],
            timestamps=generated_timestamps,
            load_strategy=load_request["load_strategy"],
        )
        archive_task = archive_load_request.override(task_id=f"archive_{os.path.basename(load_request_file_path).replace('.', '_')}")(
            load_request_file_path=load_request_file_path,
            archive_base_path=main_config_task["archive_folder"],
        )

        # Define the sequential order of tasks for each load request
        load_request.set_upstream(main_config_task)
        generated_timestamps.set_upstream(load_request)
        process_bq.set_upstream(generated_timestamps)
        archive_task.set_upstream(process_bq)

        process_tasks.append(archive_task)  # Optionally keep track of the final tasks


timestamp_processing_dag()
